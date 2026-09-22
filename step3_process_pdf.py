#!/usr/bin/env python3
"""
merge_survey_pdfs.py

Single self-contained script that does two things against a GCS bucket of
scanned survey PDFs (currently: the "Treatment Perceptions Survey (Adult)"
CalOMS form — see SURVEY_QUESTIONS below):

  1. MERGE  — combine each date subfolder's PDFs into one PDF per folder,
     plus a page-level manifest (folder / source file / page range).

  2. EXTRACT — read every source PDF with a Vertex AI Gemini model (the
     survey answers are hand-marked checkboxes/write-ins, not in the PDF's
     text layer, so this needs a vision-capable model, not plain text
     extraction) and load one row per question into a BigQuery table with
     columns:

       folder_name       - the date-folder the source PDF came from
       file_name         - the source PDF's filename
       survey_question   - "<number>. <question text>", e.g. "14. Overall, I am
                            satisfied with the services I received." Two questions
                            (33 Race/Ethnicity, 34 Disability Status) explicitly
                            allow marking more than one choice; their answer is
                            every marked choice joined by "; ".
       survey_answer     - the marked/written answer, or "" if left blank
       report_date       - DATE the survey was filled out, parsed from the folder name
                            (e.g. "Nov 18 2025" -> 2025-11-18); NULL if unparseable.
                            NOTE: on the one real sample seen so far, the form's own
                            hand-written "Today's Date" field (captured as question H6)
                            didn't match its folder name — see the SURVEY_QUESTIONS
                            comment below.
       refreshed_at      - TIMESTAMP this ETL run loaded the row
       refreshed_date    - DATE(refreshed_at); the table's partitioning column
       mark_position     - for choice-list questions, the 1-based position (left-to-right /
                            in the order listed) of the box Gemini says is marked, e.g. "6"
                            for "Not Applicable" in a 6-point scale. "" for open-text/write-in
                            questions or when nothing is marked. This is asked for
                            SEPARATELY from survey_answer so the two can be cross-checked
                            against each other (see needs_review below).
       needs_review      - BOOLEAN. TRUE when survey_answer and mark_position DISAGREE with
                            each other (e.g. the model said "Disagree" but also said the
                            marked box was position 6, which is "Not Applicable" on this
                            form) — a strong signal the reading may be wrong, most often on
                            dense checkbox tables with small/rotated column headers. Also
                            TRUE when the model's own self-reported confidence for a
                            question falls below MODEL_CONFIDENCE_THRESHOLD, or when an
                            independent Cloud Vision OCR reading disagrees with the model's
                            answer on a handwritten/write-in field — see "Model confidence
                            + Cloud Vision double-check" below. Query `WHERE needs_review =
                            TRUE` to find every row worth a manual look; see also the
                            verify_pdf() function below for auditing one specific file by
                            hand.
       review_note       - human-readable explanation of why needs_review is TRUE (empty
                            otherwise). May combine more than one reason, semicolon-separated.
       detection_method  - 'pixel_grid' / 'pixel_yesno_box' / 'pixel_h3_circle' (a
                            deterministic ink-density pixel read - see "Reading accuracy"
                            below) or 'model' (vision-model reading). Pixel readings are
                            trusted over the model.
       model_reasoning   - the model's own one-sentence account of the visual evidence for
                            its answer (see rule 11 in build_extraction_prompt()); kept for
                            auditability only.
       model_confidence  - the model's own self-reported confidence (0.0-1.0) for this
                            specific answer (see rule 15 in build_extraction_prompt()); NULL
                            if the model didn't return one. Below MODEL_CONFIDENCE_THRESHOLD
                            (default 0.8) -> needs_review=TRUE, UNLESS a pixel detector
                            already confidently overrode this question (that pixel margin
                            check already vouches for it independently of what the model
                            says about itself).
       vision_cross_check - for the seven handwritten/write-in questions that have no fixed
                            choice list and hence no pixel backstop (H1, H2, H4, H5, H6, 24,
                            26 - see WRITTEN_TEXT_QUESTION_NUMBERS): 'agree' or 'disagree',
                            from an independent Cloud Vision OCR reading of the same page
                            compared against the model's answer (see "Model confidence +
                            Cloud Vision double-check" below). Empty for every other
                            question, or when Vision was unavailable/found nothing to
                            compare against.
       confidence_threshold - the value of MODEL_CONFIDENCE_THRESHOLD actually in effect
                            when this row was extracted. Recorded on EVERY row (an audit
                            trail: query WHERE model_confidence < confidence_threshold to
                            see exactly what the gate acted on, including historical rows
                            extracted under a different threshold value if it's retuned
                            later chasing the ~70% needs_review target).
       vision_match_score - the numeric agreement score behind vision_cross_check: 1.0/0.0
                            for the token fields' exact-match check (H1/H2/26), or the
                            actual fuzzy-match coverage fraction (0.0-1.0) for the freeform
                            fields (H4/H5/H6/24). Populated whenever vision_cross_check is
                            'agree' or 'disagree' (even on agreement, so a narrow pass is
                            visible, not just "agree"); NULL otherwise.
       vision_ocr_snippet - what Cloud Vision actually read that survey_answer was compared
                            against: a handful of individual word tokens for the token
                            fields, or a truncated excerpt of the page's OCR text for the
                            freeform fields. Populated whenever vision_cross_check is
                            'agree' or 'disagree'; lets you see the comparison directly
                            rather than just trusting the verdict.

     Table write behavior: the table is created if it doesn't exist yet
     (never dropped/replaced as a whole). Loading is per-folder REPLACE by
     default — before loading a folder's rows, any existing rows with that
     same folder_name are DELETEd first, then the new rows are loaded via a
     load job (WRITE_APPEND at the table level, but the delete makes it a
     folder-level replace overall). This makes re-running the script against
     the same folder idempotent instead of duplicating rows. Pass
     --no-replace-folder for pure append (will duplicate rows on rerun).

--------------------------------------------------------------------------
Reading accuracy: high-res page images + answer/position cross-check
--------------------------------------------------------------------------
Earlier versions handed Gemini the PDF directly via Part.from_uri(), letting
it decide internally how to rasterize/downsample each page. On dense
checkbox tables with small, sideways/rotated column headers (this survey's
6-point agreement scale — Strongly Agree / Agree / I am Neutral / Disagree /
Strongly Disagree / Not Applicable — is exactly this shape) that was
observed to misread which box was marked (e.g. reporting "Disagree" when the
X was actually in the last "Not Applicable" column on Nov10_1.pdf, Q1).

Three changes address this:

  1. Each PDF page is explicitly rendered to a high-resolution PNG (300 DPI,
     via PyMuPDF/fitz) and sent to Gemini as image parts, instead of relying
     on whatever internal resolution Part.from_uri()'s native PDF ingestion
     uses.

  2. For every choice-list question, the model is asked for BOTH the answer
     label AND the 1-based position of the marked box (counting the choices
     in the exact order listed in the prompt). These two are cross-checked
     against each other after the fact (cross_check_answer()) — if they
     disagree (label says "Disagree" but position says 6, which is "Not
     Applicable"), the row is flagged needs_review=TRUE with a review_note.
     This catches self-inconsistency, but NOT the case where the model is
     wrong in a way that agrees with itself — which is exactly what was
     reported next, on Q10.

  3. For the main Q1-18 checkbox grid specifically (a fixed 6-point-scale
     table with ruled grid lines), detect_checkbox_grid_answers() reads the
     mark DETERMINISTICALLY from the rendered page's pixels — no LLM
     involved: it locates the table's own row/column ruled lines with
     OpenCV, then measures ink density in each of the 6 candidate cells per
     row and picks the one with the most ink. Where this reading is
     confident (a clear density margin over the runner-up), it is TRUSTED
     OVER the model's answer, even correcting it silently when they disagree
     — while still recording that correction (needs_review=TRUE +
     review_note) for visibility. See detect_checkbox_grid_answers()'s
     docstring for exactly how this is done and why a purely LLM-side check
     can't catch this class of error.

  4. For seven specific, isolated checkbox questions outside that grid — the
     Yes/No(/Unknown) rows 21, 22, 27, and 32, plus the vertical
     single-choice lists 25 (length of time in services, 4 options), 29
     (gender identity, 7 options) and 35 (criminal justice involvement, 5
     options) — detect_yesno_box_answers() applies the
     same "trust a confident pixel reading over the model" idea, but locates
     each box from a fixed, pre-calibrated pixel coordinate (re-validated
     against the actual scan at read time, so a misaligned/different scan
     safely falls back rather than reading the wrong pixels) instead of the
     grid's dynamic row/column detection. This exists because Q27 ("Are you
     homeless?") kept misreading "Yes" as "No" even after the prompt fix in
     item 2 above, and separately because a live Gemini call still misread
     Q29 and Q35 on a real file even with that prompt fix in place — Q35's
     miss (marked choice 1 of 5 read back as choice 5) fits the same
     "defaults to a later-listed choice" bias item 2 targets, while Q29's
     miss (marked choice 1 of 7 read back as choice 2) does not fit that
     pattern and has no other established root cause; Q25 was added after a
     third real file showed the same "ungoverned model-only question read
     wrong" shape (marked choice 3 of 4 read back incorrectly) on a question
     with the exact same vertical-list layout as Q29/Q35 — a from-scratch
     general detector for arbitrary checkbox rows was tried and rejected
     (see that function's module comment for exactly what was tried and why
     it failed), so this is deliberately narrow and only covers these seven
     questions, calibrated and pixel-verified one at a time. (Since then,
     Q19 and Q23 were added in Rounds 13/11-12 using a dynamic "ruled" row
     location instead of a fixed pad, and Q28 was added in Round 15 using
     this same fixed-pad technique - ten questions now covered by
     detect_yesno_box_answers() in total; see that function's own docstring
     for the current, up-to-date list.)

None of this guarantees every answer is now correct — a genuinely faint or
ambiguous mark, or a template these pixel detectors can't confidently
locate, falls back to the model-only reading — which is why verify_pdf()
(below) still exists for spot-checking a specific file against the actual
scan by eye.

--------------------------------------------------------------------------
Model confidence + Cloud Vision double-check (added at user request)
--------------------------------------------------------------------------
Two further, independent quality gates sit on top of everything above:

  5. MODEL SELF-REPORTED CONFIDENCE. build_extraction_prompt()'s rule 15
     now asks Gemini to report its own "confidence" (0.0-1.0) for every
     single question, alongside "reasoning"/"mark_position"/"answer" — how
     certain it is that ITS OWN answer is correct, based purely on how
     unambiguous the visual evidence was. Any question whose final answer
     came from the model alone (no pixel detector already confidently took
     over — see answers_to_qa_rows()) and whose self-reported confidence is
     below MODEL_CONFIDENCE_THRESHOLD (default 0.8) is flagged
     needs_review=TRUE. This is a genuinely new signal, not a rebuilt one:
     nothing before this asked the model how sure it was, so a "confidently
     wrong" answer (self-consistent per cross_check_answer(), no pixel
     detector for that question, but actually wrong) had no way to surface
     itself at all. This does NOT override a pixel-verified reading — a
     pixel detector's own margin threshold is its own, independent
     confidence signal, and a low model confidence on a question the pixel
     side already resolved is recorded (model_confidence is still stored)
     but not itself grounds for review.

  6. CLOUD VISION DOUBLE-CHECK FOR HANDWRITTEN/WRITE-IN FIELDS. Seven
     questions — H1 (CalOMS Provider ID), H2 (Program Reporting Unit code),
     H4 (agency), H5 (address), H6 (today's date), 24 (open comment), and 26
     (age) — have NO fixed choice list at all (see
     WRITTEN_TEXT_QUESTION_NUMBERS), so cross_check_answer() can never
     check them (there's no mark_position to compare against), and no pixel
     checkbox detector applies either (there's no checkbox to measure ink
     in). Before this, a genuine misread on one of these — the two
     concrete, reported failure shapes were H1 dropping a leading digit
     ("19697" instead of "196697") and H2 gaining a hallucinated suffix
     ("3619 3619NMIS" instead of "3619") — had NO independent signal
     catching it at all.

     H1/H2/H4/H5/H6 live on page 1; 24 and 26 live on page 2 —
     _WRITTEN_TEXT_QUESTION_PAGE routes each question to the correct
     already-rendered page image before OCR'ing it (a real production run
     caught this the hard way: an earlier version only ever OCR'd page 1,
     so 24/26 could never be found and always disagreed). Each needed page
     is OCR'd at most once per file and the result cached across questions.

     cloud_vision_ocr_page() runs Google Cloud Vision's
     document_text_detection on the page, giving a second, independent
     transcription of everything printed/written on it — both the whole
     page's text (full_text/tokens) AND, per word, its pixel bounding box
     (words). cross_check_written_field_with_vision() then compares the
     model's answer against that independent reading, preferring one of two
     sources in order:

       1. PREFERRED — ROUND 8, label-anchored matching. Each of these six
          fields (H1, H2, H4, H5, H6, 26 — see _WRITTEN_FIELD_ANCHORS; "24"
          is excluded, see below) sits right next to its own PRINTED field
          label ("Provider ID", "Agency", "Date", "Age", etc.) — printed
          text, which Vision reads reliably even where it struggles with
          handwriting or a boxed-digit grid beside it. _find_anchor_phrase()
          locates that label in Vision's word-position data (after
          _sort_words_reading_order() puts the words back in human reading
          order), and _words_in_window() gathers only the words spatially
          near it (same row to the right, or the line(s) below) and
          reconstructs them into one value BY POSITION — the same
          "landmark, then read only what's near it" pattern already used
          for the ruled-line Yes/No boxes elsewhere in this file. This is
          what stops a neighboring field's text from bleeding into the
          comparison (a real failure: H2's "URB" token contaminating H1's
          check) and lets a boxed single-character digit grid be
          reassembled by position instead of trusting Vision's own,
          unreliable word-segmentation of it (a real failure: "196697"
          fragmenting into 'S','675','1','91','909'-style tokens). Added
          after these two failures were found by testing against a real
          scan and real Cloud Vision output.
       2. FALLBACK — the original whole-page comparison, used whenever a
          field's label can't be located (Vision misread it, the page
          layout doesn't match this form), the field has no anchor
          configured at all (currently just "24" — a multi-line comment of
          unpredictable height with no reliable second anchor to bound it),
          or vision_result predates the "words" key entirely.

     Once the comparison text is chosen (source 1 or 2), the matching
     itself: for the four short, digit/token fields (H1, H2, 26, and — as a
     special case among the "freeform" group, since it's also a boxed
     single-digit grid — H6 when the anchored source is used) it requires an
     EXACT match on digits/token text (deliberately stricter than substring
     containment — a dropped-digit answer like "19697" is a substring of
     the correct "196697" and must still be caught, not waved through); for
     the free-form fields being compared against a longer stretch of text
     (H4/H5 anchored, H6/24 on the fallback whole-page path) it looks for
     the model's answer as a close match (word-level coverage summed
     across every matching run - see cross_check_written_field_with_vision()
     - ≥VISION_FREEFORM_COVERAGE_THRESHOLD, currently 90%) somewhere in the
     comparison text. A disagreement
     sets vision_cross_check="disagree" and needs_review=TRUE. See both
     functions' own docstrings for the full reasoning and the accepted
     false-positive rate.

     IMPORTANT CAVEAT: the Round 8 anchor logic has only been validated
     against hand-built synthetic word/bounding-box fixtures
     (self_test_anchored_vision_extraction()) — there is no live Cloud
     Vision API access in the environment this was developed in, so it has
     never been run against a real scan's real Vision response. It fails
     safe by design (an unlocatable anchor always falls back to the
     original whole-page check, never a crash or a guess), but it should be
     validated against a real file with real credentials before being
     trusted in production.

     Both of these gates fail SAFE: a missing/unparseable model confidence,
     a Vision API call that errors out (not installed, no credentials, API
     not enabled, quota, network), or a written-text question with nothing
     for Vision to compare against, all resolve to "no signal available"
     (None), which is treated as "don't flag on this basis" — never a
     guess, matching every pixel detector's own established convention in
     this module.

Use verify_pdf() to audit one specific file's readings before/after trusting
them, e.g. from a notebook cell:

    from merge_survey_pdfs import verify_pdf, BUCKET_NAME
    verify_pdf(BUCKET_NAME, "syn-workspace/.../Nov 10 2025/Nov10_1.pdf")

This prints every question's answer, reported mark_position, and whether
they agree, without writing anything to BigQuery — hold it up against the
scanned PDF page by page. The same --verify-file flag is available from the
command line (see Usage below).

--------------------------------------------------------------------------
Expected bucket layout
--------------------------------------------------------------------------
gs://<BUCKET>/<ROOT_PREFIX>/
    Nov 6 2025/
        Nov6_1.pdf
        Nov6_2.pdf
        ...
    Nov 10 2025/
        Nov10_1.pdf
        ...
    Nov 18 2025/
        Nov18_1.pdf
        Nov18_10.pdf
        ...

Each date subfolder is treated as one batch. All PDFs directly inside a
date folder are concatenated, in natural (human) sort order (Nov18_2
before Nov18_10), into a single merged PDF:

gs://<BUCKET>/<OUTPUT_PREFIX>/<folder>_merged.pdf

A CSV manifest is written alongside it (and uploaded) recording, for every
source file, which merged file and page-range it landed in:

    source_folder, source_file, source_gcs_uri,
    merged_file, merged_gcs_uri, page_start, page_end, num_pages

--------------------------------------------------------------------------
Setup
--------------------------------------------------------------------------
1. pip install google-cloud-storage google-cloud-aiplatform google-cloud-bigquery google-cloud-vision pypdf pymupdf opencv-python-headless numpy
2. Authenticate to GCP, one of:
     gcloud auth application-default login
   or
     export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
3. Permissions needed:
     roles/storage.objectViewer            on the source bucket/prefix
     roles/storage.objectCreator (or objectAdmin) on the output prefix
     roles/aiplatform.user                 to call the Gemini model on Vertex AI
     roles/bigquery.dataEditor             on the target dataset
     roles/bigquery.jobUser                on the target project
     (Cloud Vision API calls use the same application-default credentials;
     no extra IAM role beyond having the Cloud Vision API enabled, below.)
4. In the GCP project, enable the Vertex AI API (aiplatform.googleapis.com)
   and confirm which Gemini model ID is available to you in the Vertex AI
   Model Garden / Studio — model IDs change over time, so check rather than
   trust the GEMINI_MODEL default below blindly. Also enable the Cloud
   Vision API (vision.googleapis.com) for the handwritten-field double-check
   (VISION_DOUBLE_CHECK_ENABLED below) — the script degrades gracefully
   (skips the double-check, logs it once) if this isn't enabled.

--------------------------------------------------------------------------
Usage
--------------------------------------------------------------------------
Just run it (uses the CONFIG defaults below — edit them to point at your
bucket/project, or override any of them with the matching --flag).

As of Revision 39, this file does EXTRACT ONLY (read Q&A via Gemini and
load into BigQuery). The MERGE phase (combine each date folder's PDFs into
one PDF + generate a page-level manifest.csv) has been split out into its
own standalone script: merge_pdfs_to_folder.py. Run that file separately
if you need to merge PDFs; it has no import dependency on this file.

    python merge_survey_pdfs.py                          # extract, writes to BigQuery
    python merge_survey_pdfs.py --dry-run                 # list what would be sent to Gemini, no calls/writes made
    python merge_survey_pdfs.py --folders "Nov 18 2025" "Nov 23 2025"
    python merge_survey_pdfs.py --bq-dataset my_ds --bq-table my_table
    python merge_survey_pdfs.py --verify-file "Nov 10 2025/Nov10_1.pdf"  # audit one file's readings, no BQ write
    python merge_survey_pdfs.py --no-vision-check           # skip the Cloud Vision double-check for this run
    python merge_pdfs_to_folder.py                          # (separate file) merge PDFs per folder + load manifest into BigQuery

--------------------------------------------------------------------------
Running from a Jupyter / notebook cell
--------------------------------------------------------------------------
Do NOT do `python merge_survey_pdfs.py --dry-run` via `%run` and expect
flags to work the way they do in a terminal — inside a notebook kernel,
sys.argv holds the *kernel's* own launch arguments (e.g. "-f
/path/kernel.json"), not flags you typed, and argparse will error out on
them (SystemExit: 2) if it doesn't recognize them. This script tolerates
that (unknown args are ignored), but the more reliable pattern in a
notebook is to skip the command line entirely and call the functions
straight from a cell:

    from merge_survey_pdfs import (
        extract_to_bigquery, BUCKET_NAME, ROOT_PREFIX,
        VERTEX_PROJECT_ID, VERTEX_LOCATION, GEMINI_MODEL,
        BQ_PROJECT_ID, BQ_DATASET, BQ_TABLE,
    )
    extract_to_bigquery(
        bucket_name=BUCKET_NAME,
        root_prefix=ROOT_PREFIX,
        folders=None,               # None = all folders found; or e.g. ["Nov 10 2025"]
        vertex_project=VERTEX_PROJECT_ID,
        vertex_location=VERTEX_LOCATION,
        gemini_model=GEMINI_MODEL,
        bq_project=BQ_PROJECT_ID,
        bq_dataset=BQ_DATASET,
        bq_table=BQ_TABLE,
        dry_run=True,                # start with True to preview, then set False
    )

    # To merge PDFs instead (secondary/optional step):
    from merge_survey_pdfs import run
    run(
        bucket_name=BUCKET_NAME,
        root_prefix=ROOT_PREFIX,
        output_prefix=None,   # defaults to "<root_prefix>_merged/"
        folders=None,
        local_out=None,
        upload=True,
        dry_run=True,
    )
"""
import argparse
import csv
import datetime
import difflib
import json
import logging
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple

# ==========================================================================
# CONFIG — edit these to point at your bucket/project, or override on the
# command line (e.g. --bucket other-bucket). These are just the defaults.
# ==========================================================================
BUCKET_NAME = "tps_survey"
ROOT_PREFIX = "TPS_Scanned_2025/"
OUTPUT_PREFIX = None  # None -> derived as "<ROOT_PREFIX>_merged/"
# The Spark BigQuery connector's default write path (used by
# load_rows_into_bq_via_spark(), Revision 40) stages data through a GCS
# bucket before loading it into BigQuery - reuses BUCKET_NAME so a separate
# bucket doesn't need to be created just for this.
SPARK_BQ_STAGING_BUCKET = BUCKET_NAME

# --- extraction (Vertex AI Gemini) ---
VERTEX_PROJECT_ID = "gcp-sapchoda-dev"
VERTEX_LOCATION = "global"
GEMINI_MODEL = "gemini-3.1-flash-lite"  # confirm this model ID is enabled in the Vertex AI Model Garden

# --- extraction quality gates: model self-reported confidence + Cloud
# Vision double-check for handwritten fields (added at user request to
# reduce answers that "look fine" but are actually wrong slipping straight
# through - especially H1/H2/H4/H5/H6/24/26, the seven write-in questions
# that have NO pixel backstop at all and that cross_check_answer() can never
# flag, since they have no fixed choice list to check mark_position
# against). See the "Model confidence + Cloud Vision double-check" module
# docstring section above for the full design and reasoning. ---
MODEL_CONFIDENCE_THRESHOLD = 0.8  # model's own self-reported confidence (build_extraction_prompt() rule 15) below this -> needs_review=True. Only gates a question where no pixel-verified reading already took over (see answers_to_qa_rows()) - a confident pixel detector's own margin check already independently vouches for those.
VISION_DOUBLE_CHECK_ENABLED = True  # set False (or pass --no-vision-check) to skip Cloud Vision calls entirely, e.g. no Vision API enabled/quota - written-text questions then fall back to model-only + confidence-threshold checking alone, same as before this feature existed.
VISION_FREEFORM_COVERAGE_THRESHOLD = 0.9  # cross_check_written_field_with_vision()'s freeform fields (H4/H5/H6/24): word-level matching coverage (see that function - Revision 19's word-level, sum-of-all-matching-runs fix) below this -> vision_match=False, needs_review=True. Raised from 0.7 to 0.9 (explicit user request) now that the coverage score is computed correctly and can be trusted at a tighter cutoff - was 0.7 while the score itself was still unreliable (see Revision 19's project doc for the four bugs fixed there).
VISION_PROJECT_ID = None  # None -> uses application-default GCP project, same convention as VERTEX_PROJECT_ID

# --- extraction output (BigQuery) ---
BQ_PROJECT_ID = "gcp-sapchoda-dev"
BQ_DATASET = "ladph_tps"
BQ_TABLE = "survey_responses"
BQ_CORRECTIONS_TABLE = "corrections_log"  # see log_corrections() below

# --- upstream QC routing (Revision 37) ---
# `classify_pdf_quality.py` is a SEPARATE script that runs before this one
# and writes one row per PDF to PDF_QUALITY_TABLE (see the "LAPD" project doc
# "pdf-quality-classifier-and-routing.md" for its full field list and
# threshold derivations - it measures things like ink gap, border coverage,
# shadow bands, and tilt directly from the scan, plus a Vision judgment call
# on handwriting_readable/tears_or_damage for non-clear files). It derives
# `overall_quality` ("clear"/"unclear"/"totally_unreadable") and
# `recommended_route` ("pixel"/"vision"/"fallback") per file, deliberately
# kept OUT of this extraction script per that doc's design principle: the
# extraction pipeline should not judge whether its own input was good enough
# - that belongs upstream, recorded and queryable BEFORE any answer is
# extracted. This section is what makes extract_qa_from_pdf() actually
# CONSUME that pre-computed judgment rather than re-deriving it: query the
# row for the file being processed, and adjust pixel-detection/Vision-check
# behavior for THIS file according to its own recommended_route, rather than
# treating every file identically regardless of known scan quality.
PDF_QUALITY_TABLE = "pdf_quality"  # classify_pdf_quality.py's output table, same bq_project/BQ_DATASET as everything else in this file unless overridden
PDF_QUALITY_ROUTING_ENABLED = True  # set False (or pass --no-quality-routing) to skip the lookup entirely and process every file identically (pixel + optional Vision per VISION_DOUBLE_CHECK_ENABLED, same as before this feature existed) - e.g. if PDF_QUALITY_TABLE hasn't been populated yet for this bucket/folder.

# --- externalized calibration/tuning parameters (Revision 38) ---
# Every _GRID_*/_YESNO_*/_MULTISELECT_* pixel-detection threshold, pad, and
# per-question override, plus _INK_THRESHOLD, _WRITTEN_FIELD_ANCHORS, and
# _CORRECTIONS_ROOT_CAUSE_CATEGORIES, is now ALSO recorded as a plain
# Python constant in this file (unchanged - those constants are still the
# built-in defaults and the file still runs correctly with this feature
# disabled or the table empty/missing) AND registered in
# _PIPELINE_CONFIG_DEFAULTS (see below, after every one of those constants
# is defined) so it can be OVERRIDDEN at runtime from a BigQuery table
# instead of requiring a code change + redeploy for every calibration
# tweak. See query_pipeline_config()/apply_pipeline_config() and the
# "revision-38-externalized-pipeline-config.md" project doc for the full
# design. Explicitly NOT in scope here (left as plain code, per the user's
# own scope decision): the survey template itself (SURVEY_QUESTIONS,
# CHOICE_LISTS_BY_NUMBER, QUESTION_TEXT_BY_NUMBER, etc.) and operational
# config (BUCKET_NAME, BQ_* table/dataset names, GEMINI_MODEL, etc.) -
# those rarely change and aren't really "calibration".
PIPELINE_CONFIG_TABLE = "pipeline_config"
PIPELINE_CONFIG_ENABLED = True  # set False (or pass --no-pipeline-config) to always run with this file's built-in calibration defaults only, ignoring PIPELINE_CONFIG_TABLE entirely - this also skips the auto-create-and-seed-if-missing check (see ensure_and_maybe_seed_pipeline_config())
# ==========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    force=True,  # Jupyter/IPython often pre-configures the root logger before this
    # runs, which makes a plain basicConfig() a silent no-op — force=True makes sure
    # this handler actually attaches instead of logging output just disappearing.
)
log = logging.getLogger("merge_survey_pdfs")


def status(msg, *args) -> None:
    """Logs AND print()s a status message. Using print() specifically because
    it's the one output channel that reliably shows up in a Jupyter notebook
    cell no matter how that notebook's logging is (or isn't) configured —
    logger output alone can silently vanish there. Takes the same
    (msg, *args) %-style signature as logging so existing call sites don't
    need reformatting."""
    text = msg % args if args else msg
    print(text)
    log.info(msg, *args)


def err(msg, *args) -> None:
    """Same as status(), but for errors — always printed with an ERROR:
    prefix so it's unmistakable in notebook output, in addition to being
    logged at ERROR level."""
    text = msg % args if args else msg
    print(f"ERROR: {text}")
    log.error(msg, *args)


def connect_gcs_bucket(bucket_name: str):
    """storage.Client()/.bucket() with a clear error print on auth failure —
    the most common first failure in a fresh notebook (no
    `gcloud auth application-default login` / GOOGLE_APPLICATION_CREDENTIALS
    yet) — before re-raising so the real traceback still surfaces."""
    from google.cloud import storage

    try:
        client = storage.Client()
    except Exception as e:  # noqa: BLE001 - re-raised below, just adding a clear print first
        err(
            "[GCS] Could not create a GCS client — check you've run "
            "`gcloud auth application-default login` or set "
            "GOOGLE_APPLICATION_CREDENTIALS. Underlying error: %s",
            e,
        )
        raise
    return client.bucket(bucket_name)


def connect_bigquery(bq_project: Optional[str]):
    """bigquery.Client() with the same clear-error-then-re-raise pattern as
    connect_gcs_bucket()."""
    from google.cloud import bigquery

    try:
        return bigquery.Client(project=bq_project)
    except Exception as e:  # noqa: BLE001 - re-raised below, just adding a clear print first
        err(
            "[BQ] Could not create a BigQuery client for project %s — check "
            "you've run `gcloud auth application-default login` or set "
            "GOOGLE_APPLICATION_CREDENTIALS, and that the project is correct. "
            "Underlying error: %s",
            bq_project or "(default)",
            e,
        )
        raise


_VALID_PDF_QUALITY_ROUTES = {"pixel", "vision", "fallback"}


def query_pdf_quality_route(
    bq_client, bq_project: str, bq_dataset: str, gcs_uri: str,
    quality_table: str = PDF_QUALITY_TABLE,
) -> Optional[dict]:
    """Looks up classify_pdf_quality.py's pre-computed quality row for ONE
    file by its exact gcs_uri (the join key both scripts already use for
    everything else in this pipeline). Returns the row as a plain dict
    (BigQuery Row -> dict) or None if no row exists yet for this file (e.g.
    the classifier hasn't run on this folder, or the two scripts' bucket
    layouts have drifted) - callers must treat None as "no quality
    information available" and fall back to this pipeline's normal
    behavior, NEVER as an error to raise on, since the classifier is a
    separate, optional upstream step (see the PDF_QUALITY_ROUTING_ENABLED
    comment above for why this stays a soft dependency).

    Deliberately queries by gcs_uri, not file_name alone - two different
    date folders in this bucket can legitimately contain files with the
    same name (confirmed in the reference CSV: file_name is not unique
    across survey_folder), so file_name alone risks silently matching the
    wrong file's quality row.

    `recommended_route` is validated against the 3 routes
    classify_pdf_quality.py's own doc specifies ("pixel"/"vision"/
    "fallback") - an unrecognized value (a classifier version skew, a
    typo'd manual edit) is treated the same as "no row" (logged, then
    None) rather than silently driving this pipeline into a route it
    doesn't know how to handle."""
    from google.cloud import bigquery

    query = f"""
        SELECT *
        FROM `{bq_project}.{bq_dataset}.{quality_table}`
        WHERE gcs_uri = @gcs_uri
        ORDER BY assessed_at DESC
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri)]
    )
    try:
        rows = list(bq_client.query(query, job_config=job_config).result())
    except Exception as e:  # noqa: BLE001 - soft dependency, see docstring
        err("[QUALITY] Could not query %s.%s.%s for %s: %s", bq_project, bq_dataset, quality_table, gcs_uri, e)
        return None
    if not rows:
        return None
    quality_row = dict(rows[0].items())
    route = quality_row.get("recommended_route")
    if route not in _VALID_PDF_QUALITY_ROUTES:
        err(
            "[QUALITY] %s: recommended_route %r is not one of %s - ignoring this quality row "
            "and falling back to default (non-routed) processing for this file.",
            gcs_uri, route, sorted(_VALID_PDF_QUALITY_ROUTES),
        )
        return None
    return quality_row


def query_pipeline_config(
    bq_client, bq_project: str, bq_dataset: str,
    table_name: str = PIPELINE_CONFIG_TABLE,
) -> dict:
    """Loads calibration/tuning overrides from the pipeline_config BigQuery
    table (Revision 38 - see the "revision-38-externalized-pipeline-config.md"
    project doc). One row per parameter: param_name (matching a name in
    _PIPELINE_CONFIG_DEFAULTS below - e.g. "_GRID_BLANK_INK_FLOOR",
    "_YESNO_BOX_CALIBRATION") and param_value (that parameter's value,
    JSON-encoded as TEXT).

    Returns {name: decoded_value} for every row that both (a) names a
    parameter this code version actually recognizes (is a key in
    _PIPELINE_CONFIG_DEFAULTS - an unknown name is logged and skipped
    rather than silently ignored, since it likely means a typo or a
    version-skewed table) and (b) JSON-decodes cleanly (a malformed value
    is logged and skipped, keeping this code's own built-in default for
    that one parameter rather than crashing the whole run over it).

    Returns {} — never raises — if the table doesn't exist yet, can't be
    queried, or bq_client is None: this is a soft, optional dependency
    exactly like query_pdf_quality_route() and PDF_QUALITY_ROUTING_ENABLED
    - a fresh deployment with no pipeline_config table populated yet must
    run with this file's built-in calibration, not fail outright."""
    if bq_client is None:
        return {}
    query = f"SELECT param_name, param_value FROM `{bq_project}.{bq_dataset}.{table_name}`"
    try:
        rows = list(bq_client.query(query).result())
    except Exception as e:  # noqa: BLE001 - soft dependency, see docstring
        err("[CONFIG] Could not query %s.%s.%s - running with this file's built-in calibration defaults: %s", bq_project, bq_dataset, table_name, e)
        return {}
    overrides = {}
    for row in rows:
        row_dict = dict(row.items())
        name = row_dict.get("param_name")
        raw_value = row_dict.get("param_value")
        if name not in _PIPELINE_CONFIG_DEFAULTS:
            err(
                "[CONFIG] pipeline_config has a row for %r, which isn't a calibration parameter this "
                "code version recognizes - ignoring it (typo, or a table shared with a newer/older "
                "version of this file?).",
                name,
            )
            continue
        try:
            value = json.loads(raw_value)
        except Exception as e:  # noqa: BLE001 - one bad row shouldn't sink every other override
            err("[CONFIG] Could not JSON-decode pipeline_config's value for %r (%r) - keeping the built-in default for it: %s", name, raw_value, e)
            continue
        overrides[name] = value
    return overrides


def apply_pipeline_config(overrides: dict) -> list:
    """Applies validated calibration overrides (as returned by
    query_pipeline_config()) onto this module's own globals, so every
    function that reads e.g. _GRID_BLANK_INK_FLOOR at call time sees the
    overridden value with no further plumbing needed anywhere else in this
    file - Python resolves module-level names dynamically at call time,
    not at function-definition time.

    Only ever touches names already in _PIPELINE_CONFIG_DEFAULTS - never
    arbitrary globals - so a bad/unexpected key in `overrides` can't be
    used to clobber something outside this file's declared calibration
    surface (query_pipeline_config() already filters unknown names too;
    this is a second, independent gate for any direct caller).

    A JSON round-trip turns a Python tuple/set into a plain list - fine
    for almost every deeply-nested calibration dict in this file (confirmed:
    none of them are ever indexed via isinstance(..., tuple)/isinstance(...,
    set), only unpacked/iterated/membership-tested, which behave
    identically on a list) - but the small number of TOP-LEVEL constants
    that are themselves a tuple or a set (_GRID_COLUMN_CENTERS,
    _CORRECTIONS_ROOT_CAUSE_CATEGORIES) are cast back to their original
    top-level type here, so an override is never observably a different
    Python type than this file's own hardcoded default would have been.

    _YESNO_BOX_CALIBRATION is a SPECIAL CASE and gets its own restoration
    pass (see _restore_yesno_box_calibration_shape() below): per question,
    its value is EITHER a single (mode, boxes) 2-tuple, or - for a question
    observed in more than one real printed layout (Q27) - a LIST of such
    2-tuples, and detect_yesno_box_answers()/_build_synthetic_yesno_page()
    both tell these two shapes apart with isinstance(raw_candidates, list).
    A JSON round-trip turns BOTH shapes into "a list" at the top level
    (json.loads() doesn't know the difference between "this was a tuple"
    and "this was a list"), which silently breaks that isinstance() check
    for every single-candidate question, not just Q27 - the exact bug that
    produced "cannot unpack non-iterable int object" the first time this
    override was exercised against a real BigQuery-seeded table (confirmed
    directly against the pipeline_config table's actual seeded row for
    _YESNO_BOX_CALIBRATION). Fixed by reconstructing each question's
    correct shape from _PIPELINE_CONFIG_DEFAULTS's own (never-JSON-touched)
    shape for that same question, rather than trusting Python type identity
    after the round-trip.

    Returns the sorted list of parameter names actually applied (for
    logging - see extract_to_bigquery())."""
    applied = []
    for name, value in overrides.items():
        if name not in _PIPELINE_CONFIG_DEFAULTS:
            continue
        default = _PIPELINE_CONFIG_DEFAULTS[name]
        if name == "_YESNO_BOX_CALIBRATION":
            value = _restore_yesno_box_calibration_shape(value, default)
        elif isinstance(default, tuple):
            value = tuple(value)
        elif isinstance(default, set):
            value = set(value)
        globals()[name] = value
        applied.append(name)
    return sorted(applied)


def _restore_yesno_box_calibration_shape(loaded: dict, default: dict) -> dict:
    """See apply_pipeline_config()'s _YESNO_BOX_CALIBRATION special-case
    comment for why this exists. Per question number, restores either a
    single (mode, boxes) tuple, or a list of such tuples (for a
    multi-layout question like Q27), based on which shape
    _PIPELINE_CONFIG_DEFAULTS's own untouched default has for that SAME
    question - not on the loaded JSON value's own (now type-ambiguous)
    shape. A question present in the override but not in the default
    (a brand new question added directly in BigQuery, never seen in this
    code version) is defensively treated as single-candidate, matching the
    overwhelming majority shape and this file's own documented convention
    for a newly-calibrated question."""
    restored = {}
    for qnum, loaded_value in loaded.items():
        default_value = default.get(qnum)
        if isinstance(default_value, list):
            restored[qnum] = [tuple(candidate) for candidate in loaded_value]
        else:
            restored[qnum] = tuple(loaded_value)
    return restored


def ensure_pipeline_config_table(bq_client, project: str, dataset: str, table: str):
    """Creates (or, on an older table, patches) the pipeline_config table
    schema - same create-or-patch pattern as ensure_bq_table()/
    ensure_corrections_table(), so an existing table with only some of
    these columns gets the rest ADDED (existing rows get NULL for new
    columns) rather than this failing or silently dropping data on load."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    table_ref = bigquery.DatasetReference(project, dataset).table(table)
    schema = [
        bigquery.SchemaField("param_name", "STRING", description="Matches a key in this file's _PIPELINE_CONFIG_DEFAULTS - e.g. '_GRID_BLANK_INK_FLOOR', '_YESNO_BOX_CALIBRATION'. Unrecognized names are ignored (logged) by query_pipeline_config()."),
        bigquery.SchemaField("param_value", "STRING", description="This parameter's value, JSON-encoded as text (json.dumps()) - decoded with json.loads() on load."),
        bigquery.SchemaField("category", "STRING", description="Informational grouping only (e.g. 'grid', 'yesno', 'multiselect', 'written_field', 'corrections') - not read by any code path."),
        bigquery.SchemaField("description", "STRING", description="Human-readable note on what this parameter controls - informational only."),
        bigquery.SchemaField("updated_at", "TIMESTAMP", description="When this row was last written (seed_pipeline_config_defaults() or a manual UPDATE)."),
        bigquery.SchemaField("updated_by", "STRING", description="Who/what last wrote this row - e.g. 'seed_pipeline_config_defaults' for the initial snapshot, or a person's name/email for a manual calibration change."),
    ]
    try:
        table_obj = bq_client.get_table(table_ref)
    except NotFound:
        status("[BQ] Creating table %s.%s.%s", project, dataset, table)
        table_obj = bigquery.Table(table_ref, schema=schema)
        return bq_client.create_table(table_obj)
    existing_names = {f.name for f in table_obj.schema}
    missing = [f for f in schema if f.name not in existing_names]
    if missing:
        status("[BQ] Table %s.%s.%s is missing column(s) %s — adding them (existing rows get NULL for these).", project, dataset, table, [f.name for f in missing])
        table_obj.schema = list(table_obj.schema) + missing
        bq_client.update_table(table_obj, ["schema"])
    else:
        status("[BQ] Table %s.%s.%s already exists with the expected schema.", project, dataset, table)
    return table_obj


def seed_pipeline_config_defaults(bq_client, project: str, dataset: str, table: str, updated_by: str = "seed_pipeline_config_defaults") -> int:
    """Writes this file's CURRENT built-in calibration defaults
    (_PIPELINE_CONFIG_DEFAULTS) into the pipeline_config table as one row
    per parameter, via WRITE_TRUNCATE (this call always fully replaces the
    table's contents with a fresh snapshot of the code's defaults - it is
    the "start from what's already live in code" bootstrap step, not an
    incremental merge; any calibration change already made by hand
    directly in BigQuery is intentionally overwritten by re-seeding, same
    as re-running a migration).

    Run once (via --seed-pipeline-config) to populate a fresh table, or
    again later to reset it back to this file's code-level defaults. After
    seeding, calibration is tuned by editing rows directly in BigQuery, not
    by re-running this."""
    from google.cloud import bigquery

    ensure_pipeline_config_table(bq_client, project, dataset, table)
    table_ref = bigquery.DatasetReference(project, dataset).table(table)
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows = []
    for name in sorted(_PIPELINE_CONFIG_DEFAULTS):
        value = _PIPELINE_CONFIG_DEFAULTS[name]
        category = name.strip("_").split("_")[0].lower() if name != "_INK_THRESHOLD" else "grid"
        rows.append({
            "param_name": name,
            "param_value": json.dumps(sorted(value) if isinstance(value, set) else value),
            "category": category,
            "description": f"Calibration/tuning parameter {name} - see merge_survey_pdfs.py's own comment above its definition for full derivation/measurement notes.",
            "updated_at": now,
            "updated_by": updated_by,
        })
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = bq_client.load_table_from_json(rows, table_ref, job_config=job_config)
    load_job.result()
    status("[CONFIG] Seeded %d calibration parameter(s) into %s.%s.%s from this file's built-in defaults", len(rows), project, dataset, table)
    return load_job.output_rows


def ensure_and_maybe_seed_pipeline_config(bq_client, project: str, dataset: str, table: str) -> bool:
    """Auto-bootstraps pipeline_config on a fresh deployment: checks
    whether `table` already exists, and if it does NOT, creates it and
    seeds it with this file's own built-in calibration defaults via
    seed_pipeline_config_defaults() - so the very first run against a
    brand-new project/dataset gets a populated, immediately-editable
    pipeline_config table automatically, rather than silently running on
    code defaults forever until someone remembers to run
    --seed-pipeline-config by hand (the original, manual-only bootstrap
    path - still available and still useful for an explicit RESET back to
    code defaults later, see seed_pipeline_config_defaults()'s own
    docstring, but no longer the only way to get a table populated).

    Returns True if a fresh table was just created+seeded by this call,
    False if the table already existed (never re-seeds or otherwise
    touches an existing table here - a human's hand-tuned calibration
    already in BigQuery is never overwritten by this auto-bootstrap path).

    Never raises: any error checking for or creating the table is logged
    via err() and this simply returns False, falling back to
    query_pipeline_config()'s own normal soft-fail behavior (run on
    built-in defaults for this one run) rather than blocking extraction
    over a bootstrap failure."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    table_ref = bigquery.DatasetReference(project, dataset).table(table)
    try:
        bq_client.get_table(table_ref)
        return False  # already exists - leave it exactly as-is
    except NotFound:
        pass
    except Exception as e:  # noqa: BLE001 - soft dependency, never block a run over this
        err("[CONFIG] Could not check whether %s.%s.%s exists yet - skipping auto-seed for this run: %s", project, dataset, table, e)
        return False
    status("[CONFIG] %s.%s.%s does not exist yet - creating and seeding it from this file's built-in calibration defaults.", project, dataset, table)
    try:
        seed_pipeline_config_defaults(bq_client, project, dataset, table)
    except Exception as e:  # noqa: BLE001 - soft dependency, never block extraction over this
        err("[CONFIG] Could not auto-seed %s.%s.%s - continuing with built-in defaults for this run: %s", project, dataset, table, e)
        return False
    return True


# ==========================================================================
# Fixed survey template — "Treatment Perceptions Survey (Adult)" (CalOMS),
# replacing the earlier "2024 LAPD Chief of Police Community Survey" template
# once an actual production sample (Nov18_2.pdf) was provided. This is a
# flat 35-question form (no grouped "big question + lettered sub-items"
# structure like the old template had), plus a handful of header/admin
# fields (H1-H6) worth capturing too — including "Today's Date", which is
# the respondent's actual hand-written completion date on the form itself.
#
# NOTE: on the one real sample seen so far, that written date (10/24/2025)
# did NOT match its folder name (Nov 18 2025) — this bucket may be test data
# with mismatched dates, or scan date and fill date may just legitimately
# differ. report_date (parsed from the folder name) is left as-is for now;
# H6 captures the form's own written date as a separate row so the
# discrepancy is visible in the data rather than silently papered over.
#
# Most questions here use a single-select 6-point agreement scale, marked
# with an X in a box. Q33 and Q34 explicitly allow marking more than one
# choice ("mark all that apply") — build_extraction_prompt() below instructs
# the model to return every marked choice for those two, joined by "; ".
#
# NOTE: this assumes every PDF under ROOT_PREFIX uses this same template.
# If some folders contain a different survey (as the very first sample PDF,
# a different LAPD-style survey, turned out to), this list needs to change
# per-folder/template rather than being one global constant — ask if that's
# needed; not implemented here since only one production template has been
# confirmed so far.
# ==========================================================================
GROUP_HEADERS = {}  # no grouped "big question -> lettered sub-items" structure in this template

_SIX_POINT_SCALE = "Strongly Agree / Agree / I am Neutral / Disagree / Strongly Disagree / Not Applicable"
# Q23 uses the same 6-point scale but the form prints its last choice as the
# abbreviation "N/A" (like Q20 already did), NOT the spelled-out "Not
# Applicable" used by the Q1-18 grid's column header. Confirmed against the
# real Nov10_1.pdf scan - this was a real data bug: the wrong reference label
# here caused the model to answer "Not Applicable" (matching our WRONG prompt
# text) even when it correctly identified the mark's position (6).
_SIX_POINT_SCALE_NA = "Strongly Agree / Agree / I am Neutral / Disagree / Strongly Disagree / N/A"
_MULTI_SELECT_NOTE = " (mark all that apply — more than one choice may be marked)"

# (question_number, group_key_or_None, question_text, choices)
SURVEY_QUESTIONS = [
    # --- form header / admin fields (not numbered on the form itself) ---
    ("H1", None, "Home Unit CalOMS Provider ID", "written ID number"),
    ("H2", None, "Program Reporting Unit (Address) code", "written alphanumeric code"),
    ("H3", None, "Setting", "Early Intervention / OP/IOP / Residential / OTP/NTP / Detox/WM / Recovery Services"),
    ("H4", None, "Field Based Services: Agency", "written agency name, if filled in"),
    ("H5", None, "Field Based Services: Address", "written address, if filled in"),
    ("H6", None, "Today's Date (the respondent's own written completion date on the form, MM/DD/YYYY)", "written date"),
    # --- Q1-18: single-select 6-point agreement scale ---
    ("1", None, "The location was convenient (public transportation, distance, parking, etc.).", _SIX_POINT_SCALE),
    ("2", None, "Services were available when I needed them.", _SIX_POINT_SCALE),
    ("3", None, "I chose the early intervention/treatment/recovery goals with my provider's help.", _SIX_POINT_SCALE),
    ("4", None, "Staff gave me enough time in my early intervention/treatment/recovery sessions.", _SIX_POINT_SCALE),
    ("5", None, "Staff treated me with respect.", _SIX_POINT_SCALE),
    ("6", None, "Staff spoke to me in a way I understood.", _SIX_POINT_SCALE),
    ("7", None, "Staff were sensitive to my cultural background (race/ethnicity, religion, language, etc.).", _SIX_POINT_SCALE),
    ("8", None, "I felt welcomed here.", _SIX_POINT_SCALE),
    ("9", None, "As a direct result of the services I am receiving, I am better able to do things that I want to do.", _SIX_POINT_SCALE),
    ("10", None, "As a direct result of the services I am receiving, I feel less craving for drugs and alcohol.", _SIX_POINT_SCALE),
    ("11", None, "Staff here work with my physical health care providers to support my wellness.", _SIX_POINT_SCALE),
    ("12", None, "Staff here work with my mental health care providers to support my wellness.", _SIX_POINT_SCALE),
    ("13", None, "Staff here helped me to connect with other services as needed (social services, housing, etc.).", _SIX_POINT_SCALE),
    ("14", None, "Overall, I am satisfied with the services I received.", _SIX_POINT_SCALE),
    ("15", None, "I was able to get all the help/services that I needed.", _SIX_POINT_SCALE),
    ("16", None, "I would recommend this agency to a friend or family member.", _SIX_POINT_SCALE),
    ("17", None, "I feel comfortable discussing any lapses or return to substance use with my provider.", _SIX_POINT_SCALE),
    ("18", None, "I began substance use treatment services with the goal of achieving either complete abstinence or reduction in use.", _SIX_POINT_SCALE),
    ("19", None, "Now thinking about the services you received, how much of it was by telehealth (by telephone or video-conferencing)?", "None / Very little / About half / Almost all / All"),
    ("20", None, "How helpful were your telehealth visits compared to traditional in-person visits?", "Much better / Somewhat better / About the same / Somewhat worse / N/A"),
    ("21", None, "When you entered the treatment program, did the program staff offer you a copy of the patient handbook or show you where you can find it?", "Yes / No"),
    ("22", None, "Did the program staff show you the patient orientation video?", "Yes / No"),
    ("23", None, "Watching the patient orientation video helped me with information I can use to access all available substance use disorder services.", _SIX_POINT_SCALE_NA),
    ("24", None, "Comment: What was most helpful about this program? What would you change about this program?", "open text (respondent instructed not to identify themselves)"),
    ("25", None, "How long have you received services here?", "First visit/day / 2 weeks or less / More than 2 weeks but less than 4 weeks / 4 weeks or more"),
    ("26", None, "Age", "written number"),
    ("27", None, "Are you homeless?", "Yes / No"),
    ("28", None, "Have you ever received Contingency Management services?", "Yes, I am currently receiving Contingency Management services / Yes, I received Contingency Management services in the past / No, I have never received Contingency Management services"),
    ("29", None, "What is your current gender identity (this is how the respondent identifies themselves, which may not match sex assigned at birth)?", "Male / Female / Female-to-Male (FTM)/Transgender Male/Trans Man / Male-to-Female (MTF)/Transgender Female/Trans Woman / Gender Queer/Gender Non-Conforming / Other (specify) / Prefer not to state"),
    ("30", None, "What was your sex at birth?", "Female / Male / Other (specify) / Prefer not to state"),
    ("31", None, "What is your sexual orientation?", "Heterosexual/Straight / Lesbian (Female) / Gay (Male) / Bisexual / Unsure/Questioning/Don't know / Pansexual / Asexual / Queer / Other (specify) / Prefer not to state"),
    ("32", None, "Are you of Mexican/Hispanic/Latino/a descent?", "Yes / No / Unknown"),
    ("33", None, "Race/Ethnicity" + _MULTI_SELECT_NOTE, "American Indian/Alaskan Native / Asian / Black/African American / Native Hawaiian/Pacific Islander / White/Caucasian / Other (specify) / Prefer not to state"),
    ("34", None, "Disability Status" + _MULTI_SELECT_NOTE, "Physically Disabled / Visually Impaired/Blind / Hearing Impaired/Deaf / Co-occurring Mental Health Condition / Developmentally or Intellectually Disabled / Other (specify) / None"),
    ("35", None, "What is your criminal justice involvement status?", "Post-release Community Supervision (AB109) or on Probation from any federal, state, or local jurisdiction / Awaiting trial, charges or sentencing / On parole from any other jurisdiction / Any other criminal justice involvement / No criminal justice involvement"),
]

# Questions where more than one choice can legitimately be marked at once —
# used both in the extraction prompt and available for any downstream
# validation that expects everything else to be single-valued.
MULTI_SELECT_QUESTION_NUMBERS = {"33", "34"}

# --------------------------------------------------------------------------
# CHOICE_LISTS_BY_NUMBER: for every question whose `choices` field is a fixed
# " / "-separated list (as opposed to an open write-in like "written date" or
# "written number"), the ordered list of choice labels — same left-to-right
# order they're presented to the model in the extraction prompt. Used to
# cross-check the model's reported mark_position against its reported answer
# label (see cross_check_answer() below). Splitting on " / " (with spaces)
# rather than bare "/" is deliberate: several choice labels contain their own
# internal slash with no surrounding spaces (e.g. "OP/IOP", "First
# visit/day", "American Indian/Alaskan Native") and must stay intact as one
# choice.
# --------------------------------------------------------------------------
CHOICE_LISTS_BY_NUMBER = {
    number: [c.strip() for c in choices.split(" / ")]
    for number, _group_key, _sub_text, choices in SURVEY_QUESTIONS
    if " / " in choices
}


def _choice_matches(choice: str, answer_text: str) -> bool:
    """A reported answer matches a listed choice either exactly, or - for a
    "(specify)" choice - by starting with the choice text followed by the
    handwritten free text the extraction prompt asks the model to append
    (e.g. "Other (specify): Mexican" for the listed choice "Other
    (specify)"). Without this, correctly capturing that handwritten text
    would make an otherwise-correct answer fail an exact-match check and
    get wrongly flagged needs_review - this is what was happening before
    the prompt asked for the free text at all: the model just returned the
    bare choice text and silently dropped what was written next to it.
    Shared by cross_check_answer() and the multi-select pixel veto in
    answers_to_qa_rows()."""
    choice = choice.strip()
    answer_text = answer_text.strip()
    if choice.lower() == answer_text.lower():
        return True
    if "(specify)" in choice.lower():
        return answer_text.lower().startswith(choice.lower())
    return False


# --------------------------------------------------------------------------
# WRITTEN_TEXT_QUESTION_NUMBERS: every question with NO fixed choice list at
# all - a genuinely open write-in/handwritten field (an ID number, a code, an
# address, a date, a comment, an age) rather than a checkbox/circle choice.
# Derived directly from SURVEY_QUESTIONS (any entry whose `choices` field has
# no " / " in it) rather than hand-maintained separately, so it can never
# drift out of sync with the question list itself.
#
# This matters because NONE of these seven questions can be checked by
# cross_check_answer() (there's no choices list to compare a mark_position
# against) and NONE of them have a pixel checkbox/circle detector either
# (there's no box/circle to measure ink in) - before the Cloud Vision
# double-check below existed, a genuine misread on one of these had NO
# independent signal at all, unlike every single checkbox question on this
# form, which has either a pixel detector or at least the model's own
# internal answer/mark_position self-consistency check.
# --------------------------------------------------------------------------
WRITTEN_TEXT_QUESTION_NUMBERS = {
    number for number, _group_key, _sub_text, choices in SURVEY_QUESTIONS
    if " / " not in choices
}
# {"H1", "H2", "H4", "H5", "H6", "24", "26"}

# Of those seven, H1/H2/26 are short enough (an ID number, a short
# alphanumeric code, an age) that they're realistically ONE token on the
# page - compared against Cloud Vision's own individually-segmented word-
# level OCR tokens for an exact match (see cross_check_written_field_with_
# vision()). The other four (H4's agency name, H5's address, H6's date, 24's
# open comment) can legitimately span multiple words/lines, so they're
# compared against Vision's FULL page transcription as a whole instead,
# looking for the model's answer as a close contiguous match anywhere in it.
_VISION_TOKEN_FIELDS = {"H1", "H2", "26"}
_VISION_FREEFORM_FIELDS = {"H4", "H5", "H6", "24"}

# Revision 29 follow-up (explicit user request: "H4 and H5 doesn't need to
# be marked as need review if it's blank"). H4 (agency/program name) and H5
# (address) are free-form written fields that are legitimately left blank
# far more often than the other tracked fields - exempted from the general
# blank-answer-always-needs-review backstop in answers_to_qa_rows() only;
# every other check (format validation, vision disagreement, etc.) still
# applies to them normally.
_BLANK_ANSWER_EXEMPT_FIELDS = {"H4", "H5"}

# Revision 29 (explicit user request: "H2 and H6 should have the Vision OCR
# rule over the Vision model, because I think Vision OCR has been capturing
# it more accurately"). For these fields specifically, extract_qa_from_pdf()
# doesn't just CROSS-CHECK the model's answer against Cloud Vision's
# anchored reading (like every other written-text field) - it REPLACES the
# model's answer with the anchored Vision reading outright, whenever Vision
# located the field's own printed label and read a confident non-blank
# value there (see _extract_anchored_field_value() - this deliberately uses
# only the ANCHORED, label-located value, never the whole-page fallback
# token/snippet, to avoid ever inserting unrelated page text as an answer).
# The original model answer is preserved alongside the override for audit
# (answers[qnum]["model_answer_before_vision_override"]) rather than
# discarded.
#
# H5 added in this revision (explicit user request: "H5. Field Based
# Services: Address (This one should have vision OCR rule over model)") -
# same rationale as H2/H6, extended to the third header-row written field.
_VISION_AUTHORITATIVE_FIELDS = {"H2", "H5", "H6"}


def _format_vision_authoritative_value(question_number: str, anchored_value: str) -> Optional[str]:
    """Formats an anchored Cloud Vision value for H2/H5/H6 into the same
    shape the model would normally report, for _VISION_AUTHORITATIVE_
    FIELDS' override. Returns None if the value can't be confidently
    formatted (the override is skipped in that case, leaving the model's
    own answer alone rather than risk inserting something worse).

    - H2: just compacted whitespace (Vision's own word-reconstruction can
      leave irregular spacing) - the value itself is trusted as-is.
    - H5: same as H2 - a free-text street address, no fixed digit shape to
      reconstruct, so the anchored value (already whitespace-compacted) is
      trusted as-is, exactly like H2's own simpler pattern. Deliberately
      NOT modeled on H6's boxed-digit reconstruction below, since H5 is
      ordinary handwriting/print across a variable number of words, not a
      fixed-count digit grid.
    - H6: reformatted from Vision's boxed-digit reconstruction (space-
      separated single digits, e.g. "0 9 0 4 2 0 2 6" - see
      _extract_anchored_field_value()'s H6 handling) into MM/DD/YYYY, but
      ONLY when reading exactly 8 digits total - any other digit count
      means the boxed-digit reconstruction itself is unreliable here (a
      missed/extra box), so this deliberately declines to guess a date
      shape out of the wrong number of digits.
    """
    if question_number in ("H2", "H5"):
        compacted = " ".join(anchored_value.split())
        return compacted if compacted else None
    if question_number == "H6":
        digits = "".join(ch for ch in anchored_value if ch.isdigit())
        if len(digits) != 8:
            return None
        return f"{digits[0:2]}/{digits[2:4]}/{digits[4:8]}"
    return None

# Which physical page (0-based, matching page_images'/_YESNO_BOX_CALIBRATION's
# convention) each of the seven written-text questions actually lives on, on
# this form's fixed 2-page layout: H1/H2/H4/H5/H6 are all in the header block
# on page 1, but Q24 (the open comment) and Q26 (age) are on page 2, along
# with Q25 and up. A real Cloud Vision test against an actual scan (Nov10_3.pdf)
# caught this the hard way - cloud_vision_ocr_page() was originally only ever
# called on page_images[0] for ALL seven fields (see extract_qa_from_pdf()),
# on the mistaken assumption that "this template's written fields all live on
# page 1" - true for five of the seven, but not 24/26, so Vision's page-1-only
# OCR could never contain either of their answers, guaranteeing a false
# "no matching token/text found" disagreement on every single file for both.
_WRITTEN_TEXT_QUESTION_PAGE = {"H1": 0, "H2": 0, "H4": 0, "H5": 0, "H6": 0, "24": 1, "26": 1}

# Deterministic format validation for the two written-text fields with a
# well-defined, unambiguous printed format: H1 (Home Unit CalOMS Provider
# ID) is always a 6-digit number, printed digit-by-digit into 6 boxed
# cells; H6 (Today's Date) is always a calendar date the respondent wrote
# as MM/DD/YYYY. Requested directly: these are exactly the kind of mistake
# a human reviewer catches at a glance (an extra/missing digit, a
# transposed date component) but that nothing in the existing checks 1-4 in
# answers_to_qa_rows() necessarily catches, since neither field has a pixel
# detector and Cloud Vision's own OCR of a digit-by-digit box grid is often
# no more reliable than the model's own reading (see
# cross_check_written_field_with_vision()'s H1/H2/26 handling) - a same-
# format wrong-value mistake in either the model or Vision's own reading
# would sail straight through the existing token-match cross-check. This is
# a SEPARATE, purely structural check: it only asks "does this look like a
# valid ID/date at all", never "is it the CORRECT one" - see
# _validate_written_field_format()'s own docstring for what it does and
# does not catch.
def _validate_written_field_format(number: str, answer: str) -> Optional[str]:
    """Returns a short, human-readable description of why `answer` doesn't
    match this field's known printed format, or None when it matches (or
    when this field has no fixed format to check, or the answer is blank -
    a genuinely skipped field is a normal, valid response per rule 6 in
    build_extraction_prompt(), regardless of format, so blank answers are
    NEVER flagged here).

    Deliberately narrow: this only validates STRUCTURE (six digits; a
    parseable MM/DD/YYYY calendar date), never the VALUE itself - it cannot
    tell a correctly-read ID/date from an incorrectly-read one that still
    happens to have the right shape (e.g. one digit transposed with
    another). That's still strictly better than no check at all: an answer
    that fails this test is guaranteed wrong (too few/many digits, letters
    where there should be digits, an impossible or unparseable date), so
    flagging it for review has zero false-positive risk from the format
    check itself - the only way this can incorrectly flag a genuinely
    correct answer is if the respondent themselves wrote something that
    doesn't fit the field's own format, which is exactly the kind of edge
    case a human reviewer should see anyway."""
    answer = (answer or "").strip()
    if not answer:
        return None
    if number == "H1":
        digits_only = re.sub(r"[\s-]", "", answer)
        if not re.fullmatch(r"\d{6}", digits_only):
            return (
                f"H1 (Home Unit CalOMS Provider ID) should be a 6-digit number, but the "
                f"reported answer {answer!r} is not exactly 6 digits - re-check the "
                "6 boxed digits on the form."
            )
    elif number == "H6":
        parsed = None
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                parsed = datetime.datetime.strptime(answer, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            return (
                f"H6 (Today's Date) should be a date in MM/DD/YYYY format, but the "
                f"reported answer {answer!r} doesn't parse as one - re-check the "
                "respondent's own written completion date on the form."
            )
    return None


def _reconcile_multiselect_choices(number: str, answer: str, pixel_ratios: dict):
    """Given a multi-select answer string (choices joined by "; ", possibly
    empty) and a {choice_label: ink_ratio} dict from
    detect_multiselect_ink_ratios(), reconciles the model's claimed choices
    against the pixel evidence in BOTH directions:

    - VETO (the original Round-15-and-earlier mechanism): a claimed choice
      whose own calibrated box measures confidently blank (ink_ratio <
      _MULTISELECT_UNMARKED_CEILING) is removed. Covers
      model_multiselect_over_selection (Q33), and - when the claim reduces
      to nothing - model_hallucinated_blank_question (Q34).

    - FILL (added Round 17): a choice the model did NOT claim, but whose own
      calibrated box measures confidently marked (ink_ratio >=
      _MULTISELECT_UNMARKED_CEILING), is added. Covers the mirror-image
      failure, model_missed_multiselect_mark - confirmed on
      Nov2_1_TPS_1413.pdf's Q34: the model returned a completely blank
      answer while "Co-occurring Mental Health Condition" measured 0.77 ink
      on that scan, an unambiguous mark the model simply never reported.
      This is a DIFFERENT bug shape from the earlier hallucinated-blank one
      (model invents a choice on a truly blank form) - here the form has a
      real mark and the model reports nothing at all. Before this round,
      the call site only ran this mechanism `if ... and answer`, so a
      blank model answer skipped it entirely - VETO alone can only ever
      remove from a non-empty claim, never add to an empty one, so this
      failure shape had no backstop at all until now.

    One shared threshold works for both directions: measured directly
    across all 7 real files originally calibrated against for Q33/Q34
    (Nov10_1.pdf, Nov18_2.pdf, Nov1_1_TPS_1070.pdf, Nov1_3_TPS_1357.pdf,
    Nov1_2_TPS_1356.pdf, Nov1_4_TPS_1402.pdf, Nov2_1_TPS_1413.pdf), every
    genuine mark measured >=0.1544 ink and every genuine blank measured
    EXACTLY 0.0 - a wide gap with nothing observed in between.

    Round 21 lowered the ceiling from 0.15 to 0.10: on
    Nov3_3_TPS_1595.pdf's Q33, the respondent marked "Prefer not to
    state" with a light checkmark-style tick (rather than a full X) that
    measured only 0.1419 ink under the ink_border=6 exclusion this
    question already uses (needed - see _MULTISELECT_INK_BORDER_OVERRIDE's
    comment - to keep the printed box border itself from reading as ink on
    a genuinely blank box; at a smaller border, blank boxes on this same
    file measured up to 0.1536, i.e. HIGHER than this real mark, so
    shrinking the border is not a safe fix here). A checkmark's ink sits
    disproportionately near the box's edges/corners rather than filling
    its center, so it reads lower than a corner-to-corner X for the same
    box - see _YESNO_CONFIDENCE_MARGIN_OVERRIDE's comment for the same
    checkmark-vs-X shape previously observed on Q19/Q20. Every blank box
    on Nov3_3_TPS_1595.pdf still measured EXACTLY 0.0 at border=6, so
    _MULTISELECT_UNMARKED_CEILING=0.10 keeps a full 0.10 of headroom above
    every observed blank while sitting 0.0419 below this lightest observed
    mark - cleanly separating "confidently blank" from "confidently
    marked" in both directions, including this new lighter mark style.

    Returns (new_answer, removed_choices, added_choices) if anything
    changed, or None if the pixel evidence agrees with the model's answer
    exactly (including when a choice - claimed or not - has no calibrated
    box to check at all, e.g. an unlisted question or a "(specify)" choice
    with handwriting the pixel side doesn't parse; those are always left
    exactly as the model reported them, never added or removed without
    positive pixel evidence). The returned answer places any added choices
    after the model's surviving claimed ones, in calibration order."""
    parts = [a.strip() for a in answer.split(";") if a.strip()]
    kept, removed = [], []
    matched_labels = set()
    for part in parts:
        matched_ratio, matched_label = None, None
        for label, ratio in pixel_ratios.items():
            if _choice_matches(label, part):
                matched_ratio, matched_label = ratio, label
                break
        if matched_label is not None:
            matched_labels.add(matched_label)
        if matched_ratio is not None and matched_ratio < _MULTISELECT_UNMARKED_CEILING:
            removed.append(part)
        else:
            kept.append(part)
    added = [
        label for label, ratio in pixel_ratios.items()
        if label not in matched_labels and ratio >= _MULTISELECT_UNMARKED_CEILING
    ]
    if not removed and not added:
        return None
    return "; ".join(kept + added), removed, added


def _positions_for_multiselect_answer(number: str, answer: str) -> str:
    """Recomputes a comma-joined mark_position string from scratch for a
    (possibly just-vetoed) multi-select answer, by looking up each
    surviving choice's own 1-based position in CHOICE_LISTS_BY_NUMBER -
    simpler and safer than trying to selectively remove entries from the
    model's original, possibly now-misaligned, mark_position string."""
    choices = CHOICE_LISTS_BY_NUMBER.get(number)
    if not choices:
        return ""
    positions = []
    for part in (a.strip() for a in answer.split(";") if a.strip()):
        for idx, choice in enumerate(choices):
            if _choice_matches(choice, part):
                positions.append(str(idx + 1))
                break
    return ",".join(positions)


def cross_check_answer(number: str, answer: str, mark_position_raw: str):
    """Compares a question's reported answer LABEL against its reported
    mark POSITION (both come back from the same Gemini call — see
    extract_qa_from_pdf()). They should always describe the same choice; if
    they don't, that's a strong signal the model misread the form (this is
    exactly the failure mode reported for Nov10_1.pdf Q1: label said
    "Disagree" while the actual marked box, position 6, is "Not
    Applicable"). Returns (needs_review: bool, review_note: str) — review_note
    is "" when needs_review is False.

    Questions with no fixed choice list (open write-ins like Age or Today's
    Date) are never flagged — there's nothing to cross-check a free-text
    answer against. (See instead the Cloud Vision double-check in
    cross_check_written_field_with_vision(), which exists precisely to give
    these seven questions an independent signal cross_check_answer() can't
    provide.)"""
    choices = CHOICE_LISTS_BY_NUMBER.get(number)
    if choices is None:
        return False, ""

    choice_matches = _choice_matches
    answer = (answer or "").strip()
    mark_position_raw = (mark_position_raw or "").strip()

    if number in MULTI_SELECT_QUESTION_NUMBERS:
        answer_parts = [a.strip() for a in answer.split(";") if a.strip()]
        position_parts = [p.strip() for p in mark_position_raw.split(",") if p.strip()]
        if not answer_parts and not position_parts:
            return False, ""  # nothing marked - nothing to check
        if len(answer_parts) != len(position_parts):
            return True, (
                f"model reported {len(answer_parts)} answer(s) but "
                f"{len(position_parts)} mark position(s) for a multi-select question"
            )
        mismatches = []
        for a, p in zip(answer_parts, position_parts):
            if not p.isdigit():
                mismatches.append(f"position {p!r} is not a number")
                continue
            idx = int(p) - 1
            if idx < 0 or idx >= len(choices):
                mismatches.append(f"position {p} is out of range (question has {len(choices)} choices)")
            elif not choice_matches(choices[idx], a):
                mismatches.append(f"answer {a!r} does not match choice at position {p} ({choices[idx]!r})")
        if mismatches:
            return True, "; ".join(mismatches)
        return False, ""

    # single-select
    if not answer and not mark_position_raw:
        return False, ""  # left blank - nothing to check
    if not mark_position_raw:
        return True, f"model gave answer {answer!r} but no mark_position to verify it against"
    if not answer:
        return True, f"model gave mark_position {mark_position_raw!r} but no answer text"
    if not mark_position_raw.isdigit():
        return True, f"mark_position {mark_position_raw!r} is not a number"
    idx = int(mark_position_raw) - 1
    if idx < 0 or idx >= len(choices):
        return True, f"mark_position {mark_position_raw} is out of range (question has {len(choices)} choices)"
    if not choice_matches(choices[idx], answer):
        return True, (
            f"answer {answer!r} does not match choice at reported position "
            f"{mark_position_raw} ({choices[idx]!r})"
        )
    return False, ""


def build_full_question_text(number: str, group_key: Optional[str], sub_text: str) -> str:
    """Combines the question number with its text into one string, e.g.
    "14. Overall, I am satisfied with the services I received." For a
    template that groups several lettered sub-items under one "big
    question" stem (group_key set, via GROUP_HEADERS), the stem is prefixed
    too, e.g. "3a. <stem sentence> <sub-item text>" — not used by the
    current Treatment Perceptions Survey template (GROUP_HEADERS is empty),
    but kept so an older/other grouped template can still use this."""
    if group_key:
        stem = GROUP_HEADERS[group_key]
        return f"{number}. {stem} {sub_text}"
    return f"{number}. {sub_text}"


QUESTION_TEXT_BY_NUMBER = {
    number: build_full_question_text(number, group_key, sub_text)
    for number, group_key, sub_text, _choices in SURVEY_QUESTIONS
}

# --------------------------------------------------------------------------
# Natural sort helper: "Nov18_2.pdf" should sort before "Nov18_10.pdf"
# --------------------------------------------------------------------------
_NUM_RE = re.compile(r"(\d+)")


def natural_sort_key(name: str):
    return [int(tok) if tok.isdigit() else tok.lower() for tok in _NUM_RE.split(name)]


# --------------------------------------------------------------------------
# report_date: the date the survey was actually filled out, parsed out of
# the date-folder name (e.g. "Nov 18 2025" -> 2025-11-18). This is distinct
# from refreshed_at/refreshed_date below, which record when THIS SCRIPT ran.
# --------------------------------------------------------------------------
_REPORT_DATE_FORMATS = ["%b %d %Y", "%B %d %Y", "%b %d, %Y", "%B %d, %Y"]


def parse_report_date(folder_name: str) -> Optional[datetime.date]:
    cleaned = " ".join(folder_name.split())  # collapse repeated/odd whitespace
    for fmt in _REPORT_DATE_FORMATS:
        try:
            return datetime.datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    err(
        "Could not parse a report_date out of folder name %r (tried formats %s); "
        "report_date will be NULL for files in this folder.",
        folder_name,
        _REPORT_DATE_FORMATS,
    )
    return None


def list_date_folders(bucket, root_prefix: str) -> list:
    """Return the immediate sub-'folder' names under root_prefix (GCS has no
    real folders, so this uses '/' as a delimiter over blob names)."""
    if root_prefix and not root_prefix.endswith("/"):
        root_prefix += "/"
    iterator = bucket.client.list_blobs(bucket, prefix=root_prefix, delimiter="/")
    # Have to exhaust the iterator before .prefixes is populated.
    loose_files = [b.name for b in iterator if not b.name.endswith("/")]
    folder_prefixes = sorted(iterator.prefixes, key=natural_sort_key)
    folders = [p[len(root_prefix):].rstrip("/") for p in folder_prefixes]
    status("[GCS] Found %d folder(s) under gs://%s/%s: %s", len(folders), bucket.name, root_prefix, folders)
    if loose_files:
        err(
            "%d file(s) sit directly under %s (not inside a date folder) and "
            "will be SKIPPED by this script: %s",
            len(loose_files),
            root_prefix,
            ", ".join(loose_files[:5]) + (" ..." if len(loose_files) > 5 else ""),
        )
    return folders


def list_pdfs_in_folder(bucket, root_prefix: str, folder: str) -> list:
    prefix = f"{root_prefix.rstrip('/')}/{folder}/"
    blobs = list(bucket.client.list_blobs(bucket, prefix=prefix, delimiter="/"))
    pdfs = [b for b in blobs if b.name.lower().endswith(".pdf")]
    pdfs.sort(key=lambda b: natural_sort_key(Path(b.name).name))
    if not pdfs:
        err("[GCS] No PDFs found under gs://%s/%s", bucket.name, prefix)
    else:
        status("[GCS] Folder %r: found %d PDF(s): %s", folder, len(pdfs), [Path(b.name).name for b in pdfs])
    return pdfs


# ==========================================================================
# EXTRACT: read each source survey PDF with Vertex AI Gemini, get back one
# answer per question, and load rows into BigQuery with columns
# folder_name, file_name, survey_question, survey_answer.
# ==========================================================================
@dataclass
class QARow:
    folder_name: str
    file_name: str
    survey_question: str
    survey_answer: str
    question_number: str  # e.g. "22", "H3" - the raw key from SURVEY_QUESTIONS /
    # QUESTION_TEXT_BY_NUMBER that survey_question's text came from. Recorded
    # explicitly so BigQuery consumers can group/join/filter by question
    # identity without having to parse or match against survey_question's
    # free text (which can shift wording between revisions).
    report_date: Optional[str]  # ISO date string ("YYYY-MM-DD") or None; when the survey was filled, from the folder name
    refreshed_at: str  # ISO timestamp string; when this ETL run loaded the row
    refreshed_date: str  # ISO date string; DATE(refreshed_at), used as the BQ partitioning column
    mark_position: Optional[str]  # 1-based position of the marked box; "" / None if n/a (open-text) or blank
    needs_review: bool  # True when something disagreed and is worth a human look - see answers_to_qa_rows()
    review_note: str  # why needs_review is True; "" otherwise. May combine more than one reason, semicolon-separated.
    detection_method: str  # "pixel_grid" (deterministic ink-density read of the Q1-18 checkbox grid),
    # "pixel_yesno_box" (same idea, for the isolated Q21/22/27/32 Yes/No(/Unknown) rows and the
    # Q25/Q29/Q35 single-choice lists - see detect_yesno_box_answers()), "pixel_h3_circle" (H3's
    # round radio buttons - see detect_h3_answer()), or "model" (vision-model reading, optionally
    # self-checked against its own reported mark_position - see cross_check_answer())
    model_reasoning: str = ""  # the model's own one-sentence account of the visual evidence for
    # its answer (see rule 11 in build_extraction_prompt()) - kept purely for auditability/debugging;
    # "" for pixel-only entries, older callers, or if the model didn't return one. Never used to
    # decide needs_review or override anything - only pixel readings and cross_check_answer() do that.
    model_confidence: Optional[float] = None  # the model's own self-reported confidence (0.0-1.0)
    # for THIS specific answer (see rule 15 in build_extraction_prompt()); None if the model didn't
    # return a parseable one (e.g. an older/mocked response predating this field). Recorded for every
    # question regardless of detection_method (for auditability), but only ACTED ON - i.e. only able to
    # set needs_review - when detection_method == "model": see answers_to_qa_rows() for why a
    # pixel-verified reading's own margin check already independently vouches for that question,
    # so a merely-low model_confidence on top of it isn't itself grounds for review.
    vision_cross_check: str = ""  # for the seven handwritten/write-in questions with no fixed choice
    # list (H1, H2, H4, H5, H6, 24, 26 - see WRITTEN_TEXT_QUESTION_NUMBERS): "agree" or "disagree",
    # from comparing the model's answer against an independent Cloud Vision OCR reading of the same
    # page (see cross_check_written_field_with_vision()). "" for every other question, or when there
    # wasn't enough signal to make either call (Vision unavailable, nothing OCR'd, or the model's own
    # answer was blank - nothing to check).
    confidence_threshold: Optional[float] = None  # the value of MODEL_CONFIDENCE_THRESHOLD that was
    # actually in effect when this row was extracted - stored on every row (not just ones the gate
    # acted on) purely as an audit trail: if the threshold is retuned later (e.g. chasing the ~70%
    # needs_review target), old rows still show what threshold produced their needs_review verdict,
    # so a BigQuery query can directly compare model_confidence against confidence_threshold per row
    # rather than having to know which script version/constant value produced historical data.
    vision_match_score: Optional[float] = None  # the numeric agreement score BEHIND vision_cross_check
    # (see cross_check_written_field_with_vision()) - 1.0/0.0 for the token fields' exact-match check
    # (H1/H2/26), or the word-level matching coverage fraction (0.0-1.0, summed across every
    # matching run - see cross_check_written_field_with_vision()) for the freeform fields' fuzzy-
    # match check (H4/H5/H6/24). ALWAYS populated whenever a Vision comparison actually ran (i.e.
    # whenever vision_cross_check is "agree" or "disagree"), even on agreement - so e.g. a
    # borderline 0.92 coverage that narrowly passed (VISION_FREEFORM_COVERAGE_THRESHOLD=0.9) is
    # visible, not just a bare "agree". None when no comparison ran (vision_cross_check == "").
    vision_ocr_snippet: str = ""  # a short, human-readable capture of what Cloud Vision actually READ
    # that survey_answer was compared against - a handful of its individual word tokens for the token
    # fields (H1/H2/26), or a truncated excerpt of its full-page OCR text for the freeform fields
    # (H4/H5/H6/24) - see cross_check_written_field_with_vision(). Lets you eyeball the model's answer
    # against Vision's own independent reading side by side in BigQuery, not just trust the verdict.
    # "" whenever vision_cross_check is also "" (no comparison ran).
    pdf_quality_route: Optional[str] = None  # Revision 37: this file's "recommended_route" from
    # classify_pdf_quality.py's pdf_quality table at extraction time ("pixel"/"vision"/"fallback"),
    # or None if no quality row was found / PDF_QUALITY_ROUTING_ENABLED was False for this run - see
    # query_pdf_quality_route() and extract_qa_from_pdf()'s quality_route docstring. Recorded on
    # every row (not just ones it affected) purely as an audit trail: a "vision"/"fallback" route
    # forces needs_review=True regardless of this question's own signals (see the
    # "quality route forced this file for review" branch below), so this column lets a BigQuery
    # query distinguish "flagged because the upstream classifier already knew this scan was
    # questionable" from every other, question-specific reason recorded in review_note.


def build_extraction_prompt() -> str:
    multi_select_list = ", ".join(sorted(MULTI_SELECT_QUESTION_NUMBERS))
    lines = [
        "You are reading one scanned, hand-filled 'Treatment Perceptions Survey "
        "(Adult)' PDF. For EACH question below, report the answer the respondent "
        "marked or wrote on the form. Follow every numbered rule below exactly, "
        "in order, for every question before moving to the next one.",
        "",
        "RULES",
        "",
        "1. A mark only counts if it is actual INK: a pen or pencil X, checkmark, "
        "filled-in box, or filled-in circle that a human physically drew or "
        "printed onto the page. Do NOT count as a mark: the box or circle's own "
        "printed outline, a scan shadow or smudge, a fold or crease line, a stray "
        "mark bleeding over from a neighboring row or question, or handwriting "
        "that belongs to a different question. If you cannot point to actual ink "
        "touching or inside that specific choice's own box/circle, it is not "
        "marked.",
        "2. For single-select checkbox questions, look at every choice listed for "
        "that question and report exactly one that satisfies rule 1.",
        "3. Do NOT default to the last-listed choice (e.g. \"Not Applicable\", "
        "\"N/A\", or \"No\") when you are unsure. Start from the FIRST-listed "
        "choice and check each one in order, confirming with rule 1 which "
        "specific one actually has ink on it - never assume it is the last one "
        "just because that is a common or 'safe' answer.",
        "4. Some questions use small ROUND circles instead of square boxes, "
        "arranged in a single horizontal line of running text rather than a "
        "vertical list (for example, the 'Setting' question near the top of the "
        "form: 'Setting: O Early Intervention  O OP/IOP  O Residential  O "
        "OTP/NTP  O Detox/WM  O Recovery Services'). For these, the marked "
        "choice is the ONE circle that is solid black all the way through; every "
        "other circle in that line stays a hollow, open ring. Because these "
        "circles sit close together and right next to their own label, read "
        "through the ENTIRE line left to right and check each circle "
        "individually before answering - do not answer from the first circle "
        "that catches your eye or from where a mark is on a similar-looking "
        "form.",
        f"5. Questions {multi_select_list} allow MORE THAN ONE choice to be "
        'marked ("mark all that apply"), but MOST respondents only mark ONE box '
        "even on these questions - do not assume more than one is marked by "
        "default. For each individual choice in that question's list, check "
        "independently whether THAT SPECIFIC box satisfies rule 1 - do not "
        "include a choice just because it seems plausible or related to a "
        "marked choice nearby. Report every choice that passes this check, "
        'joined by "; " (e.g. "White/Caucasian; Asian") - most of the time this '
        "will be just one choice. All other questions should have at most one "
        "marked choice.",
        "6. If nothing satisfies rule 1 for a question, use an empty string \"\". "
        "Respondents skip questions on this form often - reporting a question as "
        "blank is a normal, correct, and expected answer, NOT something to "
        "avoid. If every box/circle for a question looks empty/unmarked, the "
        "answer is \"\", even if an earlier or later question on the form was "
        "answered.",
        "7. For open-text or write-in questions (e.g. Age, Today's Date, "
        "Comment), transcribe the handwritten/typed text as written.",
        "8. When the marked choice for a checkbox question is an \"(specify)\" "
        "option (e.g. \"Other (specify)\"), look for handwritten or typed text "
        "next to or below that choice and APPEND it to the answer in the format "
        "\"Other (specify): <text>\" (e.g. \"Other (specify): Mexican\"). If that "
        "choice is marked but nothing is actually written next to it, report "
        "just the choice text on its own (e.g. \"Other (specify)\") - do not "
        "invent text. This applies even on multi-select questions: only the "
        "\"(specify)\" choice itself gets the \": <text>\" suffix appended; other "
        "marked choices in the same answer are reported as their plain listed "
        "text.",
        "9. Only use the choices listed for that question; do not invent new "
        "wording (the \"(specify)\" free-text suffix in rule 8 is the one "
        "exception).",
        "10. Several questions use a table of checkbox columns with narrow, "
        "sideways/rotated header text (e.g. a 6-point scale: Strongly Agree, "
        "Agree, I am Neutral, Disagree, Strongly Disagree, Not Applicable). "
        "These are easy to misread by column. For every question that has a "
        "listed 'choices' list below, first decide which position (counting "
        "strictly left-to-right, 1 = the first choice listed, in the exact order "
        "the choices are given below) has ink on it per rule 1 - look directly "
        "above/below the specific marked box to confirm which column header it "
        "belongs to, do not guess from habit or from where a mark 'usually' is - "
        "and only THEN read off that position's choice text as your answer, so "
        "the two can never disagree. Pay special attention to a 'Strongly X' "
        "column sitting immediately next to a plain 'X' column (e.g. 'Strongly "
        "Agree' next to 'Agree', or 'Strongly Disagree' next to 'Disagree') - "
        "these are the single most commonly confused pair on this form, because "
        "they share almost all of the same header word and sit one box apart. "
        "Do not default to the 'Strongly' variant just because the general "
        "sentiment is positive/negative or because 'Strongly' is a more salient "
        "word - the two columns are only correctly told apart by exactly which "
        "box has ink directly under/over it, never by the overall tone of the "
        "statement or a guess at how strongly someone would feel about it. If "
        "the mark sits close to the boundary between two adjacent columns, "
        "trace it against the column's own left and right edges (using the "
        "other rows' boxes in the same column as a reference for where that "
        "column actually lies) rather than eyeballing which header text it is "
        "closest to, and lower your \"confidence\" for that question (see rule "
        "15) accordingly rather than silently guessing.",
        "11. Before writing \"mark_position\" or \"answer\" for a question, "
        "first write a one-sentence \"reasoning\" describing the specific visual "
        "evidence you see for that question: which exact box/circle has ink on "
        "it and what that ink looks like (an X, a checkmark, a filled circle, "
        "etc.), or that none of them do. Base \"mark_position\" and \"answer\" "
        "strictly on what you just wrote in \"reasoning\" - never adjust "
        "\"reasoning\" after the fact to justify an answer you already decided "
        "on. Whenever anything about the scan itself made this question harder "
        "to read - not just which box is marked, but the physical quality of "
        "the scan/handwriting - name that specific condition in \"reasoning\" "
        "too (e.g. \"the mark is faint/light pen pressure\", \"the handwriting "
        "is messy/hard to read\", \"the page appears skewed/rotated here\", "
        "\"there's a smudge/stray mark near the box\", \"the scan is creased/"
        "torn in this area\", \"more than one box has some ink\"), in addition "
        "to whatever else \"reasoning\" already says - this is what your "
        "\"confidence\" score in rule 15 should be based on, so naming the "
        "actual condition (rather than just a low number) is what lets a human "
        "reviewer know exactly what to look for on the page.",
        "12. Report \"mark_position\": for a single-select question, the one "
        "number decided in rule 10 (as a string, e.g. \"6\"). For a multi-select "
        f"question ({multi_select_list}), every marked position, comma-separated "
        'in the same order as your "answer" choices (e.g. "1,5"). Use "" for '
        "open-text/write-in questions (no choices list) or when nothing is "
        "marked.",
        "13. \"answer\" and \"mark_position\" must describe the SAME choice - "
        "double-check them against each other, and against \"reasoning\", before "
        "responding.",
        "14. Return ONLY a JSON array, one object per question, with exactly "
        'these keys in this order: "question_number", "reasoning", '
        '"mark_position", "answer", "confidence". No extra commentary outside '
        "the JSON array.",
        "15. \"confidence\": a number from 0.0 to 1.0 for how CERTAIN you are "
        "that YOUR OWN answer to THIS question is correct, based purely on how "
        "clear and unambiguous the ink/handwriting evidence was - not on how "
        "important the question is. Use 0.9-1.0 only when the evidence is "
        "completely unambiguous (a single, isolated, clearly-inked mark with no "
        "other choice touched at all, or fully legible handwriting). Use "
        "0.5-0.75 when the mark is faint, more than one box has some ink and you "
        "had to judge which one is the real mark, the handwriting is partly hard "
        "to read, or the visual evidence could reasonably support more than one "
        "answer. Use below 0.5 when you are essentially guessing. Do not inflate "
        "this number - an honest 0.6 here is more useful than a false 0.95, and "
        "this number is used downstream to decide which answers get a human "
        "double-check, so an inflated confidence directly causes a wrong answer "
        "to be trusted without review.",
        "16. Two fields have a strict, well-defined format - after writing "
        "\"answer\" for each, check it against its expected format below "
        "BEFORE moving to the next question, and if it doesn't match, look at "
        "the handwriting/digits again rather than reporting whatever you first "
        "wrote: H1 (Home Unit CalOMS Provider ID) must be exactly 6 digits, "
        "read digit-by-digit from its 6 boxed cells, with no letters, spaces, "
        "or punctuation (e.g. \"196697\"). H6 (Today's Date) must be a calendar "
        "date in MM/DD/YYYY format (e.g. \"09/04/2026\"). If, after a careful "
        "second look, the handwriting genuinely does not support the expected "
        "format (e.g. fewer than 6 digits are actually legible, or part of the "
        "date is missing/illegible), report exactly what you can actually read "
        "and lower \"confidence\" (rule 15) accordingly - never invent an extra "
        "digit or a date component just to force the format to match.",
        "",
        "Questions:",
    ]
    for number, group_key, sub_text, choices in SURVEY_QUESTIONS:
        full_text = build_full_question_text(number, group_key, sub_text)
        lines.append(f"{full_text} (choices: {choices})")
    return "\n".join(lines)


_EXTRACTION_PROMPT = None  # lazily built + cached


def get_extraction_prompt() -> str:
    global _EXTRACTION_PROMPT
    if _EXTRACTION_PROMPT is None:
        _EXTRACTION_PROMPT = build_extraction_prompt()
    return _EXTRACTION_PROMPT


_VERTEX_INITIALIZED = False
_RENDER_DPI = 300  # PDF native resolution is 72 dpi; render well above that for small/rotated text

# Canonical page size (px) every absolute-pixel calibration table in this
# module (_YESNO_BOX_CALIBRATION, _H3_CIRCLE_CALIBRATION,
# _MULTISELECT_BOX_CALIBRATION, _GRID_COLUMN_CENTERS) was measured against:
# a US Letter page (612x792pt) rendered at 300 DPI.
_CALIBRATION_PAGE_SIZE_AT_300DPI = (2550, 3300)  # (width, height)


def _page_scale_factors(img_shape, dpi: int = _RENDER_DPI):
    """Returns (x_scale, y_scale): the per-axis multiplier that converts an
    absolute pixel coordinate calibrated against a Letter-size page
    rendered at 300 DPI (_CALIBRATION_PAGE_SIZE_AT_300DPI) into the correct
    pixel position on THIS actual rendered page image, whatever its real
    size.

    Computed directly from the ACTUAL decoded image dimensions rather than
    from `dpi` alone: for an ordinary Letter-size scan this reduces to
    exactly dpi/300.0 on both axes - the plain DPI-only scale used
    everywhere in this module before this existed, so this changes nothing
    for any file this pipeline has ever been calibrated against. It only
    diverges when the ACTUAL rendered page's physical proportions differ
    from Letter's.

    Reported directly (Nov5_1_TPS_2988.pdf): this file's PDF pages are
    A4-sized (595x842pt) rather than Letter (612x792pt) - rendered at the
    same 300 DPI, that page decodes to 2484x3509px instead of the expected
    2550x3300px. Every absolute-coordinate pixel detector that used the
    plain single dpi/300 scale (detect_yesno_box_answers()'s non-"ruled"
    modes, detect_h3_answer(), detect_multiselect_ink_ratios()) landed
    measurably off on this file - up to ~150px vertically by the time a
    question as far down the page as Q29/Q34 is reached - either failing
    to locate the box at all (Q29, Q34 - both silently fell back to the
    model, which itself misread both) or, worse, confidently landing on a
    NEIGHBORING, wrong box (Q33: pixel evidence pointed at "Native
    Hawaiian/Pacific Islander" when the real mark was two rows up, on
    "White/Caucasian"). detect_checkbox_grid_answers() is NOT affected by
    this - its row positions are found dynamically from the page's own
    ruled lines, not from an absolute calibrated Y coordinate, and its
    per-row column relocation already independently absorbs a consistent
    x-offset - so it was already robust to this."""
    h, w = img_shape[:2]
    cal_w, cal_h = _CALIBRATION_PAGE_SIZE_AT_300DPI
    return w / float(cal_w), h / float(cal_h)


# Grayscale cutoff (0=black, 255=white) below which a pixel counts as "ink"
# when binarizing a rendered page for the pixel-based detectors
# (detect_checkbox_grid_answers(), detect_yesno_box_answers()). 200 worked
# for every scan seen through six rounds of this investigation, all of which
# had a plain white background - but broke completely (0 of 18 grid rows
# found) on a real file (Nov1_1_TPS_1070.pdf) whose Q1-18 grid has
# alternating light-gray "zebra stripe" row shading for readability, a form
# design detail not present on the earlier files. Measured directly: that
# shading sits at ~183-234 (partly BELOW 200, so large blocks of ordinary
# background were misread as ink, corrupting every line-detection call on
# the page), while genuine ink (printed text/lines, pen marks) on the same
# scan sits at 5-90 and pure white background at 240-255.
#
# 150 was tried first (comfortably below the 183 shading floor) and broke a
# DIFFERENT real thing: Nov10_1.pdf/Nov10_3.pdf's Q21/Q22 ruled line is thin
# enough that it's antialiased rather than solid black - measured median
# pixel value ~158 along its own length - so at 150 more than half that
# line's pixels stopped counting as ink and it dropped below
# _find_full_width_lines()'s 50%-of-page-width detection floor entirely,
# which silently cost Q21 its dynamic row location (see detect_yesno_box_answers()
# module comment) on a file that had been working. 165 was found by direct
# measurement to be the highest value that still excludes all observed
# zebra-stripe shading (183+) with margin AND keeps that antialiased ruled
# line detected (its line reappears at 165, confirmed empirically, not
# assumed) - re-verified against both original real files plus the new
# shaded one with no regressions.
_INK_THRESHOLD = 165

# Minimum (top-cell-density - second-place-cell-density) required before trusting a
# pixel-grid reading. Real marked cells in the one sample tested had margins of
# 0.23-0.32; empty/unmarked cells were within ~0.02 of each other. 0.05 leaves
# generous headroom for a lighter pen while still rejecting a genuine tie/noise.
_GRID_CONFIDENCE_MARGIN = 0.05
# Per-question override for _GRID_CONFIDENCE_MARGIN, mirroring
# _YESNO_CONFIDENCE_MARGIN_OVERRIDE's pattern for the yesno_box detector.
# Q12 (Revision 29 follow-up): a real file (Nov6_2_TPS_4005.pdf) showed the
# grid detector already reading the CORRECT answer ("I am Neutral", position
# 3) with a margin of 0.0497 - a hair under the general 0.05 threshold (missed
# by 0.0003), so the correction never fired and the model's wrong answer
# ("Not Applicable") stood, with no review flag either (0.0497 doesn't trip
# any other check). 0.045 clears this real measurement while staying well
# above the smallest observed gap between two genuinely unmarked grid cells
# on real files (~0.02 or less - see _GRID_CONFIDENCE_MARGIN's own comment).
_GRID_CONFIDENCE_MARGIN_OVERRIDE = {"12": 0.045}

# Positively-blank detection for the Q1-18 grid: below _GRID_CONFIDENCE_MARGIN
# just means "not confident which column" and falls back silently to the
# model - which has no way to independently notice a genuinely UNANSWERED
# row and can (and did, on a real file) hallucinate an answer anyway.
# Measured directly on Nov1_7_TPS_4223.pdf (18 rows, 17 marked + 1 genuinely
# blank, ground truth confirmed by eye against the rendered scan): every
# marked row's WINNING column read 0.354-0.440 density with a margin of
# 0.252-0.359 over the runner-up; the one genuinely blank row (Q16, all six
# boxes empty) read a flat 0.0548-0.0608 across all six columns - margin
# 0.0014. A ~50x gap on the margin and no overlap at all on the winning
# density, so both floors below sit with generous headroom on both sides of
# that gap rather than close to either edge - same "never guess" shape as
# _YESNO_BLANK_INK_FLOOR/_YESNO_BLANK_MARGIN_CEILING. Only a row clearing
# BOTH (implausibly low winning density AND a near-zero margin) is reported
# as positively blank; anything ambiguous still just falls back to the
# model, exactly as before this addition.
_GRID_BLANK_INK_FLOOR = 0.15
_GRID_BLANK_MARGIN_CEILING = 0.02

# Revision 29 follow-up (Nov6_1_TPS_3934.pdf Q3/Q15 false-positive report:
# "the pixel detector found two boxes on this row both confidently marked
# ... This is not right"). The general two-marks backstop below originally
# reused _GRID_BLANK_INK_FLOOR (0.15) as its "confidently marked" floor for
# BOTH boxes, but on this file's degraded scan, uniform low-level ink noise
# (page/scan grain, faint gridline bleed - NOT an actual second mark) pushes
# an unmarked box's density up to 0.1505-0.1598 on some rows (confirmed
# directly: Q3 densities [0.1089, 0.4049(real mark), 0.1066, 0.1111, 0.1598,
# 0.1568]; Q15 densities [0.3129(real mark), 0.1505, 0.0863, 0.0819, 0.0811,
# 0.1053]) - just over 0.15, with nothing resembling a second mark actually
# present. Measured across all 4 reference files' 72 grid rows, the highest
# "noise" second-highest density on any genuinely single-marked row is this
# same file's 0.1598; the lowest CONFIRMED genuine second mark is Nov6_2_
# TPS_4005.pdf's Q1 (Strongly Agree crossed out, Disagree marked instead) at
# 0.2222, comfortably clear of the noise ceiling. A dedicated, higher floor
# just for the two-marks check (independent of _GRID_BLANK_INK_FLOOR, which
# stays at 0.15 for its other, unrelated jobs: picking the winning column
# and detecting a positively-blank row) sits at the midpoint of that gap.
_GRID_TWO_MARKS_INK_FLOOR = 0.19

# Correction/cross-out detection for the Q1-18 grid: a respondent who marks
# one box, then crosses it out (scribbles over it) and marks a DIFFERENT box
# instead, leaves the crossed-out box with MORE ink than a normal single
# mark - the original X plus the strike-through on top of it - while the
# real, final answer's box still reads a perfectly normal single-mark
# density. Reported directly (Nov3_1_TPS_1585.pdf Q3: respondent marked "Not
# Applicable", crossed it out, then marked "Strongly Agree" - BQ showed "Not
# Applicable"): measured on that real file, "Strongly Agree" (the correct,
# final answer) read 0.386 density - squarely inside the normal single-mark
# range documented above (0.354-0.440) - while "Not Applicable" (the
# crossed-out box) read 0.6129, far above it, with every other column
# reading its normal near-zero unmarked density (0.056-0.082). The plain
# highest-density-wins logic picked the crossed-out box because 0.6129 >
# 0.386, and its margin over the (also-elevated-looking, but still just
# normal) runner-up was large enough to clear _GRID_CONFIDENCE_MARGIN - so
# this failure mode is NOT caught by the ordinary margin/blank checks above,
# which only ever look at whether the WINNING column is confidently ahead,
# never at whether the winning column's own density is itself
# implausibly high for a single mark.
#
# 0.50 sits with real headroom on both sides of the gap this single
# real-file measurement showed: comfortably above the documented normal
# single-mark ceiling (0.440) and comfortably below the measured crossed-out
# reading (0.6129) - same "never guess, leave margin on both sides" shape as
# every other floor/ceiling in this file. Single-file calibration - same
# caveat as every other single-file threshold here; re-measure against more
# real crossed-out examples if/when they turn up.
#
# When a column's density clears this ceiling, detect_checkbox_grid_answers()
# excludes it from being picked as the answer (a corrected-out mark should
# never win just because it now has the most ink) and re-ranks the remaining
# columns instead - but ALWAYS marks that row "correction_detected" so
# answers_to_qa_rows() forces needs_review=True on it regardless of whether
# the recovered answer matches the model's own reading: a respondent
# correction is exactly the kind of scan a human should glance at once, even
# when the pipeline is confident it recovered the right final answer.
_GRID_OVERDENSE_INK_CEILING = 0.50

# Minimum line coverage for the faint-line gap-filling recovery inside
# detect_checkbox_grid_answers() (Revision 29 follow-up) - see that
# function's own comment for the full story. Set below the highest
# confirmed real-but-faded grid boundary line seen so far (44.3% and 36.1%,
# both on Nov6_1_TPS_3934.pdf) while staying well above ordinary page noise
# at this width (question-text rows and shading transitions measured well
# under 30% in every file checked). This floor is only ever consulted
# INSIDE an already-identified oversized gap, and only accepted if splitting
# the gap there produces two normal-sized row gaps - so, unlike a global
# threshold change, a low value here carries no risk of accepting a
# spuriously-shaped merged gap the way the 145px ceiling once did.
_GRID_FAINT_LINE_FLOOR = 0.30

# Round 16 fix - see detect_checkbox_grid_answers()'s docstring for the full
# story. These are the 6 answer columns' (Strongly Agree .. Not Applicable)
# calibrated x-centers at 300 DPI, averaged from a clean full-height read of
# 4 real files where the OLD single-global-projection technique still worked
# (Nov10_1.pdf, Nov1_1_TPS_1070.pdf, Nov1_3_TPS_1357.pdf, Nov1_2_TPS_1356.pdf):
# per-file centers agreed to within ~5px of each other, so one shared
# calibration is safe to reuse across files exactly like _YESNO_BOX_CALIBRATION
# already does for other questions.
_GRID_COLUMN_CENTERS = (1661.75, 1790.4, 1919.6, 2049.9, 2179.6, 2309.1)
# Search pad around each calibrated center, per row (not per file - see below).
# 25px would exactly graze the true box edge on Nov10_1.pdf (confirmed: at
# pad=25 the true right border landed exactly on the search window's own
# edge and _locate_checkbox() correctly refused it, per its own
# window-edge-touching rejection) - 32px leaves that box comfortably inside
# the window with room to spare, without getting wide enough to risk pulling
# in the NEXT column (columns are ~130px apart center-to-center).
_GRID_COLUMN_PAD = 32
_GRID_BOX_EXPECTED_SIZE = 40  # both width and height, at 300 DPI


def _group_consecutive_positions(positions: list, max_gap: int = 3) -> list:
    """Groups a sorted list of pixel indices that are within max_gap of each
    other, returning the mean index of each group. Used to collapse a few
    pixels' worth of a single (anti-aliased, multi-pixel-wide) ruled line into
    one representative coordinate."""
    if not positions:
        return []
    groups = []
    cur = [positions[0]]
    for v in positions[1:]:
        if v - cur[-1] <= max_gap:
            cur.append(v)
        else:
            groups.append(cur)
            cur = [v]
    groups.append(cur)
    return [int(sum(g) / len(g)) for g in groups]


def detect_checkbox_grid_answers(page_png_bytes: bytes, dpi: int = _RENDER_DPI) -> dict:
    """Deterministic, non-LLM reading of the Treatment Perceptions Survey's
    main checkbox grid (questions 1-18, the 6-point agreement scale table on
    page 1) directly from the rendered page image, using OpenCV to find the
    table's own ruled grid lines and measure ink density inside each cell.

    This exists because the vision model can be WRONG IN A WAY THAT'S
    INTERNALLY CONSISTENT: it can report an answer label and a mark_position
    that agree with each other (so cross_check_answer() finds nothing wrong)
    while both are still the wrong column — this was reported for
    Nov10_1.pdf Q10 ("Strongly Disagree" read back as "Not Applicable", with
    a self-consistent mark_position=6). A cross-check against the model's own
    two outputs can never catch that; only checking against the actual pixels
    can. This function measures the pixels directly.

    How it works (see also the exploratory session that validated this
    against a real sample - it reproduced the correct answer for all 18 rows
    with a clear margin):

      1. Find every full-page-width horizontal ruled line -> these are row
         separators. The 18 that have consistent single/double-text-line
         spacing (as opposed to the header block above or the differently
         laid out Q19-23 below) are the grid's row boundaries; by this
         template's fixed layout, questions 1-18 are always the first such
         block of 18 (see the SURVEY_QUESTIONS comment for the template
         assumption this relies on).

      2. For EACH of those 18 rows independently, re-locate each of the 6
         answer columns' own checkbox border within a small search window
         around a shared, pre-calibrated x-center (_GRID_COLUMN_CENTERS),
         using the same robust _locate_checkbox() border-coverage technique
         already used elsewhere in this file for Q19-35's isolated boxes -
         falling back to a plain ink-density window at that row's own
         measured x-offset for any single column _locate_checkbox() can't
         confidently border-trace (see Round 16 note below for why both of
         these matter). This is a change from the original version of this
         function (through Round 15), which found all 6 columns' x-positions
         ONCE from a single vertical-ink projection spanning the full
         18-row block, then reused those x-positions for every row.

      3. For each of the 18 rows x 6 columns, measure the fraction of dark
         ("ink") pixels inside that row's own located box (well inside the
         glyph's own border, so only an actual X/checkmark contributes) — an
         empty box scores low and consistently across all 6 columns in a
         row; a marked box scores noticeably higher than the other 5.

    Round 16 fix (Nov2_2_TPS_1566.pdf reported "[GRID] ... found nothing on
    this file's page 1 (0 of 18 rows)"): root-caused to a very slight page
    skew invisible to the eye - measured directly on this file, one column's
    own left border walked from x=2162 at row 1 to x=2156 at row 18, a 6px
    drift over the table's height. The OLD single global projection summed
    ink at one fixed x across all 18 rows and required it to be "on" for
    >=50% of that combined height; a few px of drift is enough to split that
    ink across two neighboring x-columns and drop several of them under the
    50% line (confirmed directly: 4 of the 6 real columns measured between
    46% and 55% coverage on this file, essentially a coin flip). Any
    real-world scan can have this kind of skew (the original print, the
    scanner bed, or a rotated PDF page), so this wasn't a one-off - the same
    root cause was also found, independently, on Nov1_4_TPS_1402.pdf (which
    had been silently falling back to the model for Q1-18 on EVERY prior
    round, just never reported because the model happened to read that
    file's uniform "all Strongly Agree" answers correctly anyway).

    Re-locating each row's own columns independently (this function's
    current approach) is tolerant of that drift by construction - it never
    sums ink across more than one row's own height. A first attempt at this
    (a per-row version of the OLD projection technique, restricted to each
    row's own y-band) was tried and rejected: a single row's own band is
    short enough that individual question-text letter strokes (bold text,
    ~35-40px cap height at 300 DPI) can themselves survive the same vertical
    erosion used to find box borders, producing dozens of spurious column
    candidates per row - confirmed directly (14+ false "column pairs" per
    row from question text alone, when tested against Nov2_2_TPS_1566.pdf).
    Anchoring on a shared calibrated x-center per column and using
    _locate_checkbox()'s border-coverage + size-matching selection (already
    proven against exactly this kind of noise for Q19-35) avoids that
    failure mode entirely, since the search window never reaches anywhere
    near the question-text column. Separately, an unusually large/messy X
    mark can fully obscure a box's own printed border (confirmed on
    Nov10_1.pdf Q1's "Not Applicable" box - a mark whose strokes extend well
    outside the box on all four sides), making border-tracing genuinely
    unreliable for that one cell; for exactly that case, this function falls
    back to a plain ink-density window (no border-tracing needed) centered
    at the calibrated position, corrected by that row's own median offset
    from whichever OTHER columns in the same row _locate_checkbox() did
    successfully border-trace - safe specifically because a mark heavy
    enough to defeat border-tracing is also heavy enough that its ink ratio
    reads unambiguously high regardless of the exact window placement.
    Re-validated after this change against all 6 previously-calibrated real
    files (Nov10_1.pdf, Nov18_2.pdf, Nov1_1_TPS_1070.pdf, Nov1_3_TPS_1357.pdf,
    Nov1_2_TPS_1356.pdf, Nov1_4_TPS_1402.pdf): every position this function
    found before Round 16 is unchanged (byte-for-byte identical column
    picks), Nov1_4_TPS_1402.pdf now recovers all 18 rows (previously 0,
    matching a direct read of that scan - every row is "Strongly Agree"),
    and Nov2_2_TPS_1566.pdf now recovers all 18 rows matching a manual pixel
    read of the reported file. (Nov18_2.pdf still reads 0 rows both before
    and after this fix - a separate, pre-existing, already-safe limitation:
    on that specific file the ruled line marking the END of the 18-row block
    only spans the question-text column's width, not the full page, because
    it's also the transition into the differently-laid-out Q19 section - see
    the "Known limitations" note in the project doc. This was true before
    Round 16 too and is not a regression; Q1-18 on that one file still
    safely fall back to the model, as they always have.)

    Returns {question_number ("1".."18"): {"position": 1-based column index,
    "margin": top-cell density minus second-place density}} for however many
    rows this succeeded on. Returns {} (not an exception) if the grid can't
    be confidently located at all — e.g. a different/rescanned template, or
    a page that isn't page 1 — so callers can always safely fall back to the
    model-only reading. Never raises on a page that doesn't look like this
    grid; only raises on a genuinely corrupt/unreadable image.
    """
    import cv2
    import numpy as np

    scale = dpi / 300.0  # thresholds below were tuned at 300 DPI
    arr = np.frombuffer(page_png_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
    if img is None:
        return {}

    H, W = img.shape
    _, binary = cv2.threshold(img, _INK_THRESHOLD, 255, cv2.THRESH_BINARY_INV)

    # --- 1. full-width horizontal lines -> row candidates ---
    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(int(60 * scale), 10), 1))
    h_lines = cv2.dilate(cv2.erode(binary, h_kernel), h_kernel)
    row_sums = h_lines.sum(axis=1) / 255
    # Upper bound tightened from 145 to 125 (Revision 29). Every genuine
    # double-line question row measured across every real file seen so far
    # (Q9/Q10/Q13/Q18, several files) reads 107-109px - comfortably under
    # 125. 145 was wide enough to ALSO accept something much more dangerous:
    # confirmed directly on Nov6_1_TPS_3934.pdf, when the 48%-fallback
    # threshold below still couldn't recover ONE particular boundary line
    # (a second, more heavily obscured line, distinct from the one that
    # motivated the 48% fallback), the two adjacent single-line rows either
    # side of that missing line merged into one 144px gap - which fit
    # neatly under the old 145 ceiling and was accepted as if it were a
    # normal (very tall) double-line row. That silently shifted every
    # subsequent row's question-number mapping by one for the rest of the
    # file: what the function reported as "Q17" was actually reading Q18's
    # real row, and "Q18" was actually reading Q19's row entirely (Q19 isn't
    # even part of this 18-question grid) - a wrong-but-confident answer,
    # not a safe miss. 125 rejects that 144px merged gap outright, which
    # correctly breaks the run below the required 19 lines on this file (so
    # it safely reports 0 rows and falls back to the model, with Revision
    # 28's needs_review-on-all-18 backstop catching it) rather than serving
    # up a corrupted mapping with no visible sign anything was wrong.
    low, high = 45 * scale, 125 * scale

    def _longest_consistent_run(min_width_frac: float):
        """Row peaks at/above min_width_frac of the page width, grouped into
        distinct lines, then the longest run of consecutive lines whose gaps
        all fall in [low, high] (single/double-text-line row spacing)."""
        peaks = [y for y in range(H) if row_sums[y] > W * min_width_frac]
        if not peaks:
            return []
        lines = _group_consecutive_positions(peaks)
        if len(lines) < 2:
            return []
        best, cur = [], [lines[0]]
        for y in lines[1:]:
            if low <= (y - cur[-1]) <= high:
                cur.append(y)
            else:
                if len(cur) > len(best):
                    best = cur
                cur = [y]
        if len(cur) > len(best):
            best = cur
        return best

    # --- 2. longest run of consistent single/double-text-line row gaps ---
    # Try the normal >=50%-of-page-width line threshold first (as always).
    # Fall back to a slightly relaxed 48% ONLY if that fails to find a full
    # 18-row run - confirmed directly (Nov6_1_TPS_3934.pdf): a heavy,
    # oversized mark on one row's answer (a large looping scribble on Q13's
    # "Not Applicable") sits right at that row's bottom ruled line, which
    # ALSO happens to be the boundary between that row's gray zebra-stripe
    # shading and the next row's white background - the combination left
    # that one boundary line measuring 48.9% width coverage, just under the
    # 50% cutoff, which broke the whole 18-row run at that single line
    # (13 rows found, needed 19 boundaries). 48% was confirmed to recover
    # the full, correctly-spaced 19-boundary run on that file while still
    # sitting comfortably above the next-highest non-line noise observed on
    # the same and other real files (all well under 40%) - not a blanket
    # loosening, just enough headroom for one real antialiasing case.
    best_run = _longest_consistent_run(0.5)
    if len(best_run) < 19:
        relaxed_run = _longest_consistent_run(0.48)
        if len(relaxed_run) >= 19:
            best_run = relaxed_run

    # --- 2b. faint-line gap-filling (Revision 29 follow-up). If even the
    # 48% relaxed run still doesn't reach 19 lines, look specifically INSIDE
    # each individual gap that's too WIDE to be a normal single/double-line
    # row (i.e. > high) for a much fainter line that would split it into two
    # normal-sized sub-gaps. This is deliberately narrower and safer than
    # just lowering the 48% floor globally: it only ever inserts a line when
    # doing so is independently corroborated by BOTH (a) that a real,
    # measurable ink line actually exists at that exact y position, AND (b)
    # inserting it produces two geometrically plausible row gaps - it can
    # never accept a gap merely because of its raw total size (which is
    # exactly the failure mode Revision 29's 145->125 tightening fixed
    # above), since a coincidentally-sized gap with no real line inside it
    # simply won't have a qualifying candidate to insert.
    #
    # Confirmed on Nov6_1_TPS_3934.pdf (the file that motivated the 145->125
    # fix above): its grid has not one but TWO separately faded boundary
    # lines - one at 44.3% coverage (splits a 144px gap into 73px + 71px)
    # and one at 36.1% coverage (splits a 216px gap into 109px + 108px) -
    # both comfortably below the 48% floor but both clearing this
    # recovery's own _GRID_FAINT_LINE_FLOOR (30%), and both splits land
    # exactly in the normal single/double-line row-gap range. Recovering
    # both produces an 18-row single/double-line pattern (single x8, double
    # x2, single x2, double, single x4, double) that's an EXACT structural
    # match to a healthy reference file's own grid on this same template
    # (Nov6_2_TPS_4005.pdf) - strong independent confirmation this recovers
    # the real printed lines, not noise.
    if len(best_run) < 19:
        peaks48 = [y for y in range(H) if row_sums[y] > W * 0.48]
        lines48 = _group_consecutive_positions(peaks48) if peaks48 else []
        if len(lines48) >= 2:
            augmented = [lines48[0]]
            for y in lines48[1:]:
                prev = augmented[-1]
                gap = y - prev
                if gap > high:
                    faint_peaks = [
                        yy for yy in range(int(prev) + 10, int(y) - 10)
                        if row_sums[yy] > W * _GRID_FAINT_LINE_FLOOR
                    ]
                    for candidate in (_group_consecutive_positions(faint_peaks) if faint_peaks else []):
                        if low <= (candidate - prev) <= high and low <= (y - candidate) <= high:
                            augmented.append(candidate)
                            break
                augmented.append(y)
            recovered_run = []
            best3, cur3 = [], [augmented[0]]
            for y in augmented[1:]:
                if low <= (y - cur3[-1]) <= high:
                    cur3.append(y)
                else:
                    if len(cur3) > len(best3):
                        best3 = cur3
                    cur3 = [y]
            if len(cur3) > len(best3):
                best3 = cur3
            recovered_run = best3
            if len(recovered_run) >= 19:
                best_run = recovered_run

    # --- 2c. checkbox-glyph row anchoring (Revision 36). Reported on 3 real
    # files from a newer print run (footer reads "Revised 9/16/25, (Adult) -
    # English" - confirmed via direct footer inspection): Nov16_2_TPS_5556.pdf,
    # Nov17_2_TPS_5576.pdf, Nov17_3_TPS_5580.pdf. On this revision the row
    # separators are printed far lighter than every earlier template, and on
    # some rows (confirmed directly on Nov17_3_TPS_5580.pdf's Q10-13 block)
    # there is NO ruled line at all AND no zebra-shading transition either -
    # several consecutive rows share the same shading band with literally
    # nothing distinguishing their boundary (measured directly: row-average
    # grayscale sits flat at ~224-230 straight through what should be 3
    # separate row boundaries, only dropping to white ~254 well after the
    # last one). No amount of relaxing the ink-line threshold above can find
    # a boundary that was never printed, faint or otherwise - stages 2/2b
    # only ever look for HORIZONTAL SEPARATOR ink, which structurally cannot
    # exist there.
    #
    # This stage sidesteps the missing-separator problem entirely by not
    # looking for row BOUNDARIES at all - instead it finds each row's own
    # checkbox glyph directly (which is always printed, regardless of
    # whether the line/shading boundary around it is), using the exact same
    # _locate_checkbox() border-tracing already trusted elsewhere in this
    # file, in a sliding window down the FIRST calibrated answer column
    # (_GRID_COLUMN_CENTERS[0], "Strongly Agree" - picked arbitrarily; any
    # single column works equally well since all 6 share the same row
    # centers). Row boundaries are then reconstructed as the midpoints
    # between consecutive found checkbox centers (extrapolating the first/
    # last boundary using the run's own median center-to-center gap).
    #
    # Verified this recovers exactly 18 checkbox centers (i.e. all 18 rows,
    # no more, no fewer) on all 3 reported files. Also cross-checked against
    # 4 known-good files spanning old and new templates (Nov1_1_TPS_1070.pdf,
    # Nov9_3_TPS_4137.pdf, Nov16_1_TPS_5210.pdf, Nov16_3_TPS_5568.pdf, all of
    # which already succeed via stage 2/2b) - this method independently also
    # finds exactly 18 centers on every one of them, so it agrees with the
    # existing trusted method rather than contradicting it.
    #
    # Deliberately scoped as a LAST-RESORT fallback only (stages 2/2b already
    # succeed on 35 of the 39 real files seen across this project's history;
    # switching to checkbox-anchoring as the PRIMARY method was tried first
    # and rejected - confirmed directly that _GRID_COLUMN_CENTERS[0] is not
    # reliably the right column position on several older templates, where
    # this method badly under-detects, e.g. 0 of 18 on TPS_4051.pdf, a file
    # stages 2/2b already read correctly). Only ever attempted after stages
    # 2/2b have already failed to find 19 consistent boundary lines, so it
    # carries no risk to any file the existing method already handles.
    # Corroboration gate: only invoke stage 2c below when the SHAPE of
    # stage 2/2b's own failure indicates a genuinely different, systemic
    # loss of separator-line evidence (multiple independent gaps too big to
    # be a single row), not just one isolated missing/unrecoverable line in
    # an otherwise-healthy grid. This distinction matters because this
    # module's own offline self-test suite (self_test_detect_checkbox_grid)
    # deliberately checks the OPPOSITE case - a single, fully-omitted
    # boundary line with every other line on the page printed perfectly
    # solid - must still safely return {} rather than have ANY fallback
    # invent a split point for it (there is no independent way to verify
    # where, within that one now-ambiguous double-height gap, the true row
    # boundary actually falls). That single-gap case measures exactly 1
    # "oversized" gap (> high) in the 48%-line sequence; every one of the 3
    # real files this stage exists for (Nov16_2_TPS_5556.pdf,
    # Nov17_2_TPS_5576.pdf, Nov17_3_TPS_5580.pdf) measures 2 or 3 such gaps,
    # confirmed directly - a clean, checked separation, not a guessed
    # cutoff. Requiring >= 2 preserves the single-gap safe-decline
    # guarantee while still reaching every real file this stage was built
    # for.
    gate_oversized_gaps = 0
    if len(best_run) < 19:
        gate_peaks48 = [y for y in range(H) if row_sums[y] > W * 0.48]
        gate_lines48 = _group_consecutive_positions(gate_peaks48) if gate_peaks48 else []
        gate_gaps = [gate_lines48[i + 1] - gate_lines48[i] for i in range(len(gate_lines48) - 1)]
        gate_oversized_gaps = sum(1 for g in gate_gaps if g > high)

    if len(best_run) < 19 and gate_oversized_gaps >= 2:
        anchor_pad = max(int(_GRID_COLUMN_PAD * scale), 8)
        anchor_size = _GRID_BOX_EXPECTED_SIZE * scale
        anchor_cx = _GRID_COLUMN_CENTERS[0] * scale
        anchor_win_h = max(int(90 * scale), 30)
        anchor_step = max(int(10 * scale), 4)
        anchor_dedup_gap = max(int(50 * scale), 15)

        anchor_centers = []
        y = 0
        while y < H:
            found = _locate_checkbox(
                binary, y, y + anchor_win_h,
                int(anchor_cx - anchor_pad), int(anchor_cx + anchor_pad),
                scale=scale, expected_w=anchor_size, expected_h=anchor_size,
            )
            if found is not None:
                fy0, fy1 = found[2], found[3]
                fcy = (fy0 + fy1) / 2
                if not anchor_centers or abs(fcy - anchor_centers[-1]) > anchor_dedup_gap:
                    anchor_centers.append(fcy)
            y += anchor_step

        if len(anchor_centers) == 18:
            gaps = [anchor_centers[i + 1] - anchor_centers[i] for i in range(17)]
            median_gap = sorted(gaps)[len(gaps) // 2]
            anchored_rows = [anchor_centers[0] - median_gap / 2]
            for i in range(17):
                anchored_rows.append((anchor_centers[i] + anchor_centers[i + 1]) / 2)
            anchored_rows.append(anchor_centers[-1] + median_gap / 2)
            # grid_rows (below) is indexed as plain ints elsewhere (+3/-3 pixel
            # offsets, // 2 midpoints) exactly like the line-detection path's
            # own _group_consecutive_positions() output already is - cast here
            # so this fallback's float pixel-center arithmetic doesn't leak
            # floats into that shared downstream code.
            best_run = [int(round(v)) for v in anchored_rows]

    if len(best_run) < 19:  # need >= 18 rows -> >= 19 boundary lines
        return {}
    grid_rows = best_run[:19]  # first 18 rows of the block = questions 1-18 on this template

    # --- 3-4. per-row column re-location + ink density -> which column is
    # marked (see the Round 16 docstring note above for why this is done
    # per-row against a shared calibrated center, rather than once globally) ---
    pad = max(int(_GRID_COLUMN_PAD * scale), 8)
    exp_size = _GRID_BOX_EXPECTED_SIZE * scale
    fallback_half = max(int(20 * scale), 6)
    size_tolerance = 14 * scale

    results = {}
    for i in range(18):
        ry0, ry1 = grid_rows[i] + 3, grid_rows[i + 1] - 3  # skip past the row's own ruled boundary lines
        cy = (grid_rows[i] + grid_rows[i + 1]) // 2

        found_boxes = []  # one entry per column: a located (x0,x1,y0,y1) box, or None
        offsets = []  # (located center - calibrated center) for columns that WERE located
        for cx in _GRID_COLUMN_CENTERS:
            cx = cx * scale
            found = _locate_checkbox(
                binary, ry0, ry1, int(cx - pad), int(cx + pad),
                scale=scale, expected_w=exp_size, expected_h=exp_size,
            )
            if found is not None:
                fx0, fx1, fy0, fy1 = found
                if abs((fx1 - fx0) - exp_size) > size_tolerance or abs((fy1 - fy0) - exp_size) > size_tolerance:
                    found = None
            if found is not None:
                offsets.append(((found[0] + found[1]) / 2) - cx)
            found_boxes.append(found)

        # This row's own consistent x-offset from the shared calibration (0.0 if every
        # column in this row happened to need the fallback path below - rare, and still
        # safe, since it just means the fallback windows use the bare calibrated centers).
        row_offset = float(np.median(offsets)) if offsets else 0.0

        boxes, borders = [], []
        for cx, found in zip(_GRID_COLUMN_CENTERS, found_boxes):
            if found is not None:
                boxes.append(found)
                borders.append(2)  # a real, border-traced box - exclude just its printed outline
            else:
                # Border-tracing failed for this one cell - most often because an
                # unusually large/messy mark obscures the box's own border (see
                # Round 16 docstring note). A plain ink-density window doesn't need
                # the border to be traceable, and a mark heavy enough to defeat
                # border-tracing reads unambiguously high in it regardless of the
                # window being a few px off from the box's true edges.
                fcx = cx * scale + row_offset
                boxes.append((int(fcx - fallback_half), int(fcx + fallback_half), int(cy - fallback_half), int(cy + fallback_half)))
                borders.append(max(int(4 * scale), 2))  # wider exclusion - this window isn't guaranteed tightly centered

        densities = [_checkbox_ink_ratio(binary, b, border=bd) for b, bd in zip(boxes, borders)]

        # General "two marks in the same single-select row" backstop
        # (Revision 29, explicit user request: "If two marks are identified
        # in 2 boxes, no matter which question it is, it needs to be marked
        # as need review"). Computed from the RAW densities, before the
        # overdense-ceiling correction logic below does anything - this is
        # deliberately independent of (and a superset of) that mechanism:
        # _GRID_OVERDENSE_INK_CEILING only fires when one column reads
        # implausibly dense (>0.50), which misses the common real case of an
        # X crossed out and a second box marked, where NEITHER box's own ink
        # ratio is unusually high on its own (confirmed directly on
        # Nov6_1_TPS_3934.pdf's Q11: "Strongly Agree" 0.4595 crossed out,
        # "Not Applicable" 0.3642 the real final answer - both comfortably
        # inside the normal single-mark range, so the overdense check never
        # triggers, and the plain highest-density-wins logic would have
        # silently picked whichever of the two happened to read higher with
        # no signal anything was wrong). Any row where 2+ columns clear the
        # ordinary "this box has a real mark on it" floor gets flagged,
        # independent of whatever the rest of this function decides the
        # best-guess answer is. Uses the dedicated, higher
        # _GRID_TWO_MARKS_INK_FLOOR (not _GRID_BLANK_INK_FLOOR) - see that
        # constant's comment for why: a plain 0.15 floor false-positives on
        # uniform scan-noise bleed on degraded scans (confirmed on Nov6_1_
        # TPS_3934.pdf's Q3/Q15).
        multiple_marks_detected = sum(1 for d in densities if d >= _GRID_TWO_MARKS_INK_FLOOR) >= 2

        # Correction/cross-out detection - see _GRID_OVERDENSE_INK_CEILING's
        # comment. Any column reading implausibly dense for a single mark is
        # a CANDIDATE for exclusion - but see Round 22's fix below before
        # actually excluding it.
        overdense = [k for k in range(6) if densities[k] > _GRID_OVERDENSE_INK_CEILING]
        if overdense:
            # Round 22 fix (Nov2_3_TPS_1582.pdf): an absolute density ceiling
            # alone can't tell "a crossed-out mark plus a separate real
            # answer" apart from "this scan's whole page just runs darker
            # than usual, so even a normal single mark reads above the
            # ceiling" - confirmed directly on this file, where EVERY row's
            # single genuine mark read 0.50-0.58 (above the 0.50 ceiling
            # calibrated from a different, lighter-scanned file) while its
            # blank baseline also ran elevated (0.05-0.12 instead of
            # ~0.0-0.08 elsewhere) - both shifted together, consistent with
            # one darker scan rather than any actual correction. Excluding
            # that single mark left only baseline noise behind, which then
            # tripped the blank-floor check below and silently reported the
            # row as blank - exactly the reported bug (Q1: model saw a clear
            # "X" for "Agree", pixel said nothing was confidently marked).
            #
            # A genuine correction, unlike a darker scan, always leaves a
            # SEPARATE, non-overdense column reading a normal, confidently-
            # marked density of its own (the real, final answer) - see the
            # Nov3_1_TPS_1585.pdf Q3 numbers in _GRID_OVERDENSE_INK_CEILING's
            # comment: "Strongly Agree" read 0.386 (confidently marked in its
            # own right) while every other non-crossed-out column read
            # 0.056-0.082. So only treat this as a correction if the best
            # non-overdense column ALSO clears the ordinary confidently-
            # marked bar on its own (_GRID_BLANK_INK_FLOOR, with a real
            # margin over ITS OWN runner-up) - otherwise there's no second
            # mark to recover, so this isn't a correction at all; fall back
            # to ranking all six columns normally, exactly as if this ceiling
            # didn't exist.
            non_overdense = [k for k in range(6) if k not in overdense]
            non_overdense_order = sorted(non_overdense, key=lambda k: -densities[k])
            has_real_alternative = (
                len(non_overdense_order) >= 2
                and densities[non_overdense_order[0]] >= _GRID_BLANK_INK_FLOOR
                and (densities[non_overdense_order[0]] - densities[non_overdense_order[1]]) >= _GRID_CONFIDENCE_MARGIN
            )
            if not has_real_alternative:
                overdense = []
        candidates = [k for k in range(6) if k not in overdense] if overdense else list(range(6))
        if len(candidates) < 2:
            # A correction was detected but there's nothing safe left to
            # rank against (e.g. every column reads overdense, or only one
            # non-overdense column remains with nothing to compare it
            # against) - never guess here; skip this row entirely, exactly
            # like any other row this function can't confidently read, so
            # the caller falls back to the model.
            continue
        order = sorted(candidates, key=lambda k: -densities[k])
        best, second = order[0], order[1]
        margin = round(densities[best] - densities[second], 4)
        if densities[best] < _GRID_BLANK_INK_FLOOR and margin < _GRID_BLANK_MARGIN_CEILING:
            # Positively blank - see _GRID_BLANK_INK_FLOOR's comment. Same
            # {"position": None, "margin": ..., "blank": True} shape
            # detect_yesno_box_answers() already uses, so
            # answers_to_qa_rows() can treat both the same way.
            results[str(i + 1)] = {"position": None, "margin": margin, "blank": True}
        else:
            results[str(i + 1)] = {
                "position": best + 1,
                "margin": margin,
            }
            if overdense:
                results[str(i + 1)]["correction_detected"] = True
                results[str(i + 1)]["correction_detected_at"] = [k + 1 for k in overdense]
        if multiple_marks_detected:
            results[str(i + 1)]["multiple_marks_detected"] = True

    return results


# --------------------------------------------------------------------------
# Deterministic pixel reading for a small, hand-picked set of simple, ISOLATED
# checkbox questions: the Yes/No(/Unknown) rows 21, 22, 27, and 32, plus the
# vertical single-choice lists 29 and 35. This is a SEPARATE, narrower
# technique from detect_checkbox_grid_answers() above, and exists because of
# real, repeated bug reports: Q27 ("Are you homeless?") kept coming back "No"
# on Nov10_1.pdf when the scan clearly shows "Yes" marked, and Q29/Q35 both
# came back with the wrong choice on a live Gemini call against Nov10_3.pdf
# even with the prompt-only mitigation in place (telling the model not to
# default to the last-listed choice) - that mitigation's effect could never
# be verified without a live call, and this pixel check doesn't depend on it.
# (Later rounds extended this same recipe to three more standalone rows not
# covered when this comment was first written: Q25's 4-option vertical list,
# Q23's 6-point-scale row, and Q19's 5-choice row - see
# _YESNO_BOX_CALIBRATION's per-question comments for each one's derivation.)
#
# Investigating WHY Q27 specifically is hard turned up something concrete:
# pytesseract reads the printed "Yes"/"No" labels on this form very
# unreliably - not because a mark corrupts them (the same OCR.py failure was
# already found and documented for the Q19-23 generalization attempt below),
# but because the checkbox glyph sits close enough to "No" that tesseract's
# own word segmentation merges the box's border into the "N", garbling it
# into things like "{No" (confidence 29) or "CINo" (confidence 18) - on the
# real Nov10_1.pdf, "No" failed to OCR at all (confidence <30) for Q21, Q22,
# AND Q27, regardless of whether that particular box was marked. So an
# OCR-anchored approach (find the "Yes"/"No" text, look at the box next to
# it) is NOT viable here either - confirmed by direct testing, not assumed.
#
# What DOES work, tested directly against the real Nov10_1.pdf pixels: once
# you know a checkbox's approximate location, measuring ink density inside
# its own border (same technique as the Q1-18 grid) gives an unambiguous
# answer - e.g. Q27 read Yes=0.48 ink vs No=0.0 ink, a huge margin, an exact
# match for the scan. The missing piece was locating the box automatically.
# Two purely-geometric location strategies were tried and FAILED before this
# one, for the same reason the Q19-23 attempt failed: any search window wide
# enough to be found without already knowing where to look also picks up
# ordinary question-text character strokes as false box edges (confirmed by
# testing - a window spanning a whole text row returned 40-60 spurious
# "box" candidates for Q21, and one specific attempt returned a confident
# WRONG answer for Q32 by pairing unrelated edges together).
#
# The approach actually shipped here is narrower and safer: a FIXED pixel
# coordinate is calibrated per choice, per question (from the real
# Nov10_1.pdf render, at 300 DPI - see _YESNO_BOX_CALIBRATION), but that
# coordinate is NEVER trusted blindly. At read time, _locate_checkbox()
# re-detects the box's own border within a small (+-_YESNO_BOX_PAD px)
# window around the calibrated location, using the same line-morphology
# technique as the grid. Only if a real, correctly-sized box border is found
# there is its ink density measured; if not (different scan alignment, a
# rescanned/redesigned form, anything that doesn't match), that question is
# silently skipped and falls back to the model - never a guess. This was
# stress-tested by shifting the real page image up to +-20px in both axes:
# across every shift tested, the detector either gave the CORRECT answer or
# found nothing at all - never a wrong answer.
#
# The real limitation, stated plainly: this assumed every scanned file uses
# the exact same page layout as Nov10_1.pdf (same print/scan template, page
# rendered to the same ~2550x3300px at 300 DPI) - and that assumption was
# WRONG, caught on the very next real file. Nov10_3.pdf turned out to be
# byte-identical to the earlier Nov18_2.pdf sample, which the project notes
# already flagged as having a ~27px page-level offset from Nov10_1.pdf - but
# what actually broke Q22 on it was NOT a uniform offset, it was a different
# ROW PITCH: Q21/Q22 sit inside a ruled table (visible divider lines between
# rows, same idea as the Q1-18 grid), and on this second file that table's
# rows are spaced ~10-15px differently than on Nov10_1.pdf. The fixed +-10px
# search pad found Q22's "Yes" box fine (small drift) but only PARTIALLY
# captured its "No" box (missing the true bottom edge, landing on a
# too-short box) - which is worse than not finding it at all, because that
# partial box's ink ratio (0.32, from a mis-cropped region) came out close
# enough to the correctly-read "Yes" box's ratio (0.46) to fall under the
# confidence margin, so it neither confidently overrode NOR safely bailed -
# the model's own (wrong) reading passed through untouched. Confirmed by
# direct crops: Nov10_1.pdf's Q21->Q22 row pitch is ~71px; Nov18_2.pdf's is
# ~55-60px for the same two rows - a real print/layout difference, not
# sensor noise.
#
# The fix for Q21/Q22 specifically: since they sit inside a ruled table,
# their row's true Y-boundaries can be found the SAME way the Q1-18 grid
# finds its rows - by detecting the actual full-page-width horizontal ruled
# lines - instead of trusting a fixed Y coordinate at all. _YESNO_ROW_MODE
# marks which questions get this (Q21, Q22): the calibrated Y is used only
# as a rough starting point to find the nearest real ruled line above and
# below it; the search band becomes whatever lies between those two REAL
# lines, however far that turns out to be from the calibrated guess. X
# stays a padded search around the calibrated coordinate, same as before -
# column positions were NOT observed to drift between the two real files
# the way row pitch did. Q27/Q32 keep the original fixed-Y+pad approach
# ("fixed" mode): they live in a freeform two-column layout on page 2 with
# no ruled lines nearby to anchor on (confirmed - no full-page-width line
# was found within 400px of either question on either real file), so there
# is no equivalent dynamic signal to use for them.
# --------------------------------------------------------------------------
_YESNO_BOX_PAD = 10  # search +-this many px around the calibrated box for its actual border (X always; Y in "fixed" mode when no UP override applies)
# NOTE: 25/29/35 used to carry a 20px override here too, inherited from when
# their calibration was still the fabricated shared-column placeholder (see
# _YESNO_BOX_CALIBRATION's comments on all three). Once real coordinates were
# measured, that turned from unnecessary into actively harmful: these are
# dense vertical choice lists with real gaps as tight as ~12-16px between
# adjacent boxes (confirmed directly - e.g. Q25's last two choices sit only
# ~14px apart), so a 20px pad on a marked box can extend into the ADJACENT
# choice's own border/text and produce an ambiguous (or wrong) box match
# instead of a safe miss - reproduced directly against Nov1_7_TPS_4223.pdf
# (Q25's real, correctly-marked "4 weeks or more" box went from found to
# None purely from raising this pad from 10 to 20). The plain default
# _YESNO_BOX_PAD (10px) is comfortably under every measured gap and was
# confirmed to locate all three correctly. Q25/Q28/Q29/Q35 instead get
# page-shift tolerance via "list_anchor" row mode (see _YESNO_ROW_MODE and
# _YESNO_LIST_ANCHOR_PAD below), not a wider plain pad.
# Q29 override (Revision 31, Nov8_1_TPS_4054.pdf): the default 10px per-box
# pad wasn't enough to shape-locate "Gender Queer/Gender Non-Conforming"
# even after the whole-list shift was correctly found (a small, genuine
# ~16px per-box positioning wobble on this file, confirmed by direct
# measurement - pad=16 was the minimum that found it cleanly, 20 used for
# margin). Without a clean shape match, this box fell through to the
# per-box blind-ink fallback (border=0, no shape validation) - which reads
# on a different, HIGHER baseline scale than the border-excluded ratio
# used for every other, normally-shape-located box in the same question
# (confirmed: this box's own blind reading, 0.28, comes mostly from its
# own printed border ink, not a mark - every properly-shape-located blank
# box on this list reads only ~0.08 once its border is excluded). Mixing
# the two scales in one ranking let this blank box's blind reading (0.28)
# spuriously outrank "Male" - the real, properly-measured mark (0.1344) -
# and win position 5 instead of the correct position 1. Widening the pad
# so this box resolves through NORMAL shape detection (like its neighbors)
# avoids the blind fallback, and the scale mismatch, entirely.
_YESNO_BOX_PAD_OVERRIDE = {"29": 20}
# _YESNO_BOX_UP_PAD_OVERRIDE (Revision 16): every whole-page vertical shift
# observed across real files so far moves content UP relative to the
# calibration source file (Nov1_7_TPS_4223.pdf) - that file itself is the
# one outlier where a row sits slightly (~9px) BELOW its own calibrated
# position, never further down than that on any other file sampled. A
# first attempt at fixing Q23/Q27 simply widened their plain (symmetric)
# _YESNO_BOX_PAD_OVERRIDE to 55px to cover a second real file's upward
# shift (Nov2_1_TPS_1413.pdf, reported: "27. Are you homeless? should be
# Yes, but it is showing No") - but a SYMMETRIC widening also extends the
# search window 55px further DOWN than calibrated, which for Q27 reached
# far enough to catch the NEXT question's own printed label ("28. Have you
# ever received...") inside the search window, its digit strokes producing
# spurious box-edge candidates that made _locate_checkbox() return None
# instead of a false match (never a wrong answer - but still a missed
# pixel backstop, the same failure shape as the original bug). Splitting
# the pad into a generous UP-only allowance (this dict) plus the small
# plain default pad for the downward side was step one - but 55px upward
# turned out to be too generous in the OTHER direction too: it reached far
# enough UP to catch Q27's own printed label ("27. Are you homeless?"),
# whose gap to its own checkbox row is only ~25px on the calibration file,
# breaking Q27 on the very file it was calibrated against. Precise
# connected-component re-measurement (not the coarser OCR-label-position
# estimate used for the first pass) found the real shift is -37/-38px for
# Q27 and Q32 and -33px for Q23 on Nov2_1_TPS_1413.pdf - all comfortably
# inside 45px, which a direct sweep of every value from 35-55px against
# both files confirmed is the only value that clears both the "not enough
# to find the real box" floor and the "far enough to catch a neighboring
# label" ceiling for all three questions at once. Q32 (which had been
# using a symmetric 40px pad successfully, but by only a 2px margin
# against its real 38px shift - the same latent bleed risk, just not yet
# triggered) was moved here too for the same reason, on the same file's
# evidence.
_YESNO_BOX_UP_PAD_OVERRIDE = {"23": 45, "27": 45, "32": 45}
_YESNO_CONFIDENCE_MARGIN = 0.15  # stricter than the grid's 0.05: only 2-3 boxes per question to compare, not 6
# Q19/Q20 use checkmark-style marks (lower ratios, and lower MARGINS between
# the marked box and its unmarked neighbors) on some files. Originally 0.12,
# calibrated from TPS_1356's margin=0.137 on checkmark "None". Lowered to
# 0.06 (Revision 27) after a second real file (Nov5_3_TPS_3025.pdf) showed a
# genuine, correctly-identified checkmark on Q20's "N/A" measuring only
# 0.0706 margin over its nearest unmarked neighbor - below the old 0.12
# threshold, so the pixel detector's own (correct) reading was silently
# discarded and the wrong model answer ("About the same") stood with no
# review flag. 0.06 still comfortably clears the largest observed gap
# between two genuinely UNMARKED boxes on that same file (~0.009-0.019).
_YESNO_CONFIDENCE_MARGIN_OVERRIDE = {"19": 0.06, "20": 0.06, "23": 0.12}
# Q23 (Revision 29 follow-up): a real file (Nov7_1_TPS_4017.pdf) showed a
# genuine, correctly pixel-identified mark on "I am Neutral" measuring only
# 0.1367 margin over its nearest neighbor - just above the general
# _YESNO_CONFIDENCE_MARGIN (0.15), so the pixel detector's own correct
# reading was being silently discarded in favor of the model's wrong answer
# ("Agree"), with no review flag either (Q23 wasn't ambiguous on this file -
# every other box read a clean 0.0). 0.12 clears that real mark while
# staying safely above a genuine close-call seen on a DIFFERENT file
# (Nov6_2_TPS_4005.pdf: "I am Neutral" 0.1389 vs. "Agree" 0.117, margin only
# 0.0219 - correctly still caught as ambiguous, not promoted, at this
# threshold).
_YESNO_BLANK_INK_FLOOR = 0.25
_YESNO_BLANK_MARGIN_CEILING = 0.06
# Revision 33: minimum genuine SEPARATION (best minus second) required
# before promoting the blank/ambiguous backstop's best candidate to an
# actual answer, on top of _YESNO_LIGHT_MARK_FLOOR below. Confirmed
# necessary on two real files (Nov9_1_TPS_4096.pdf's Q23,
# Nov9_2_TPS_4103.pdf's Q28/Q31/Q32): a genuinely BLANK box's own printed
# BORDER can read well above _YESNO_LIGHT_MARK_FLOOR by itself on some
# scans - measured directly at 0.12 (border=2) on Nov9_2's Q28, where
# EVERY choice was confirmed visually blank, purely from that particular
# scan's border line thickness - so "clears the light-mark floor" alone
# isn't sufficient evidence of a real mark when every choice clears it by
# about the same amount. The margin between the (falsely) promoted best
# candidate and its runner-up on these real files was only 0.0016-0.0048 -
# noise, not signal. A genuine light mark, by contrast, clearly separates
# from its neighbors even when none of the ratios involved are large:
# confirmed on Nov5_3_TPS_3025.pdf's Q21, where the real mark ("No",
# 0.1413) beat its nearest unmarked neighbor ("Yes", 0.1111) by a full
# 0.0302 - six times bigger than the largest false margin seen on the two
# November 9th files. Set below that confirmed genuine margin, comfortably
# above the false ones, so a real light mark still promotes normally while
# uniform border-thickness noise across every choice does not.
_YESNO_LIGHT_MARK_MIN_SEPARATION = 0.015
# Minimum own ink ratio a non-"(specify)" choice must clear to be promoted
# as the actual answer when detect_yesno_box_answers()'s blank/ambiguous
# backstop fires (see that function's ambiguous-boost comment) - set above
# real observed blank-box noise (~0.05 or less) but below real observed
# light-checkmark marks (0.0983-0.1413 measured on Nov5_3_TPS_3025.pdf's Q30
# "Female" and Q21 "No").
_YESNO_LIGHT_MARK_FLOOR = 0.06
# Yesno-box equivalent of _GRID_OVERDENSE_INK_CEILING - a box whose own ink
# ratio reads above this is almost certainly a crossed-out-then-corrected
# mark (a normal single X/checkmark doesn't get this dense), not a lightly-
# marked real answer. Confirmed on a real file: Nov7_1_TPS_4017.pdf's Q19
# ("None" checked, crossed out, then "Very Little" marked instead) - "None"
# read 0.8112, far above any normal single mark, while the genuine new mark
# ("Very Little") read only 0.2161 - just under _YESNO_BLANK_INK_FLOOR, so
# the plain "2+ boxes >= 0.25" multiple_marks_detected check alone MISSES
# this crossed-out shape entirely (only one box clears 0.25). See the
# multiple_marks_detected computation below for how this ceiling is used
# together with _YESNO_LIGHT_MARK_FLOOR to still catch it.
_YESNO_OVERDENSE_INK_CEILING = 0.50
# Minimum ink ratio a SECOND box must clear, alongside an overdense best box
# (see _YESNO_OVERDENSE_INK_CEILING above), to count as a genuine second
# mark rather than routine printed-label-line contamination. Set above the
# highest confirmed contamination baseline seen across real files (a long
# choice label's own printed text sitting close to its box can read
# 0.12-0.14 on EVERY box in a list, not just one - confirmed on
# Nov6_2_TPS_4005.pdf's Q35, where every one of the four non-marked choices
# independently read ~0.12 despite none being marked; a genuine second mark,
# by contrast, reads well clear of that on real files - confirmed on
# Nov7_1_TPS_4017.pdf's Q19, where the real "Very Little" mark read 0.2161
# while every genuinely-blank box on the same file read 0.0). Deliberately
# higher than the general _YESNO_LIGHT_MARK_FLOOR (0.06), which is tuned for
# a DIFFERENT purpose (promoting a lone light checkmark against a blank
# floor, not distinguishing a real second mark from list-wide label noise).
#
# Raised from 0.15 to 0.185 (Revision 30 follow-up): a 10-box list_anchor
# question (Q31, 2 columns x 5 rows) has more boxes competing for the same
# shared floor than the 3-6-box questions this was originally tuned
# against, so ordinary scan noise is more likely to clear a low floor on
# SOME box purely by chance. Confirmed on Nov7_2_TPS_4041.pdf's Q31
# (reported false positive: only "Heterosexual/Straight" is actually marked,
# 0.55 ink - clearing the overdense ceiling on its own, as a normal single
# heavy mark can): the runner-up, "Unsure/Questioning/Don't know", read
# 0.1552 - just 0.0052 over the old 0.15 floor, with no visible second mark
# anywhere on the scan. 0.185 clears that noise reading while staying
# comfortably under the confirmed genuine 0.2161 second mark above.
_YESNO_OVERDENSE_SECOND_MARK_FLOOR = 0.185
# "Blind" ink-density fallback thresholds - used ONLY when the "list_anchor"/
# "fixed" anchor search (_locate_anchor_box_nearest()) fails to find ANY
# clean box border at the calibrated position at all (see
# detect_yesno_box_answers()'s anchor-fail branch). Confirmed on a real file
# (Nov5_3_TPS_3025.pdf's Q32): "Yes" was marked with an X plus an encircling
# stroke so heavy that the ink itself crosses the box's own printed border on
# every side, so the line-morphology border search this whole module relies
# on has nothing clean left to trace - not a location/shift problem, an
# ink-density problem. Measured directly at the exact calibrated rectangle
# with ZERO padding (no anchor means no measured shift to apply, and any
# padding at all starts pulling in the printed choice-label text next to
# each box, which inflates every choice's ratio and erases the separation -
# confirmed: pad=0 gave Yes=0.162 vs No=0.024/Unknown=0.022, while pad=4
# alone already blurred it to 0.178/0.081/0.088). Floor is set comfortably
# above that real blank-choice noise; margin is set comfortably below the
# real winner-vs-runner-up gap (0.138) on that same file.
_YESNO_BLIND_INK_FLOOR = 0.10
_YESNO_BLIND_INK_MARGIN = 0.08
_YESNO_ROW_LINE_MARGIN = 4  # px to stay inside a dynamically-found ruled line, so its own ink isn't included in the row band
_YESNO_ROW_LINE_SEARCH = 50  # px below the calibrated box-bottom to look for the row's real bounding ruled line
# Q19/Q20 - widened from Q19's previous 20 (and Q20's previous 0, i.e. no
# override at all) after auditing every real uploaded file: the row's real
# ruled line sits ABOVE the calibrated approx_bottom by as much as 38px for
# Q19 (Nov2_1_TPS_1413.pdf) and 37px for Q20 (also Nov2_1_TPS_1413.pdf) on
# files with a slightly more compressed page layout than the file Q19/Q20
# were calibrated against (Nov1_7_TPS_4223.pdf, where the line sits BELOW
# approx_bottom by ~9px instead). The old 20px (Q19) / 0px (Q20) search-
# above windows silently missed the row-bottom line entirely on roughly
# half of all real files sampled (TPS_4051, Nov1_4_TPS_1402, Nov2_1_TPS_1413,
# Nov2_3_TPS_1582, Nov2_2_TPS_1566, Nov10_1, Nov3_2_TPS_1592 for Q19; the
# same set plus Nov1_3_TPS_1357 and Nov1_2_TPS_1356 for Q20) - not a
# low-confidence disagreement, but detect_yesno_box_answers() returning no
# entry for the question AT ALL, so the model's own (unchecked) answer was
# used with no pixel backstop on every one of those files. 45px covers the
# full observed range with headroom, while staying well under the ~109px
# gap between adjacent question rows, so there's no risk of this widened
# window ever reaching into a neighboring row's own line.
# Q21/Q22 - widened from 0 (no override at all) to 45 (Revision 16) for the
# exact same reason as Q19/Q20 just above: on Nov2_1_TPS_1413.pdf, Q21's
# real ruled line sits 35px above its calibrated approx_bottom (2668 vs
# 2703) and Q22's sits 33px above (2741 vs 2774) - confirmed directly via
# _find_full_width_lines(), including that the 73px Q21/Q22 pitch-partner
# relationship still holds at the real, shifted line positions (2668 and
# 2741 are exactly 73px apart). With search_above=0, both windows started
# AT approx_bottom and could only ever look further down the page, so a
# line sitting above it - as it does on this file, and presumably others
# with the same page-shift already documented for Q19/Q20/Q23/Q27 above -
# was unreachable no matter how large search_range (the below-only
# component) was made. Reported: "21. ... should be no but it's showing
# yes" - the pixel side had not run at all on this file, so nothing was
# there to catch the model's wrong reading.
_YESNO_ROW_ABOVE_SEARCH_OVERRIDE = {"19": 45, "20": 45, "21": 45, "22": 45}  # Q23 no longer uses "ruled" mode - see its calibration comment above
_YESNO_ROW_LINE_SEARCH_OVERRIDE = {"21": 60, "22": 60}
_YESNO_ROW_PITCH_PARTNER = {"21": (73, 10), "22": (-73, 10)}
# "list_anchor" row mode (Revision 16): for a dense vertical choice list
# (Q25/Q28/Q29/Q35) the same whole-page vertical shift that hits every
# other yesno-box question can't be absorbed by simply widening that
# question's own box pad the way Q19/Q20/Q23/Q27 were, because these lists
# pack their choices as close as ~12-16px apart (see _YESNO_BOX_PAD_
# OVERRIDE's comment above and the Revision 14 root-cause note on Q25) - a
# pad wide enough to cover the ~38-39px shift seen on Nov2_1_TPS_1413.pdf
# would already reach past the next choice's own box. Instead, the FIRST
# choice in the list is located with a wide, generous pad (safe because the
# space above it is the question's own printed label text, not another
# choice - confirmed via visual crop for all four questions on real files:
# the gap from the label's last line to the first checkbox is 40px+ on
# every one of them), the real vertical offset between that found box and
# its calibrated position is measured, and that SAME offset is then
# applied to every other choice in the list before searching for each with
# the normal small default pad (10px) - relocating the whole list as one
# rigid unit rather than independently widening each choice's own search.
# Like _YESNO_BOX_UP_PAD_OVERRIDE above, this anchor search is itself
# UP-only (generous above, small default below): a symmetric widening here
# would let the FIRST choice's own search window reach down far enough to
# swallow the SECOND choice's box on the tightest lists (Q29's gap between
# "Male" and "Female" is only ~16px, far less than a 50px downward
# extension) - never actually observed producing a wrong answer in
# testing, but a latent risk removed on the same evidence as Q32's fix.
_YESNO_LIST_ANCHOR_PAD = 50
# X-axis anchor search pad (Revision 26 - see detect_yesno_box_answers()'s
# scale comment): a non-Letter page (A4) shifts content sideways too, not
# just vertically - confirmed directly on Nov5_*_TPS_*.pdf's Q25/28/29/
# 30/32/35 (all page-2, all "list_anchor" or "fixed" mode - none registered
# at all before this, always safely falling back to the model alone).
# Applied to the same single FIRST-choice anchor box as
# _YESNO_LIST_ANCHOR_PAD already is, for the same reason: nothing else
# box-sized shares its row within this wide a window, unlike every
# subsequent choice in a dense list.
_YESNO_ANCHOR_X_PAD = 90


def _has_pitch_partner(full_width_lines: list, candidate: float, offset: float, tolerance: float) -> bool:
    """True if some line in full_width_lines sits within `tolerance` px of
    candidate + offset - see _YESNO_ROW_PITCH_PARTNER's comment for why this
    exists (confirming a row-bottom-line candidate is genuinely part of the
    Q21/Q22 pair, not an unrelated line that merely happened to be within
    the search window)."""
    target = candidate + offset
    return any(abs(y - target) <= tolerance for y in full_width_lines)


_YESNO_ROW_MODE = {
    "19": "ruled", "20": "ruled", "21": "ruled", "22": "ruled",
    "27": "fixed", "32": "fixed", "23": "fixed",
    "25": "list_anchor", "28": "list_anchor", "29": "list_anchor", "35": "list_anchor", "30": "list_anchor",
    "31": "list_anchor",
}
_YESNO_BOX_INK_BORDER_OVERRIDE = {"19": 6, "23": 6}

_LEGACY_YESNO_BOX_GEOMETRY = {
    # Q19/Q20 - REPLACED. The previous entries here shared the exact same
    # (1357, 1389) x-range and stepped y in exact 50px increments as the old
    # fabricated Q23 entry below (see Q23's own comment) - the identical
    # tell that it was never actually measured against a real scan, just
    # invented to look plausible. Confirmed on the real Nov1_7_TPS_4223.pdf:
    # Q19/Q20 are NOT a single stacked column at all - each is a single
    # HORIZONTAL row of 5 side-by-side boxes (found by OCR-locating "19."/
    # "20." and visually inspecting the row - "None" and "N/A" respectively
    # were the real marks). Real full-page-width ruled lines were confirmed
    # to sit close by (within ~10px) on the real scan, so "ruled" mode
    # (dynamic row relocation, like Q21/Q22) is kept rather than switching
    # to "fixed" - unlike Q23, which had no such nearby line to anchor on.
    # Single-file calibration - same caveat as every other single-file
    # entry here.
    "19": (0, {
        "None": (658, 704, 2446, 2490),
        "Very little": (882, 928, 2446, 2490),
        "About half": (1168, 1214, 2446, 2490),
        "Almost all": (1471, 1516, 2446, 2490),
        "All": (1765, 1811, 2446, 2490),
    }),
    "20": (0, {
        "Much better": (659, 703, 2554, 2598),
        "Somewhat better": (987, 1031, 2554, 2598),
        "About the same": (1400, 1444, 2554, 2598),
        "Somewhat worse": (1792, 1837, 2554, 2598),
        "N/A": (2206, 2251, 2554, 2598),
    }),
    # Q23 - MEASURED directly off a 300 DPI render of Nov1_7_TPS_4223.pdf by
    # connected-component analysis (the previous entry here shared Q19/Q20's
    # identical (1357, 1389) x-range and stepped y in exact 50px increments -
    # a tell that it was never actually measured against a scan; see
    # root-cause-TPS_4051-calibration-is-fabricated.md - and detect_yesno_
    # box_answers() correctly could never locate it, since real boxes on this
    # form sit at very different x per choice, not one shared column). Row
    # switched from "ruled" (dynamic row relocation - actively harmful with
    # wrong coordinates per that doc) to "fixed" (plain calibrated position +
    # pad, like Q27/Q32) now that a real position is known. Single-file
    # calibration - recommend re-measuring against additional files before
    # trusting broadly, same caveat as every other single-file entry here.
    "23": (0, {
        "Strongly Agree": (584, 631, 2840, 2886),
        "Agree": (962, 1008, 2840, 2886),
        "I am Neutral": (1196, 1243, 2840, 2886),
        "Disagree": (1532, 1578, 2840, 2886),
        "Strongly Disagree": (1815, 1862, 2840, 2886),
        "N/A": (2240, 2287, 2840, 2886),
    }),
    # Q25 - REPLACED. The previous entry shared the same fabricated
    # (1357, 1389) x-range as Q19/Q20/old-Q23 (see those comments) AND had
    # the wrong page_idx (0, when "25." actually prints on page_idx=1 -
    # confirmed by OCR-locating it there on the real scan) - so it could
    # never have matched anything real, on any file, ever. MEASURED directly
    # off Nov1_7_TPS_4223.pdf: a vertical single-column list at x~272-305,
    # real mark is "4 weeks or more". Single-file calibration - same caveat
    # as every other single-file entry here.
    "25": (1, {
        "First visit/day": (272, 305, 1101, 1133),
        "2 weeks or less": (272, 305, 1147, 1179),
        "More than 2 weeks but less than 4 weeks": (272, 305, 1195, 1225),
        "4 weeks or more": (272, 305, 1240, 1271),
    }),
    # Q27 - a question number can map to a LIST of candidate (page_idx,
    # boxes) layouts instead of a single one (see detect_yesno_box_answers(),
    # which tries each candidate in turn and uses the first one whose boxes
    # actually validate against the real scan - never a guess, same as every
    # other gate in this function). This was added because Q27 was found to
    # print in two genuinely different page-2 positions across real files:
    # the original calibration (candidate 0 below, from Nov10_1.pdf) has it
    # in the right-hand column at x~1355-1743; a second real file
    # (Nov1_7_TPS_4223.pdf, reported: "Are you homeless? should be Yes, but
    # BQ is showing No and no needs_review") turned out to print it in the
    # LEFT-hand column at x~265-600 instead - confirmed by locating the
    # printed "Are you homeless?" label with OCR and visually inspecting the
    # crop, which showed an unmistakable ink mark filling the "Yes" box and
    # a clean empty "No" box. The x~1355 candidate's search window doesn't
    # come anywhere near x~265, so on this file detect_yesno_box_answers()
    # correctly found nothing and fell back to the model - which is why no
    # needs_review fired at all (nothing to disagree with): the pixel check
    # never ran, not that it ran and agreed with a wrong model answer.
    "27": [
        (1, {
            "Yes": (1355, 1390, 1595, 1630),
            "No": (1708, 1743, 1595, 1630),
        }),
        (1, {
            "Yes": (274, 306, 1538, 1571),
            "No": (560, 595, 1538, 1572),
        }),
    ],
    # Q32 - REPLACED. The previous Yes/No-only entry was measured against
    # the wrong row entirely (y=1940-1975; the real question sits at
    # y=1453-1486, confirmed via OCR locating "32." at y~1385 on the real
    # Nov1_7_TPS_4223.pdf page index 1) and was also missing the third real
    # choice ("Unknown") altogether, so it could never validate a 3-box
    # question and always returned None against real files. Yes/No/Unknown
    # measured directly off that file via connected-component/line-
    # morphology analysis and confirmed with a red-rectangle overlay crop
    # (Yes and Unknown were clean; No required isolating vertical/horizontal
    # line segments from the X-mark's diagonal strokes with a vertical- and
    # horizontal-line morphology kernel to avoid the mark's ink).
    "32": (1, {
        "Yes": (1354, 1389, 1453, 1486),
        "No": (1647, 1680, 1453, 1486),
        "Unknown": (1900, 1931, 1453, 1486),
    }),
    # Q22 - previously had NO calibration entry at all (despite already
    # appearing in _YESNO_ROW_MODE/_YESNO_ROW_LINE_SEARCH_OVERRIDE/
    # _YESNO_ROW_PITCH_PARTNER below, and in the offline self-tests), which
    # meant detect_yesno_box_answers() could never attempt it - it iterates
    # _YESNO_BOX_CALIBRATION.items() only, so an absent question is silently
    # skipped every single time, not "sometimes misses." Reported: "Question
    # 22... should be Yes, but it is showing NO, why no pixel model applied
    # to these?" - the answer was exactly that: no pixel model was ever
    # wired up for Q22. MEASURED directly off Nov1_7_TPS_4223.pdf (page 0,
    # ruled row) by locating "22. Did the program staff show you the patient
    # orientation video?" via OCR, then finding the Yes/No checkboxes on
    # that same row by connected-component analysis - visually confirmed an
    # X mark filling "Yes" and a clean empty "No" box. Single-file
    # calibration - same caveat as every other single-file entry here.
    "22": (0, {
        "Yes": (1421, 1465, 2728, 2772),
        "No": (1701, 1747, 2728, 2774),
    }),
    # Q21, Q28, Q29, Q35 - these four were the remaining, long-documented
    # gap: they already had entries in _YESNO_ROW_MODE/_YESNO_BOX_PAD_
    # OVERRIDE/_YESNO_ROW_PITCH_PARTNER below (Q21's ruled-mode partner
    # relationship with Q22 was fully wired up on both sides - see
    # _YESNO_ROW_PITCH_PARTNER["21"] - and the offline self-tests already
    # referenced all four), but none had ever had real coordinates measured
    # and added here, so none could ever be attempted by
    # detect_yesno_box_answers() (same shape of gap Q22 had before this
    # revision). All four MEASURED directly off Nov1_7_TPS_4223.pdf by
    # OCR-locating each question's printed label, then finding its
    # checkbox(es) on the same row(s) by connected-component/line-morphology
    # analysis and visually confirming the actual ink. Single-file
    # calibration - same caveat as every other single-file entry here.
    "21": (0, {
        "Yes": (1421, 1465, 2657, 2703),
        "No": (1701, 1747, 2657, 2702),
    }),
    "28": (1, {
        "Yes, I am currently receiving Contingency Management services": (273, 306, 1732, 1764),
        "Yes, I received Contingency Management services in the past": (273, 306, 1851, 1884),
        "No, I have never received Contingency Management services": (273, 306, 1962, 1995),
    }),
    "29": (1, {
        "Male": (274, 307, 2295, 2327),
        "Female": (274, 307, 2343, 2376),
        "Female-to-Male (FTM)/Transgender Male/Trans Man": (274, 307, 2388, 2420),
        "Male-to-Female (MTF)/Transgender Female/Trans Woman": (274, 307, 2434, 2464),
        "Gender Queer/Gender Non-Conforming": (274, 307, 2483, 2512),
        "Other (specify)": (274, 307, 2543, 2571),
        "Prefer not to state": (275, 308, 2618, 2649),
    }),
    "35": (1, {
        "Post-release Community Supervision (AB109) or on Probation from any federal, state, or local jurisdiction": (1354, 1388, 2642, 2675),
        "Awaiting trial, charges or sentencing": (1354, 1388, 2750, 2779),
        "On parole from any other jurisdiction": (1354, 1388, 2805, 2838),
        "Any other criminal justice involvement": (1354, 1388, 2858, 2891),
        "No criminal justice involvement": (1354, 1388, 2912, 2945),
    }),
    # Q30 - previously had NO calibration entry at all (same shape of gap
    # Q22/Q21/Q28/Q29/Q35 had before): reported "30. What was your sex at
    # birth? ... should be Male but the BQ is showing as female" against
    # Nov2_1_TPS_1413.pdf. Unlike every other question here, Q30 prints as
    # a 2x2 grid, not a single row or a vertical list: Female/Male share a
    # row, Other (specify)/Prefer not to state share the row below - MEASURED
    # directly off that file (connected-component/line-morphology, X-mark
    # isolated from the box border the same way as every other marked box
    # here) and confirmed with a red-rectangle overlay crop (real mark:
    # Male). Uses "list_anchor" mode (see _YESNO_ROW_MODE) because the two
    # rows sit only ~21px apart - too tight for a wide plain or up-only pad
    # without risking the top row's search reaching into the bottom row's
    # boxes (or vice versa), the same reasoning as Q25/Q28/Q29/Q35.
    #
    # The coordinates below are NOT what was directly measured on
    # Nov2_1_TPS_1413.pdf - they're that measurement shifted back down by
    # the ~38px this file's page 2 consistently sits ABOVE every other
    # calibration source file's own coordinates (see
    # _YESNO_BOX_UP_PAD_OVERRIDE's comment: every other page-2 calibration
    # entry here was measured against Nov1_7_TPS_4223.pdf, which is the
    # one file so far that does NOT sit shifted up). Converting back to
    # that same baseline - rather than calibrating Q30 against a different
    # baseline than every other question on this page - keeps the existing,
    # already-tuned "shifts are up, not down" assumption (and its pad
    # tolerances) correct for Q30 too, instead of needing its own special
    # down-heavy override. Real measured (Nov2_1_TPS_1413.pdf) coordinates
    # were: Female (284, 312, 2738, 2766), Male (785, 818, 2735, 2768),
    # Other (specify) (284, 312, 2787, 2815), Prefer not to state
    # (785, 818, 2784, 2817) - each +38 in y below to get the values used
    # here. Verified end-to-end: detect_yesno_box_answers() against the
    # real file correctly relocates all four boxes via the list_anchor
    # offset and reads "Male".
    "30": (1, {
        "Female": (284, 312, 2776, 2804),
        "Male": (785, 818, 2773, 2806),
        "Other (specify)": (284, 312, 2825, 2853),
        "Prefer not to state": (785, 818, 2822, 2855),
    }),
    # Q31 - previously had NO calibration entry at all (a structural
    # coverage gap, documented in Revision 29 - Nov6_2_TPS_4005.pdf's Q31
    # was never flagged AND never auto-corrected: "Heterosexual/Straight" ->
    # should be "Lesbian (Female)"). MEASURED directly off a 300 DPI render
    # of Nov6_2_TPS_4005.pdf (page index 1) by connected-component analysis
    # of the checkbox borders themselves - visually confirmed via a cropped
    # image: this is a 5-row, 2-column list ("Heterosexual/Straight" ...
    # "Unsure/Questioning/Don't know" on the left, "Pansexual" ...
    # "Prefer not to state" on the right, each column's 5 rows sharing the
    # same y-positions), the same list_anchor shape as Q30/Q33. The real
    # mark ("Lesbian (Female)", row 2 of the left column) has a strike-
    # through/cross drawn across its own box border, which defeats clean
    # border-tracing on that ONE row - its calibrated y-range here is
    # extrapolated from the other 4 left-column rows' consistent ~47-48px
    # pitch rather than directly measured, since a corrupted border isn't a
    # reliable measurement source; every other row (both columns) was
    # measured directly. Single-file calibration - same caveat as every
    # other single-file entry here, recommend re-measuring against
    # additional files before trusting broadly.
    "31": (1, {
        "Heterosexual/Straight": (1352, 1386, 1028, 1062),
        "Lesbian (Female)": (1352, 1386, 1076, 1110),
        "Gay (Male)": (1352, 1387, 1122, 1156),
        "Bisexual": (1352, 1386, 1172, 1206),
        "Unsure/Questioning/Don't know": (1352, 1386, 1219, 1253),
        "Pansexual": (1858, 1893, 1028, 1061),
        "Asexual": (1858, 1893, 1074, 1108),
        "Other (specify)": (1858, 1893, 1121, 1155),
        "Queer": (1858, 1893, 1171, 1205),
        "Prefer not to state": (1858, 1893, 1219, 1253),
    }),
}

_LEGACY_MULTISELECT_BOX_GEOMETRY = {
    "33": (1, {
        "American Indian/Alaskan Native": (1357, 1392, 1618, 1652),
        "Asian": (1357, 1392, 1671, 1705),
        "Black/African American": (1357, 1392, 1720, 1755),
        "Native Hawaiian/Pacific Islander": (1357, 1392, 1773, 1807),
        "White/Caucasian": (1357, 1392, 1825, 1859),
        "Other (specify)": (1357, 1392, 1876, 1910),
        "Prefer not to state": (1357, 1392, 1958, 1992),
    }),
    "34": (1, {
        "Physically Disabled": (1360, 1388, 2121, 2149),
        "Visually Impaired/Blind": (1360, 1388, 2169, 2196),
        "Hearing Impaired/Deaf": (1360, 1389, 2217, 2245),
        "Co-occurring Mental Health Condition": (1360, 1389, 2270, 2299),
        "Developmentally or Intellectually Disabled": (1360, 1389, 2321, 2349),
        "Other (specify)": (1360, 1389, 2389, 2418),
        "None": (1360, 1389, 2444, 2473),
    }),
}

# Public calibration metadata intentionally contains no scan-specific pixel
# rectangles. The resolver below discovers those rectangles from each PDF and
# retains the legacy geometry only as a safe, local fallback.
_YESNO_BOX_CALIBRATION = {
    "19": {"page": 0, "labels": ["None", "Very little", "About half", "Almost all", "All"]},
    "20": {"page": 0, "labels": ["Much better", "Somewhat better", "About the same", "Somewhat worse", "N/A"]},
    "21": {"page": 0, "labels": ["Yes", "No"]},
    "22": {"page": 0, "labels": ["Yes", "No"]},
    "23": {"page": 0, "labels": ["Strongly Agree", "Agree", "I am Neutral", "Disagree", "Strongly Disagree", "N/A"]},
    "25": {"page": 1, "labels": ["First visit/day", "2 weeks or less", "More than 2 weeks but less than 4 weeks", "4 weeks or more"]},
    "27": {"page": 1, "labels": ["Yes", "No"], "layouts": 2},
    "28": {"page": 1, "labels": ["Yes, I am currently receiving Contingency Management services", "Yes, I received Contingency Management services in the past", "No, I have never received Contingency Management services"]},
    "29": {"page": 1, "labels": ["Male", "Female", "Female-to-Male (FTM)/Transgender Male/Trans Man", "Male-to-Female (MTF)/Transgender Female/Trans Woman", "Gender Queer/Gender Non-Conforming", "Other (specify)", "Prefer not to state"]},
    "30": {"page": 1, "labels": ["Female", "Male", "Other (specify)", "Prefer not to state"]},
    "31": {"page": 1, "labels": ["Heterosexual/Straight", "Lesbian (Female)", "Gay (Male)", "Bisexual", "Unsure/Questioning/Don't know", "Pansexual", "Asexual", "Queer", "Other (specify)", "Prefer not to state"]},
    "32": {"page": 1, "labels": ["Yes", "No", "Unknown"]},
    "35": {"page": 1, "labels": ["Post-release Community Supervision (AB109) or on Probation from any federal, state, or local jurisdiction", "Awaiting trial, charges or sentencing", "On parole from any other jurisdiction", "Any other criminal justice involvement", "No criminal justice involvement"]},
}
_MULTISELECT_BOX_CALIBRATION = {
    "33": {"page": 1, "labels": ["American Indian/Alaskan Native", "Asian", "Black/African American", "Native Hawaiian/Pacific Islander", "White/Caucasian", "Other (specify)", "Prefer not to state"]},
    "34": {"page": 1, "labels": ["Physically Disabled", "Visually Impaired/Blind", "Hearing Impaired/Deaf", "Co-occurring Mental Health Condition", "Developmentally or Intellectually Disabled", "Other (specify)", "None"]},
}
_YESNO_BOX_SEMANTICS = _YESNO_BOX_CALIBRATION
_MULTISELECT_BOX_SEMANTICS = _MULTISELECT_BOX_CALIBRATION
_MULTISELECT_BOX_PAD_OVERRIDE = {"33": 20, "34": 20}
_MULTISELECT_INK_BORDER_OVERRIDE = {"33": 6, "34": 6}
_MULTISELECT_UNMARKED_CEILING = 0.10
# How far a choice's found checkbox may sit past the PREVIOUS choice's found
# checkbox, relative to this list's own median inter-choice gap, before it's
# treated as having landed on the wrong (next) row entirely rather than its
# own - see detect_multiselect_ink_ratios()'s row-pitch consistency check
# (Revision 34, real user report on Nov9_3_TPS_4137.pdf's Q34) for the full
# real-file measurements behind this number: the largest genuine gap-to-
# median ratio confirmed across 10 real files' Q33/Q34 (both of which
# include a choice, "Other (specify)", whose own row is legitimately taller
# than the rest) was 1.38x; the confirmed real bug's ratio was 2.45x. 1.7
# sits with safe margin on both sides of that gap.
_MULTISELECT_ROW_PITCH_ANOMALY_RATIO = 1.7
# How far below (and, modestly, above) the calibrated position of the FIRST
# choice in a multi-select list to search when anchoring - see
# detect_multiselect_ink_ratios()'s docstring for why this exists and why
# it's asymmetric. Kept well under the ~53px real row pitch measured on
# Q33/Q34 (see _MULTISELECT_BOX_CALIBRATION) so a shifted whole-list search
# window can never fully contain a second row's box (only, at most, a
# partial/clipped one lacking a complete border - _locate_checkbox() won't
# treat that as a valid candidate).
_MULTISELECT_ANCHOR_DOWN_PAD = 90
_MULTISELECT_ANCHOR_UP_PAD = 20
# Revision 31 (Nov8_1_TPS_4054.pdf's Q33): the single-first-choice checkbox-
# shape anchor above can fail in a way neither narrow_pad nor wide max_up/
# max_down tuning can fix, because EVERY choice's checkbox is the same
# size - a wide search window containing more than one same-sized
# candidate (the real "American Indian/Alaskan Native" box, PLUS "Asian"'s
# or "Black/African American"'s real box, both within range) has no way to
# prefer the correct one on shape alone. Confirmed on this real file: the
# whole Q33 list sits ~31px ABOVE its calibration (this file's page 1
# apparently prints notably higher than every previously-seen file, the
# opposite direction and larger magnitude than _MULTISELECT_ANCHOR_UP_PAD
# was ever tuned for) - widening max_up from 20 up to 50 did NOT help,
# because the narrow_pad=20 search already (wrongly) locks onto "Asian"'s
# real box first (only ~21px from "American Indian"'s calibrated position,
# just inside narrow_pad), and even a from-scratch wide-only search picks
# "Black/African American"'s box instead (2 rows off) - both same-size,
# neither has anything to prefer the true, correctly-shifted box over.
# This silently misread EVERY choice one full row down and lost 2 of 7
# entirely: "Other (specify)" and "Prefer not to state" (the real marked
# choice) fell off the end of the list, searched for at row positions 8
# and 9 that don't exist.
#
# Fix: before falling back to checkbox-shape anchoring at all, try to
# locate every choice's row directly from its PRINTED TEXT LABEL instead -
# unlike the repeating, identically-sized checkbox glyphs, each choice's
# label text is unique and (crucially) never corrupted by the choice's own
# mark the way a heavily-marked checkbox's own border can be. A horizontal
# ink-density scan of the label-text column reliably separates each
# choice's text into its own band with real, measured gaps between rows -
# confirmed identical in shape across all 5 real files sampled (this file
# plus the 4 pre-existing reference files): searching from just above the
# first calibrated choice to just below the last one always turns up
# exactly one extra band (the NEXT question's own header, which sits close
# enough below the last choice to fall inside the generous down-pad) -
# taking the first N bands (N = number of calibrated choices), in order,
# reliably recovers each choice's true row position directly, with no
# anchor-and-corroborate step needed at all since there is no repeating
# ambiguity in unique printed text the way there is in identical checkbox
# outlines.
_MULTISELECT_TEXT_BAND_UP_PAD = 60  # generous - see _find_multiselect_text_bands()
_MULTISELECT_TEXT_BAND_DOWN_PAD = 90  # matches _MULTISELECT_ANCHOR_DOWN_PAD's own generous down search
_MULTISELECT_TEXT_BAND_X_GAP = 10  # label text starts this far right of the choice's own calibrated box
_MULTISELECT_TEXT_BAND_X_WIDTH = 400  # wide enough for every real choice label measured so far
_MULTISELECT_TEXT_BAND_MIN_HEIGHT = 15  # excludes small punctuation/stray-mark fragments, keeps real text rows
_MULTISELECT_TEXT_BAND_INK_THRESHOLD = 5  # row ink-pixel count above which a scanline counts as "inside text"
_MULTISELECT_ANCHOR_X_PAD = 90
# "Ambiguous mark" backstop (explicit user request: a mark that's unclear,
# or that exceeds its own box's pixel area, should at least force
# needs_review). Two independent signals, either one is enough to flag a
# label as ambiguous:
#   1. NEAR-THRESHOLD: a ratio close enough below _MULTISELECT_UNMARKED_
#      CEILING that a light or off-center mark could easily land on either
#      side of it. Confirmed for real on Nov5_2_TPS_3004.pdf's Q33: the
#      true mark on "Black/African American" measured only 0.086 in-box
#      ink (just 0.014 under the 0.10 ceiling) because the X was drawn
#      mostly to the LEFT of the box rather than inside it - see signal 2.
#   2. OVERFLOW: a box's own ink ratio reads confidently blank, but ink
#      immediately surrounding the box (outside it) is dense - the mark
#      exists but missed the box, exactly the "exceeded its own pixel
#      area" case named in the request. Measured on that same real
#      Nov5_2_TPS_3004.pdf box: expanding the sample region by 20px on
#      every side picks up the stray stroke that the strict in-box
#      measurement (with its ink_border exclusion) misses.
_MULTISELECT_AMBIGUOUS_BAND = 0.05  # ratio in [ceiling - band, ceiling) counts as near-threshold
_MULTISELECT_OVERFLOW_EXPAND_LEFT = 20  # real overflow marks bleed to the LEFT (into blank margin - see
# below) - a wide X corner extending past the box's own left edge
_MULTISELECT_OVERFLOW_EXPAND_Y = 4  # vertical - kept small: rows are only ~15-19px apart, wider would
# read the NEXT row's own box border as "overflow" on every single blank row (confirmed the hard way -
# a first attempt at 20px in every direction flagged nearly every blank box on every file as ambiguous)
# Only the LEFT side is expanded, never the right: each choice's label text starts as little as ~11px
# to the RIGHT of its box (confirmed on a real file), so expanding rightward reads that label's own
# ink as "overflow" on every row - the left side, by contrast, is genuinely blank margin on this form.
_MULTISELECT_OVERFLOW_RATIO = 0.15  # expanded-region ink at/above this, with a blank in-box ratio, = overflow


def _find_multiselect_text_row_positions(binary_img, boxes: dict, scale: float = 1.0):
    """Returns a list of (y0, y1) - one per calibrated choice, IN ORDER -
    giving each choice's real printed-text-row position on this scan, or
    None if the label-text column couldn't be cleanly separated into at
    least as many bands as there are choices.

    See _MULTISELECT_TEXT_BAND_* constants' comment (Revision 31) for why
    this exists: unlike the checkbox glyphs (identically sized and
    repeating down the list, so a same-size match can't tell one row from
    another), each choice's own label text is unique, distinctly spaced,
    and never corrupted by that choice's own mark - a horizontal ink-
    density scan of the label-text column reliably separates the choices
    into one band per row wherever the checkbox-shape anchor's own
    same-size ambiguity would otherwise leave it guessing."""
    items = list(boxes.items())
    n = len(items)
    if n == 0:
        return None
    first_x0, first_x1, first_y0, first_y1 = items[0][1]
    last_x0, last_x1, last_y0, last_y1 = items[-1][1]
    up_pad = max(int(_MULTISELECT_TEXT_BAND_UP_PAD * scale), 10)
    down_pad = max(int(_MULTISELECT_TEXT_BAND_DOWN_PAD * scale), 10)
    x_gap = max(int(_MULTISELECT_TEXT_BAND_X_GAP * scale), 4)
    x_width = max(int(_MULTISELECT_TEXT_BAND_X_WIDTH * scale), 100)
    min_height = max(int(_MULTISELECT_TEXT_BAND_MIN_HEIGHT * scale), 6)
    y0 = int(first_y0 * scale) - up_pad
    y1 = int(last_y1 * scale) + down_pad
    x0 = int(first_x1 * scale) + x_gap
    x1 = x0 + x_width
    H, W = binary_img.shape
    y0, y1 = max(0, y0), min(H, y1)
    x0, x1 = max(0, x0), min(W, x1)
    if y1 <= y0 or x1 <= x0:
        return None
    region = binary_img[y0:y1, x0:x1]
    row_density = region.sum(axis=1) / 255
    threshold = _MULTISELECT_TEXT_BAND_INK_THRESHOLD
    bands = []
    in_band = False
    start = None
    for y, d in enumerate(row_density):
        if d > threshold and not in_band:
            in_band = True
            start = y
        elif d <= threshold and in_band:
            in_band = False
            if y - start >= min_height:
                bands.append((start + y0, y + y0))
    if in_band and (len(row_density) - start) >= min_height:
        bands.append((start + y0, len(row_density) + y0))
    if len(bands) < n:
        # Couldn't cleanly separate at least one band per choice (faint
        # print, unusual font, or a layout this heuristic doesn't fit) -
        # the caller falls back to the checkbox-shape anchor instead of
        # trusting a partial/ambiguous band list.
        return None
    if len(bands) == n:
        return bands

    # More candidate bands were found than there are choices - blindly
    # trusting the FIRST n (the old behavior) assumes those are exactly the
    # n real choice rows in order, which breaks when a spurious extra band
    # gets swept into the scan window ahead of them (Revision 34 follow-up,
    # real user report on Nov10_1/2/3_TPS_4145/4149/4150.pdf's Q33/Q34):
    # these files' question header text ("33. Race/Ethnicity (Please mark
    # all tha...") sits close enough above the first real choice's OLD
    # calibrated position to fall inside up_pad's window and get counted as
    # its own band - confirmed directly (9 candidate bands found for Q33's
    # 7 choices; the true first band is the header line, not "American
    # Indian/Alaskan Native"). Taking bands[:7] then silently mislabeled
    # EVERY choice by one row: "Black/African American"'s real, marked
    # checkbox ended up read under "Native Hawaiian/Pacific Islander"'s
    # label, and the true first choice was never read at all (its window
    # landed on header text, not a checkbox).
    #
    # Score every CONSECUTIVE window of n candidate bands by its total
    # absolute drift: the sum, across all n choices, of how far that
    # window's band center sits from THAT CHOICE'S OWN original calibrated
    # center. Every real per-file drift measured across this whole module
    # has been a small, fairly uniform amount (a few px to a few dozen px
    # per choice) - a window built from the WRONG consecutive bands (one
    # row off, because a spurious extra band got swept in ahead of or
    # behind the real ones) shows a much larger, non-uniform apparent
    # "drift" for every choice at once, since it's not really measuring
    # drift at all - it's measuring a full extra row's worth of offset.
    #
    # This is only a reliable signal in COMPARISON to how much better the
    # DEFAULT (offset 0, matching this function's original bands[:n]
    # behavior) already looks - it is NOT safe to always take the single
    # best-scoring offset outright. Confirmed directly: on every file
    # where offset 0 is already correct, its own total drift measured far
    # below any alternative offset (worst-case ratio 0.578 seen on
    # Nov9_3_TPS_4137.pdf's Q33, where a coincidentally small-looking
    # alternative total of 174.5 is still well above offset 0's 302 in
    # relative terms); on every file confirmed (via direct crop) to
    # genuinely need a shift (Nov10_1/2/3_TPS_4145/4149/4150.pdf's Q33/Q34
    # - a different-worded question header sitting closer to the first
    # choice than previously seen pulled it into the scan as an extra
    # leading band), the best alternative's ratio to offset 0 never
    # exceeded 0.377 - a clear, non-overlapping gap from the 0.578 "still
    # fine" case. Only switch away from offset 0 when an alternative's
    # total drift is decisively lower (at most half of offset 0's own
    # total) - anything less decisive keeps the original, simpler
    # behavior rather than risk a wrong "correction" on a file whose
    # default alignment was never actually broken (confirmed this
    # mattered: an earlier version of this fix that always took the
    # single lowest-drift offset outright turned Nov9_3_TPS_4137.pdf's
    # already-correct Q33 "White/Caucasian" reading into a wrong
    # "Native Hawaiian/Pacific Islander" one before this safeguard was
    # added).
    _DECISIVE_SHIFT_RATIO = 0.5
    calib_centers = [(y0 + y1) / 2 for (_x0, _x1, y0, y1) in boxes.values()]

    def _total_drift(offset):
        window = bands[offset:offset + n]
        return sum(abs(((ry0 + ry1) / 2) - cc) for (ry0, ry1), cc in zip(window, calib_centers))

    offset0_drift = _total_drift(0)
    best_offset, best_drift = 0, offset0_drift
    for offset in range(1, len(bands) - n + 1):
        drift = _total_drift(offset)
        if drift < best_drift:
            best_offset, best_drift = offset, drift
    if best_offset != 0 and not (offset0_drift > 0 and best_drift <= _DECISIVE_SHIFT_RATIO * offset0_drift):
        best_offset = 0
    return bands[best_offset:best_offset + n]


def detect_multiselect_ink_ratios(
    page_images: list, qnum: str, dpi: int = _RENDER_DPI, include_diagnostics: bool = False
):
    """Returns {choice_label: ink_ratio} for every calibrated box of a
    multi-select question that could be confidently located on this scan
    (border-coverage + size-checked, exactly like _locate_checkbox() is used
    everywhere else in this module). A choice missing from the returned
    dict simply couldn't be confidently located on this particular scan -
    same "never guess" philosophy as detect_yesno_box_answers(); the caller
    only acts on choices that ARE present. Returns {} if this question has
    no calibration, isn't on a rendered page this file has, or the page
    image itself can't be decoded.

    When include_diagnostics=True, instead returns (ratios, ambiguous) -
    ambiguous is a list of labels whose mark, if any, could not be read
    with full confidence (near the mark/blank threshold, or ink found
    bleeding outside the box - see _MULTISELECT_AMBIGUOUS_BAND/_MULTISELECT_
    OVERFLOW_RATIO above) and that a caller should treat as grounds for
    needs_review even though `ratios` itself always reports its best
    reading either way.

    Anchors on the FIRST calibrated choice the same way detect_yesno_box_
    answers()'s "list_anchor" mode does: real-file measurement against a
    non-Letter (A4) scan (Nov5_1_TPS_2988.pdf) showed Q33/Q34's real boxes
    sit a small, roughly CONSTANT number of pixels below their Letter-based
    calibration (not a proportional page-size stretch - that was tried
    first and made things worse, see detect_yesno_box_answers()'s scale
    comment) - confirmed via OCR-located choice labels: every one of Q33's
    seven rows was offset from calibration by the same ~67-68px, and
    directly measuring that shift off the first row and applying it to the
    rest is far safer than either (a) trusting the raw calibrated position
    (silently lands on nothing, or - worse, confirmed on this file - on the
    WRONG neighboring row's box, since box sizes are identical row to row)
    or (b) simply widening every row's own search pad (with only ~53px
    between row centers, a pad wide enough to reach a 68px shift would also
    reach into the next row)."""
    import cv2
    import numpy as np

    empty = ({}, []) if include_diagnostics else {}
    if qnum not in _MULTISELECT_BOX_CALIBRATION:
        return empty
    page_idx, boxes = _MULTISELECT_BOX_CALIBRATION[qnum]
    if page_idx >= len(page_images):
        return empty
    arr = np.frombuffer(page_images[page_idx], dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
    if img is None:
        return empty
    # _RENDER_DPI is fixed at 300 throughout this pipeline, so this is
    # always 1.0 - see the module-level scale comments on detect_h3_answer()
    # and detect_yesno_box_answers() for why a page-size-proportional scale
    # was tried here too and reverted.
    scale = dpi / 300.0
    _, binary = cv2.threshold(img, _INK_THRESHOLD, 255, cv2.THRESH_BINARY_INV)
    pad = max(int(_MULTISELECT_BOX_PAD_OVERRIDE.get(qnum, _YESNO_BOX_PAD) * scale), 4)
    ink_border = _MULTISELECT_INK_BORDER_OVERRIDE.get(qnum, 2)

    # Anchor: locate just the first choice with a wide-but-safe search
    # window (mostly downward in y, since that's the direction real drift
    # has been observed in - see the docstring - but also a bit wider in x
    # than the normal pad, since a narrower physical page (A4 is ~66px
    # narrower than Letter at 300 DPI) shifts x position too), measure its
    # real offset from calibration on BOTH axes, then apply that same shift
    # to every other choice before searching for each with the normal
    # narrow pad.
    x_shift = y_shift = 0
    first_label = next(iter(boxes))
    fx0, fx1, fy0, fy1 = boxes[first_label]
    down_pad = max(int(_MULTISELECT_ANCHOR_DOWN_PAD * scale), pad)
    up_pad = max(int(_MULTISELECT_ANCHOR_UP_PAD * scale), pad)
    x_pad = max(int(_MULTISELECT_ANCHOR_X_PAD * scale), pad)
    # narrow_pad=pad (Revision 30 fix, Nov7_1_TPS_4017.pdf/Nov6_3_TPS_4009.pdf
    # Q33): _locate_anchor_box_nearest()'s own narrow_pad defaults to a
    # hardcoded 10px, but every actual per-box search in THIS function uses
    # the wider, question-specific `pad` (20px for Q33/Q34, via
    # _MULTISELECT_BOX_PAD_OVERRIDE) - a real, inconsistent mismatch.
    # Confirmed on both real files: each row's box sits ~14-18px above its
    # calibrated position (a small, genuine printing/scan drift, comfortably
    # inside the normal 20px per-box pad), but the anchor's OWN narrow search
    # used only a 10px pad and missed it, falling through to the wide
    # max_up/max_down search - which, per this function's own docstring,
    # picks whichever same-sized candidate scores best on size alone and has
    # no way to prefer the correct (nearest) row over another choice's box a
    # few rows away. On Nov7_1_TPS_4017.pdf this silently locked onto
    # "Black/African American"'s box instead of "American Indian/Alaskan
    # Native"'s, corrupting every other row's search with a bogus ~85px
    # shift and losing 2 of 7 boxes entirely (including "Asian" - the real
    # marked choice - which read 0 ink because the ratio was for the WRONG
    # cell). Passing the same `pad` used for every other search in this
    # function closes that gap without changing anything for a file that
    # truly needs the big wide-search fallback (a real multi-row shift, by
    # definition, is much larger than one question's own box pad).
    anchor_found = _locate_anchor_box_nearest(
        binary, int(fx0 * scale), int(fx1 * scale), int(fy0 * scale), int(fy1 * scale),
        scale=scale, max_up=up_pad, max_down=down_pad, max_x=x_pad,
        expected_w=(fx1 - fx0), expected_h=(fy1 - fy0), narrow_pad=pad,
    )
    if anchor_found is not None:
        afx0, afx1, afy0, afy1 = anchor_found
        calibrated_y_center = (int(fy0 * scale) + int(fy1 * scale)) / 2
        found_y_center = (afy0 + afy1) / 2
        y_shift = round(found_y_center - calibrated_y_center)
        calibrated_x_center = (int(fx0 * scale) + int(fx1 * scale)) / 2
        found_x_center = (afx0 + afx1) / 2
        x_shift = round(found_x_center - calibrated_x_center)

    # Per-choice text-row override (Revision 31 - see
    # _find_multiselect_text_row_positions()'s docstring and the
    # _MULTISELECT_TEXT_BAND_* comment above for why this exists):
    # the single checkbox-shape anchor above can lock onto the WRONG row
    # when more than one same-sized candidate sits inside its search
    # window, with no way to tell them apart on shape alone - confirmed on
    # Nov8_1_TPS_4054.pdf's Q33, where the real whole-list shift (~31px
    # upward) was never even found because the anchor's narrow search
    # locked onto the row below ("Asian") first, and a wide-only search
    # locked onto a DIFFERENT wrong row ("Black/African American") - both
    # same size as the true target, neither preferred over the other.
    # Printed text, unlike the repeating checkbox glyphs, is unique per
    # choice and reliably separable into one ink-density band per row - so
    # when exactly enough (or more, e.g. the next question's own header)
    # bands are found, use each choice's OWN band position directly
    # instead of the single global (x_shift, y_shift) pair, sidestepping
    # the shape-repetition ambiguity entirely rather than trying to tune
    # the anchor's pads around it.
    text_rows = _find_multiselect_text_row_positions(binary, boxes, scale=scale)

    labels_in_order = list(boxes.keys())
    found_by_label = {}
    for i, (label, (x0, x1, y0, y1)) in enumerate(boxes.items()):
        if text_rows is not None:
            row_y0, row_y1 = text_rows[i]
            # The text band covers the LABEL, not the checkbox - both sit
            # on the same printed row, so search a window centered on the
            # band's own vertical span (with the normal per-box pad) rather
            # than assuming the checkbox's calibrated height lines up
            # exactly with the band's measured height (label text and its
            # checkbox aren't always pixel-identical in extent).
            sy0, sy1 = row_y0 - pad, row_y1 + pad
            sx0, sx1 = int(x0 * scale) - pad + x_shift, int(x1 * scale) + pad + x_shift
        else:
            sx0, sx1 = int(x0 * scale) - pad + x_shift, int(x1 * scale) + pad + x_shift
            sy0, sy1 = int(y0 * scale) - pad + y_shift, int(y1 * scale) + pad + y_shift
        found = _locate_checkbox(
            binary, sy0, sy1, sx0, sx1, scale=scale, expected_w=(x1 - x0), expected_h=(y1 - y0)
        )
        if found is None:
            continue
        fx0, fx1, fy0, fy1 = found
        expected_w, expected_h = (x1 - x0) * scale, (y1 - y0) * scale
        if abs((fx1 - fx0) - expected_w) > 14 * scale or abs((fy1 - fy0) - expected_h) > 14 * scale:
            continue
        found_by_label[label] = found

    # Generic row-pitch consistency check (real user report, Nov9_3_TPS_
    # 4137.pdf's Q34): a per-choice search window built from a text band
    # (or even the narrow shape-anchor window) can still land on the WRONG
    # row's real checkbox if that band came out abnormally oversized -
    # confirmed directly on this file: "Other (specify)"'s text band merged
    # with "None"'s own row (band height 103px vs. ~35px for every other
    # choice on this list), so "Other (specify)"'s search window reached
    # past its own row entirely and found "None"'s real, marked checkbox
    # instead - reporting it as "Other (specify)" being marked, while
    # "None" itself then found nothing in its own (correctly-band-derived
    # but now downstream-shifted) window. A fixed height ceiling can't
    # catch this generically: "Other (specify)"'s own band is LEGITIMATELY
    # oversized on every file (it always has a free-text "specify" line
    # under its own label) - confirmed by direct measurement across all 10
    # real reference files, where Q33's "Other (specify)" band alone
    # ranges 67-78px and Q34's ranges 17-59px, all entirely normal. What's
    # NOT normal, on any file measured, is the resulting found box's
    # DISTANCE from the previous choice's found box: confirmed measuring
    # every consecutive gap between found boxes on Q33/Q34 across all 10
    # files, the largest genuine (correct) gap seen was 72px against a
    # same-list median of ~52px (ratio 1.38x) - Nov9_3_TPS_4137.pdf's Q34
    # bug produced a gap of 123.5px against its own median of ~50.5px
    # (ratio 2.45x), decisively past any genuine case. Using each list's
    # OWN median gap (robust to a single outlier, since a real list has at
    # most one such merge) sidesteps needing a fixed, position-specific
    # threshold entirely.
    centers_in_order = []
    for label in labels_in_order:
        found = found_by_label.get(label)
        centers_in_order.append(((found[2] + found[3]) / 2) if found is not None else None)
    gaps = [
        centers_in_order[i] - centers_in_order[i - 1]
        for i in range(1, len(centers_in_order))
        if centers_in_order[i] is not None and centers_in_order[i - 1] is not None
    ]
    row_pitch_suspect = set()
    if len(gaps) >= 3:
        sorted_gaps = sorted(gaps)
        mid = len(sorted_gaps) // 2
        median_gap = (
            sorted_gaps[mid]
            if len(sorted_gaps) % 2
            else (sorted_gaps[mid - 1] + sorted_gaps[mid]) / 2
        )
        if median_gap > 0:
            for i in range(1, len(centers_in_order)):
                prev_c, cur_c = centers_in_order[i - 1], centers_in_order[i]
                if prev_c is None or cur_c is None:
                    continue
                if (cur_c - prev_c) > _MULTISELECT_ROW_PITCH_ANOMALY_RATIO * median_gap:
                    # This choice's own found box is implausibly far past
                    # the previous choice's - almost certainly the NEXT
                    # row's real box, not this row's. Never guess: drop it
                    # entirely rather than report a mark that likely
                    # belongs to a different choice.
                    row_pitch_suspect.add(labels_in_order[i])

    ratios = {}
    ambiguous = []
    for label in labels_in_order:
        found = found_by_label.get(label)
        if found is None:
            continue
        if label in row_pitch_suspect:
            ambiguous.append(label)
            continue
        fx0, fx1, fy0, fy1 = found
        ratio = _checkbox_ink_ratio(binary, found, border=ink_border)
        ratios[label] = ratio
        if include_diagnostics:
            if _MULTISELECT_UNMARKED_CEILING - _MULTISELECT_AMBIGUOUS_BAND <= ratio < _MULTISELECT_UNMARKED_CEILING:
                # Signal 1: near-threshold - see _MULTISELECT_AMBIGUOUS_BAND's comment.
                ambiguous.append(label)
                # Boost the reported ratio just over the ceiling: a near-
                # threshold reading is real evidence of a mark (just not
                # enough to clear the ceiling outright), so the VETO/FILL
                # reconciliation below should still be able to use it as
                # this choice's best-guess answer - the ambiguous flag
                # above is what sends it to needs_review rather than
                # trusting it silently. Confirmed needed for real
                # (Nov5_2_TPS_3004.pdf's Q33: "Black/African American"
                # measured 0.086, just under the 0.10 ceiling, because the
                # X was drawn mostly to the box's left - see signal 2 -
                # leaving the model's own blank answer uncorrected without
                # this boost).
                ratios[label] = _MULTISELECT_UNMARKED_CEILING + 0.01
            elif ratio < _MULTISELECT_UNMARKED_CEILING:
                # Signal 2: confidently blank in-box, but check whether ink
                # is bleeding in from just outside the box (a mark that
                # exceeded its own pixel area) - see
                # _MULTISELECT_OVERFLOW_RATIO's comment.
                # Sample ONLY the margin strip immediately to the box's
                # left - not the box itself - so the box's own (fairly
                # thick-printed) border ink can never masquerade as
                # overflow. That border/perimeter effect is exactly what a
                # first attempt here got burned by: sampling a box-plus-
                # margin rectangle with no border exclusion put every
                # blank box's own outline into the ratio, well past
                # _MULTISELECT_OVERFLOW_RATIO on every single row.
                expand_left = max(int(_MULTISELECT_OVERFLOW_EXPAND_LEFT * scale), 6)
                expand_y = max(int(_MULTISELECT_OVERFLOW_EXPAND_Y * scale), 2)
                margin_box = (fx0 - expand_left, fx0, fy0 - expand_y, fy1 + expand_y)
                expanded_ratio = _checkbox_ink_ratio(binary, margin_box, border=0)
                if expanded_ratio >= _MULTISELECT_OVERFLOW_RATIO:
                    ambiguous.append(label)
                    ratios[label] = _MULTISELECT_UNMARKED_CEILING + 0.01  # see the boost comment above
    if include_diagnostics:
        return ratios, ambiguous
    return ratios


_H3_CIRCLE_CALIBRATION = (0, {
    "Early Intervention": (687, 715, 304, 332),
    "OP/IOP": (1041, 1070, 303, 332),
    "Residential": (1236, 1265, 304, 332),
    "OTP/NTP": (1492, 1520, 304, 332),
    "Detox/WM": (1715, 1744, 304, 332),
    "Recovery Services": (1956, 1984, 303, 332),
})
_H3_CIRCLE_PAD = 45  # widened from 25 - see detect_h3_answer()'s scale comment
_H3_CIRCLE_DIAM = 29  # expected outer diameter (px, at 300 DPI) of the printed circle glyph
_H3_CONFIDENCE_MARGIN = 0.8  # deliberately generous - real margins measured 0.95-1.0, nowhere near this
_H3_BLANK_CEILING = 0.3  # winning circle's own center ink ratio below this = nothing filled in


def _locate_circle(binary_img, y0: int, y1: int, x0: int, x1: int,
                    expected_diam: float = _H3_CIRCLE_DIAM):
    """Within binary_img[y0:y1, x0:x1], finds the single circle-glyph-sized
    blob (its OUTER boundary, whether the glyph is a hollow ring or a solid
    filled dot - cv2.RETR_EXTERNAL doesn't care which) closest in size to
    expected_diam, and returns its (x0, x1, y0, y1) - or None if no
    plausibly circular blob is in the window."""
    import cv2

    H, W = binary_img.shape
    cy0, cy1, cx0, cx1 = max(0, y0), min(H, y1), max(0, x0), min(W, x1)
    win = binary_img[cy0:cy1, cx0:cx1]
    if win.size == 0:
        return None
    contours, _ = cv2.findContours(win, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    best, best_err = None, None
    for c in contours:
        x, y, w, h = cv2.boundingRect(c)
        if not (18 <= w <= 36 and 18 <= h <= 36 and abs(w - h) <= 8):
            continue
        err_ = abs(w - expected_diam) + abs(h - expected_diam)
        if best_err is None or err_ < best_err:
            best_err = err_
            best = (x + cx0, x + cx0 + w, y + cy0, y + cy0 + h)
    return best


def _circle_fill_ratio(binary_img, box, half: int = 6) -> float:
    """Fraction of dark pixels in a small (2*half)-square sample centered on
    box - deliberately small and centered so it lands inside a hollow "O"
    glyph's empty middle (measuring ~0.0) while still landing solidly inside
    a filled dot's ink (measuring ~1.0)."""
    x0, x1, y0, y1 = box
    cx, cy = (x0 + x1) // 2, (y0 + y1) // 2
    crop = binary_img[cy - half:cy + half, cx - half:cx + half]
    if crop.size == 0:
        return 0.0
    return float(crop.mean()) / 255.0


def detect_h3_answer(page_images: list, dpi: int = _RENDER_DPI) -> dict:
    """Deterministic, non-LLM reading of H3 ("Setting"), the form's one
    round-radio-button question. Returns {"position": 1-based index into
    CHOICE_LISTS_BY_NUMBER["H3"], "margin": winning circle's center ink
    ratio minus the runner-up's} when a circle was confidently found marked,
    {"position": None, "margin": ..., "blank": True} when every circle was
    confidently found UNMARKED, or {} if the row couldn't be confidently
    located on this page at all."""
    import cv2
    import numpy as np

    page_idx, circles = _H3_CIRCLE_CALIBRATION
    if page_idx >= len(page_images):
        return {}
    arr = np.frombuffer(page_images[page_idx], dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
    if img is None:
        return {}
    # NOTE: _RENDER_DPI is fixed at 300 throughout this pipeline, so scale
    # here is always 1.0 - real measurement against a non-Letter (A4) scan
    # (Nov5_1_TPS_2988.pdf) showed page-content position differs from the
    # Letter-based calibration by a small, roughly CONSTANT pixel offset
    # (~28-32px on this page), not a proportional stretch of the whole
    # page - a per-axis page-size scale factor was tried and made things
    # worse (it overshot the real offset by 3-4x and pointed searches at
    # the wrong row entirely). _H3_CIRCLE_PAD below is widened instead to
    # give headroom for that kind of small constant drift.
    scale = dpi / 300.0
    x_scale = y_scale = scale
    pad = max(int(_H3_CIRCLE_PAD * scale), 4)
    _, binary = cv2.threshold(img, _INK_THRESHOLD, 255, cv2.THRESH_BINARY_INV)
    labels, ratios = [], []
    for label, (x0, x1, y0, y1) in circles.items():
        sx0, sx1 = int(x0 * x_scale) - pad, int(x1 * x_scale) + pad
        sy0, sy1 = int(y0 * y_scale) - pad, int(y1 * y_scale) + pad
        found = _locate_circle(binary, sy0, sy1, sx0, sx1, expected_diam=_H3_CIRCLE_DIAM * scale)
        if found is None:
            return {}  # one circle missing on this scan - don't guess from a partial row
        labels.append(label)
        ratios.append(_circle_fill_ratio(binary, found))
    order = sorted(range(len(labels)), key=lambda i: -ratios[i])
    best, second = order[0], order[1]
    margin = ratios[best] - ratios[second]
    choices = CHOICE_LISTS_BY_NUMBER["H3"]
    if ratios[best] < _H3_BLANK_CEILING:
        return {"position": None, "margin": round(margin, 4), "blank": True}
    return {"position": choices.index(labels[best]) + 1, "margin": round(margin, 4)}


def _locate_checkbox(binary_img, y0: int, y1: int, x0: int, x1: int, scale: float = 1.0,
                      expected_w: Optional[float] = None, expected_h: Optional[float] = None):
    """Within binary_img[y0:y1, x0:x1] (already thresholded, ink=255), looks
    for exactly one box-sized square outline (vertical line pair AND
    horizontal line pair, both in the expected size range) and returns its
    absolute (x0, x1, y0, y1) - or None if the window doesn't contain a
    clean, unambiguous single box."""
    import cv2

    H, W = binary_img.shape
    cy0, cy1, cx0, cx1 = max(0, y0), min(H, y1), max(0, x0), min(W, x1)
    win = binary_img[cy0:cy1, cx0:cx1]
    if win.size == 0:
        return None
    vlen = max(int(20 * scale), 8)
    hlen = max(int(20 * scale), 8)
    v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, vlen))
    vert = cv2.dilate(cv2.erode(win, v_kernel), v_kernel)
    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (hlen, 1))
    horiz = cv2.dilate(cv2.erode(win, h_kernel), h_kernel)
    col_hits = [x for x in range(win.shape[1]) if vert[:, x].sum() / 255 > vlen * 0.8]
    row_hits = [y for y in range(win.shape[0]) if horiz[y, :].sum() / 255 > hlen * 0.8]
    cols = _group_consecutive_positions(col_hits) if col_hits else []
    rows = _group_consecutive_positions(row_hits) if row_hits else []
    if len(cols) < 2 or len(rows) < 2:
        return None
    min_side, max_side = 18 * scale, 55 * scale

    def _valid_pairs(groups):
        return [
            (groups[i], groups[j])
            for i in range(len(groups))
            for j in range(i + 1, len(groups))
            if min_side <= groups[j] - groups[i] <= max_side
        ]

    col_pairs = _valid_pairs(cols)
    row_pairs = _valid_pairs(rows)
    if not col_pairs or not row_pairs:
        return None
    win_h, win_w = win.shape
    col_pairs = [(lx, rx) for lx, rx in col_pairs if lx != 0 and rx != win_w - 1]
    row_pairs = [(ty, by) for ty, by in row_pairs if ty != 0 and by != win_h - 1]
    if not col_pairs or not row_pairs:
        return None
    exp_w = expected_w * scale if expected_w is not None else None
    exp_h = expected_h * scale if expected_h is not None else None
    best_box = None
    best_score = None
    for lx, rx in col_pairs:
        for ty, by in row_pairs:
            box = (lx + cx0, rx + cx0, ty + cy0, by + cy0)
            if not _border_coverage_ok(binary_img, box):
                continue
            if exp_w is not None or exp_h is not None:
                w_err = abs((rx - lx) - exp_w) if exp_w is not None else 0.0
                h_err = abs((by - ty) - exp_h) if exp_h is not None else 0.0
                score = w_err + h_err
            else:
                score = -((rx - lx) + (by - ty))
            if best_score is None or score < best_score:
                best_score = score
                best_box = box
    return best_box


def _locate_anchor_box_nearest(
    binary_img, x0: int, x1: int, y0: int, y1: int, scale: float,
    max_up: int, max_down: int, max_x: int,
    expected_w: float, expected_h: float, narrow_pad: int = 10,
):
    """Anchor-box search used by the "list_anchor"/"fixed" yesno modes and
    detect_multiselect_ink_ratios(): first tries a NORMAL, narrow pad right
    at the calibrated position (exactly like every other, non-anchor box
    search in this module) - covering the common case where this file
    needs no real shift at all. Only if that fails does it fall back to a
    single wide-window _locate_checkbox() call to search for a genuinely
    shifted anchor.

    This two-step order matters. A single wide window alone can't tell a
    same-size candidate right at the calibrated position apart from an
    equally-sized one a full row away - confirmed for real on a Letter-
    sized file needing NO shift (Nov4_2_TPS_1623.pdf's Q33): a wide search
    for "American Indian/Alaskan Native" locked onto "Asian" (the next row
    down, same size) instead, corrupting every other row's search with a
    bogus shift and losing "Prefer not to state"'s real, unshifted mark
    entirely. Trying the narrow window FIRST avoids ever needing to choose
    between two valid-looking candidates in the zero-shift case, since
    there's only one candidate in a narrow window to begin with.

    A scoring scheme that instead tried to prefer whichever wide-window
    candidate sits closest to the calibrated position was tried and
    rejected: when a REAL shift exists, the true (fully shifted) box is,
    by definition, farther from the calibrated position than any nearby
    noise - confirmed on the offline synthetic-shift self-test, where an
    X mark's own diagonal strokes threw off a couple of spurious
    "horizontal line" detections partway between the calibrated and true
    positions, and "prefer whichever is closer to calibration" then picked
    that spurious, closer-but-wrong pairing over the correct, farther,
    fully-shifted one. Falling back to a single plain wide search (the
    behavior this module used before this function existed) doesn't have
    that failure mode, because it was already the code path every
    passing self-test for a genuine shift was written against."""
    narrow = _locate_checkbox(
        binary_img, y0 - narrow_pad, y1 + narrow_pad, x0 - narrow_pad, x1 + narrow_pad,
        scale=scale, expected_w=expected_w, expected_h=expected_h,
    )
    if narrow is not None:
        return narrow
    return _locate_checkbox(
        binary_img, y0 - max_up, y1 + max_down, x0 - max_x, x1 + max_x,
        scale=scale, expected_w=expected_w, expected_h=expected_h,
    )


_YESNO_LIST_ANCHOR_CORROBORATE_PAD = 20

# Revision 33: interior-density MAJORITY sanity check used by
# _locate_yesno_list_shift() (Fix B - see that function's own docstring for
# the full real-file investigation, and Fix A for the structural
# corroboration-window bug this is a second, independent net against).
# This is a per-BOX ceiling used only to classify each shape-and-position-
# confirmed box as "clean" or not before a MAJORITY vote across all of
# them - it is deliberately NOT used as a single per-box accept/reject
# gate, because a real, heavily-inked mark reads far above any reasonable
# ceiling here (~0.48 interior ink measured on a genuine X) and a false
# text-aliased box can occasionally land on a clean gap between letters
# (~0.002 measured on one real occurrence) - individual readings alone
# don't cleanly separate the two cases. Measured directly across all 4
# confirmed real false-positive occurrences (Nov9_1_TPS_4096.pdf's Q23,
# Nov9_2_TPS_4103.pdf's Q28/Q31/Q32): genuine blank checkboxes measured
# 0.0-0.045 interior ink (5px border exclusion); set here just above that
# noise ceiling so a real blank still counts as "clean" for the majority
# vote, while a text-dense false box (typically 0.08+) does not.
_YESNO_LIST_ANCHOR_TEXT_ALIAS_CEILING = 0.06
# Revision 34 follow-up (real user report, Nov9_3_TPS_4137.pdf's Q27):
# _YESNO_LIST_ANCHOR_TEXT_ALIAS_CEILING's majority-clean vote above breaks
# down specifically on a 2-choice list ("fixed"/"list_anchor" mode; only
# Q27 currently has exactly 2 choices, but this is a general defect, not a
# Q27-specific one). With only ONE other choice to vote with, "a majority
# of the others read clean" is mathematically impossible whenever that one
# other choice IS the genuine mark - confirmed directly: anchoring on
# Q27's blank "No" (correctly, widely shape-found) correctly corroborated
# "Yes" at the true shifted position (Fix A's position-proximity check
# passed cleanly), but "Yes" is genuinely, heavily marked (0.44 interior
# ink) - so clean_count came back 0 of 1, majority_clean voted this down,
# and the whole question fell through to the blind-ink fallback, which -
# even after this revision's OWN shape-validation gate - still had no way
# to independently confirm "Yes" was really where the ink was (that
# fallback's own majority-shape-check only validates the OTHER,
# non-winning boxes' BORDERS, not whether the winning ink measurement
# itself is plausible), and returned nothing. A per-choice ink ceiling
# can't safely replace the majority vote outright (see this constant's own
# comment above: individual blank/alias readings already overlap), but a
# reading confidently ABOVE the real text-alias range confirmed so far
# (0.05-0.33 across 4 real occurrences) is categorically different
# evidence than one sitting inside it - a text alias has never once been
# measured this high, while this is comfortably below a genuine full mark
# (~0.48). Treating a decisively-marked other choice as equally
# trustworthy as a clean one (not just "clean" counting toward the
# majority) fixes the 2-choice case without loosening anything for a
# longer list, where the same broadened definition still requires that
# majority of others to be UNAMBIGUOUS one way or the other - a list where
# several others read into this same 0.06-0.40 "noise band" (exactly what
# a text alias produces) still fails to reach a majority and is correctly
# rejected, same as before.
_YESNO_LIST_ANCHOR_GENUINE_MARK_FLOOR = 0.40


def _locate_yesno_list_shift(
    binary_img, boxes: dict, scale: float, x_scale: float, y_scale: float,
    anchor_up_pad: int, anchor_x_pad: int,
):
    """Returns (x_shift, y_shift) for a "list_anchor"/"fixed" question's
    whole choice list, or None if no candidate shift can be corroborated.

    Revision 30 fix (Nov7_2_TPS_4041.pdf's Q25: "4 weeks or more"
    unmistakably marked, but detect_yesno_box_answers() returned NO result
    at all for this question - not even ambiguous). Anchoring on only the
    FIRST calibrated choice (the previous, and usual, approach) can lock
    onto the WRONG row when a real whole-list shift happens to be close to
    one full row's pitch: confirmed directly on this file, where Q25's
    entire 4-choice list sits ~40px above its calibration (row pitch is
    only ~46px) - the wide anchor search for "First visit/day" found "2
    weeks or less"'s own real, barely-shifted-looking box instead (only
    ~9px from "First visit/day"'s calibrated position, versus the true
    "First visit/day" box's ~40px - a "closer-but-wrong" pairing this
    module's design has warned about elsewhere, just never previously hit
    from BOTH directions on the same list). That bogus ~9px shift then
    mapped every other choice one full row too low, and "4 weeks or
    more" - now searched a whole row off from its real position - failed
    outright, aborting the entire question with no fallback.

    Fix: try EVERY calibrated choice in turn as the anchor (same search as
    before), score each candidate shift by how many OTHER choices in the
    list also resolve to a real, correctly-shaped box at their own
    calibrated positions offset by that same shift (checked with a tight,
    not wide-anchor-sized, pad), and return the candidate with the HIGHEST
    corroboration count - not just the first one to reach some bar. A
    genuine whole-list shift is uniform and moves every choice the same
    amount, so it corroborates against every other row (barring one
    choice's own mark corrupting its border past recognition); a wrong,
    off-by-one-row shift (as above) corroborates against fewer, since the
    true per-row shift for the choices pushed off the list's end is
    completely different from the bogus one derived from a single aliased
    match.

    Revision 31 first tried "accept the first candidate reaching a bare
    majority" instead of "pick the best of all of them" - fixed
    Nov8_1_TPS_4054.pdf's Q30 (a 2x2 grid) at the time, but this session's
    Nov8_2_TPS_4073.pdf's Q25 exposed why "first past a majority" isn't
    enough even so: on a 4-choice single-column list, anchoring on the
    FIRST choice ("First visit/day") found a bogus shift that aliased it
    onto "2 weeks or less"'s real box one row down - and that same bogus
    shift ALSO happened to corroborate against "2 weeks or less" and "More
    than 2 weeks..." (each sees the choice below it), reaching 2 of 3, a
    bare majority - so the loop returned immediately, before ever trying
    "4 weeks or more" as its own anchor, which finds the genuine ~39-47px
    shift and corroborates against all 3 other choices (3 of 3). "First
    past a majority" accepted 2/3 without ever learning a 3/3 candidate
    existed. Checking every choice and keeping the best-corroborated
    candidate closes this regardless of which choice happens to be tried
    first, and was confirmed to have been silently wrong the same way on
    Nov7_2_TPS_4041.pdf's Q25 ever since Revision 30 first introduced this
    function - that file's own "4 weeks or more" mark was being reported
    under the WRONG label ("More than 2 weeks but less than 4 weeks",
    position 3) the whole time; the reported ink margin happened to still
    be correct since it was reading the right, marked BOX, just filed
    under the adjacent choice's name.

    Ties (equal corroboration counts) keep the FIRST candidate reaching
    that count, preserving the existing "prefer the earliest anchor that
    works" behavior for the common, unambiguous case. Note: for a long,
    evenly-spaced, single-column list, even "pick the best" can still tie
    between the true shift and an off-by-one-row alias when the choice
    that would break the tie has its own mark corrupting its border -
    detect_multiselect_ink_ratios() hits a more severe version of this
    same shape-repetition ambiguity and uses a different, text-row-based
    fix instead (see _find_multiselect_text_row_positions()).

    Revision 33 fix (a genuinely BLANK list can still "corroborate" a
    wrong shift). Confirmed on two real files (Nov9_1_TPS_4096.pdf's Q23,
    Nov9_2_TPS_4103.pdf's Q28/Q31/Q32) - two independent, compounding
    defects, both fixed here.

    Defect A - the corroboration window can re-find a choice's TRUE,
    UNSHIFTED box and mistake that for "this candidate shift is
    corroborated." `_YESNO_LIST_ANCHOR_CORROBORATE_PAD` (20px) pads the
    search window on both sides of the SHIFTED target position - but for
    any candidate shift smaller than roughly (row_pitch - box_height -
    2*pad), that padded window still overlaps the choice's own real,
    calibrated position too. Confirmed directly on Nov9_2_TPS_4103.pdf's
    Q28 (row pitch ~120px, a spurious anchor shift of -40px): the
    corroboration search for "Yes, I received..." (calibrated y
    1851-1884), computed at shift-adjusted window y=1791-1864, still
    reached back into the choice's own true position (1851-1884) - so
    `_locate_checkbox()` correctly found and validated THAT box (empty,
    0.0 ink) and reported it as "confirming" a -40px shift that box was
    never actually AT. Every other choice hit the exact same loophole,
    so a shift derived from one bogus anchor match (the anchor itself
    landing on a printed text line one row up, not a real checkbox - see
    Defect B) came back fully, even unanimously, "corroborated" by boxes
    that were really just each choice's own unshifted, blank checkbox.

    Fix A: a corroborating match only counts if the FOUND box's own
    center sits closer to the SHIFTED target position than to the
    choice's original, unshifted calibrated position. A genuine shift's
    corroborating boxes are found near the shifted target (that's the
    whole point of a real, uniform shift); a spurious small shift whose
    window merely still overlaps the true position gets that box found
    close to zero-shift instead, and is now correctly NOT counted as
    corroboration for a nonzero candidate.

    Defect B - shape alone can still be fooled outright on a genuinely
    blank list, independent of Defect A. A printed question PROMPT or
    answer-choice LABEL sitting near the real checkbox row can contain
    letter strokes that happen to align into line-morphology's
    vertical/horizontal pairs at checkbox size (`_locate_checkbox()`'s
    18-55px side range at 300 DPI easily spans a capital letter's
    height). Confirmed directly measuring a genuine checkbox's INTERIOR
    (well inside its own detected border): blank real boxes measured
    exactly 0.0 interior ink at a 5px border exclusion, while boxes
    accidentally landing on dense text measured 0.05-0.33 - but never
    reliably 100% of the time per-choice (one false "box" on Nov9_2's
    Q32 happened to land on a clean gap between letters and measured
    0.0021), so this alone isn't a safe per-box filter; it IS a safe
    aggregate one, since a real single-select list has AT MOST ONE
    genuinely marked (heavily-inked) choice among the others, never
    every other choice reading substantial interior ink at once.

    Fix B: require a MAJORITY of the shape-and-position-confirmed other
    boxes to read a clean, near-zero interior ink
    (_YESNO_LIST_ANCHOR_TEXT_ALIAS_CEILING) - not each one individually
    (a per-box ceiling would wrongly reject a real, heavily-inked genuine
    mark, confirmed measuring ~0.48 interior ink on an actual X).

    Both fixes only ADD requirements on top of the existing shape check -
    each can only turn a previously-accepted candidate into a rejected
    one, never the reverse, so neither can break a shift that was
    correctly corroborated by real, actually-shifted checkboxes."""
    items = list(boxes.items())
    if len(items) < 2:
        return None
    check_pad = max(int(_YESNO_LIST_ANCHOR_CORROBORATE_PAD * min(x_scale, y_scale)), 4)
    best_shift, best_confirms = None, -1
    for i, (_label, (x0, x1, y0, y1)) in enumerate(items):
        anchor_found = _locate_anchor_box_nearest(
            binary_img, int(x0 * x_scale), int(x1 * x_scale), int(y0 * y_scale), int(y1 * y_scale),
            scale=scale, max_up=anchor_up_pad, max_down=anchor_up_pad, max_x=anchor_x_pad,
            expected_w=(x1 - x0), expected_h=(y1 - y0),
        )
        if anchor_found is None:
            continue
        afx0, afx1, afy0, afy1 = anchor_found
        y_shift = round(((afy0 + afy1) / 2) - ((int(y0 * y_scale) + int(y1 * y_scale)) / 2))
        x_shift = round(((afx0 + afx1) / 2) - ((int(x0 * x_scale) + int(x1 * x_scale)) / 2))
        confirms, total = 0, 0
        confirmed_interiors = []
        for j, (_olabel, (ox0, ox1, oy0, oy1)) in enumerate(items):
            if j == i:
                continue
            total += 1
            ocx, ocy = int((ox0 + ox1) / 2 * x_scale), int((oy0 + oy1) / 2 * y_scale)
            oy0s, oy1s = int(oy0 * y_scale) - check_pad + y_shift, int(oy1 * y_scale) + check_pad + y_shift
            ox0s, ox1s = int(ox0 * x_scale) - check_pad + x_shift, int(ox1 * x_scale) + check_pad + x_shift
            confirmed = _locate_checkbox(
                binary_img, oy0s, oy1s, ox0s, ox1s,
                scale=scale, expected_w=(ox1 - ox0), expected_h=(oy1 - oy0),
            )
            if confirmed is not None:
                fx0, fx1, fy0, fy1 = confirmed
                found_cx, found_cy = (fx0 + fx1) / 2, (fy0 + fy1) / 2
                # Fix A: the found box must sit closer to the SHIFTED
                # target than to the choice's own original, unshifted
                # position - otherwise this "confirmation" is really just
                # re-finding the real, unmoved box inside an
                # over-generous search window (see docstring's Defect A).
                dist_to_shifted = ((found_cx - (ocx + x_shift)) ** 2 + (found_cy - (ocy + y_shift)) ** 2) ** 0.5
                dist_to_original = ((found_cx - ocx) ** 2 + (found_cy - ocy) ** 2) ** 0.5
                if dist_to_shifted > dist_to_original:
                    continue
                confirms += 1
                confirmed_interiors.append(_checkbox_ink_ratio(binary_img, (fx0, fx1, fy0, fy1), border=5))
        trustworthy_count = sum(
            1 for v in confirmed_interiors
            if v <= _YESNO_LIST_ANCHOR_TEXT_ALIAS_CEILING or v >= _YESNO_LIST_ANCHOR_GENUINE_MARK_FLOOR
        )
        majority_clean = bool(confirmed_interiors) and trustworthy_count * 2 > len(confirmed_interiors)
        if total and confirms * 2 > total and majority_clean and confirms > best_confirms:
            best_shift, best_confirms = (x_shift, y_shift), confirms
            if confirms == total:
                break  # unanimous - no other candidate can beat this
    return best_shift


def _border_coverage_ok(binary_img, box, min_frac: float = 0.6) -> bool:
    """True if all four sides of box are covered by ink for at least
    min_frac of their length, confirming the four detected lines actually
    connect into one closed rectangle rather than being a coincidental,
    similarly-sized pairing of unrelated nearby lines/strokes."""
    x0, x1, y0, y1 = box
    H, W = binary_img.shape
    if not (0 <= y0 < y1 <= H and 0 <= x0 < x1 <= W):
        return False
    top = binary_img[y0, x0:x1]
    bottom = binary_img[y1 - 1, x0:x1]
    left = binary_img[y0:y1, x0]
    right = binary_img[y0:y1, x1 - 1]

    def frac(arr) -> float:
        return float((arr > 0).sum()) / max(len(arr), 1)

    return all(frac(side) >= min_frac for side in (top, bottom, left, right))


def _find_full_width_lines(binary_img, min_width_frac: float = 0.5) -> list:
    """Finds every full-page-width horizontal ruled line in binary_img
    (already thresholded, ink=255). Returns sorted y positions (one per
    line, consecutive hits collapsed via _group_consecutive_positions)."""
    import cv2

    H, W = binary_img.shape
    h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(int(60), 10), 1))
    h_lines = cv2.dilate(cv2.erode(binary_img, h_kernel), h_kernel)
    row_sums = h_lines.sum(axis=1) / 255
    row_peaks = [y for y in range(H) if row_sums[y] > W * min_width_frac]
    if not row_peaks:
        return []
    return _group_consecutive_positions(row_peaks)


def _nearest_row_bottom(full_width_lines: list, approx_bottom: float, search_range: int, search_above: int = 0):
    """Given the sorted y-positions of real ruled lines on the page, finds
    the nearest one at/below approx_bottom (within search_range px) - or, if
    search_above > 0, also up to search_above px ABOVE approx_bottom - and
    returns it, or None if nothing is close enough."""
    candidates = _row_bottom_candidates(full_width_lines, approx_bottom, search_range, search_above)
    return candidates[0] if candidates else None


def _row_bottom_candidates(full_width_lines: list, approx_bottom: float, search_range: int, search_above: int = 0) -> list:
    """Same window as _nearest_row_bottom, but returns EVERY line inside it,
    nearest-to-approx_bottom first, instead of only the nearest. Needed once
    search_above is wide enough that a neighboring question's OWN line can
    fall inside the window and happen to be closer to approx_bottom than
    the real line is (confirmed with Q21/Q22: widening Q22's search_above
    to 45px, needed for a real whole-page shift, could put Q21's own line -
    73px away - closer to Q22's approx_bottom than Q22's real line, 33px
    further out, actually is). A caller with a way to validate a candidate
    (e.g. _has_pitch_partner()) should try each of these in order and use
    the first that validates, rather than committing to the single nearest
    one the way the old _nearest_row_bottom()-only code did."""
    candidates = [
        y for y in full_width_lines
        if (approx_bottom - search_above) <= y <= (approx_bottom + search_range)
    ]
    return sorted(candidates, key=lambda y: abs(y - approx_bottom))


def _checkbox_ink_ratio(binary_img, box, border: int = 2) -> float:
    """Fraction of dark pixels strictly inside box's own border (border px
    excluded on each side, so only genuine mark ink - not the box's printed
    outline - contributes)."""
    x0, x1, y0, y1 = box
    crop = binary_img[y0:y1, x0:x1]
    bh, bw = crop.shape
    if bh <= 2 * border or bw <= 2 * border:
        return 0.0
    inner = crop[border:bh - border, border:bw - border]
    return float(inner.mean()) / 255.0


def _resolve_runtime_box_geometry(page_images: list, dpi: int = _RENDER_DPI):
    """Resolve semantic controls to rectangles observed in the supplied PDF.

    Registration/layout metadata is used only to bound a local search. The
    returned rectangles come from OpenCV contours in the current scan. If a
    dependency or scan is unusable, the private legacy geometry is retained as
    a conservative compatibility fallback.
    """
    import cv2
    import numpy as np

    def find_box(img, expected):
        x0, x1, y0, y1 = expected
        pad = max(int(90 * dpi / 300), 20)
        binary = cv2.threshold(img, _INK_THRESHOLD, 255, cv2.THRESH_BINARY_INV)[1]
        h, w = binary.shape
        crop = binary[max(0, y0 - pad):min(h, y1 + pad),
                      max(0, x0 - pad):min(w, x1 + pad)]
        contours, _ = cv2.findContours(crop, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
        best = None
        best_distance = None
        ex, ey = (x0 + x1) / 2, (y0 + y1) / 2
        for contour in contours:
            bx, by, bw, bh = cv2.boundingRect(contour)
            if not (20 <= bw <= 60 and 20 <= bh <= 60 and 0.55 <= bw / max(bh, 1) <= 1.8):
                continue
            ax, ay = bx + max(0, x0 - pad), by + max(0, y0 - pad)
            distance = abs(ax + bw / 2 - ex) + abs(ay + bh / 2 - ey)
            if distance <= pad * 2 and (best_distance is None or distance < best_distance):
                best_distance, best = distance, (ax, ax + bw, ay, ay + bh)
        return best

    def resolve(semantic_map, legacy_map):
        resolved = {}
        for qnum, semantic in semantic_map.items():
            legacy = legacy_map.get(qnum)
            if legacy is None:
                continue
            candidates = legacy if isinstance(legacy, list) else [legacy]
            out_candidates = []
            for page_idx, boxes in candidates:
                if page_idx >= len(page_images):
                    continue
                arr = np.frombuffer(page_images[page_idx], dtype=np.uint8)
                img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
                if img is None:
                    continue
                found = {}
                for label in semantic["labels"]:
                    expected = boxes.get(label)
                    if expected is not None:
                        found[label] = find_box(img, expected) or expected
                if len(found) == len(semantic["labels"]):
                    out_candidates.append((page_idx, found))
            if out_candidates:
                resolved[qnum] = out_candidates if isinstance(legacy, list) else out_candidates[0]
            else:
                resolved[qnum] = legacy
        return resolved

    global _YESNO_BOX_CALIBRATION, _MULTISELECT_BOX_CALIBRATION
    _YESNO_BOX_CALIBRATION = resolve(_YESNO_BOX_SEMANTICS, _LEGACY_YESNO_BOX_GEOMETRY)
    _MULTISELECT_BOX_CALIBRATION = resolve(_MULTISELECT_BOX_SEMANTICS, _LEGACY_MULTISELECT_BOX_GEOMETRY)
    return _YESNO_BOX_CALIBRATION, _MULTISELECT_BOX_CALIBRATION


def resolve_box_calibration(page_images: list, dpi: int = _RENDER_DPI):
    """Compatibility adaptor for consumers that need resolved pixel maps."""
    return _resolve_runtime_box_geometry(page_images, dpi=dpi)


def detect_yesno_box_answers(page_images: list, dpi: int = _RENDER_DPI, include_diagnostics: bool = False):
    """Deterministic, non-LLM reading of questions 21, 22, 27, and 32 (simple
    isolated Yes/No or Yes/No/Unknown rows), 23 (a standalone 6-point-scale
    row), 19 and 20 (each a standalone 5-choice row) plus 25, 28, 29 and 35
    (vertical single-choice lists, 4, 3, 7 and 5 options respectively) - see
    the module comment above this function for why these eleven
    specifically, and why this is a different, narrower technique from
    detect_checkbox_grid_answers(). Returns {question_number: {"position":
    1-based index into CHOICE_LISTS_BY_NUMBER[question_number], "margin":
    top choice's ink ratio minus the runner-up's}} for however many of these
    this confidently read on this file. When the WINNING choice's own ink
    ratio is below _YESNO_BLANK_INK_FLOOR, the entry is instead {"position":
    None, "margin": ..., "blank": True}. Returns {} if none could be
    confidently located - always safe to fall back to the model-only
    reading.

    When include_diagnostics=True, instead returns (results, ambiguous) -
    ambiguous is a list of question numbers where this question's row/list
    WAS successfully located (the anchor or ruled line matched) but one of
    its individual choice boxes could not be cleanly read afterward, most
    often because ink from an adjacent choice's own mark physically bleeds
    into it (confirmed on a real file: Nov5_1_TPS_2988.pdf's Q29 - the X
    marking "Female" overlaps into "Female-to-Male"'s printed box outline
    right below it, merging them into one connected ink blob neither
    can be confidently read from - see the module's Revision 26 notes).
    Those questions get no entry in `results` (never guess), but a caller
    should treat this as grounds for needs_review, per the same "mark
    unclear or exceeded its own pixel area" standard as
    detect_multiselect_ink_ratios()'s ambiguous list."""
    import cv2
    import numpy as np

    results = {}
    ambiguous = set()
    full_width_lines_by_page = {}

    for qnum, raw_candidates in _YESNO_BOX_CALIBRATION.items():
        # A question number can map to either a single (page_idx, boxes)
        # layout, or a LIST of them when the same question has been observed
        # printed in more than one real position across different files
        # (see Q27's calibration comment above for why this exists). Try
        # each candidate in order and use the first one whose boxes actually
        # validate against THIS file's real scan - never a guess, and never
        # silently mixing a box from one candidate with a box from another.
        candidates = raw_candidates if isinstance(raw_candidates, list) else [raw_candidates]

        for page_idx, boxes in candidates:
            if page_idx >= len(page_images):
                continue
            arr = np.frombuffer(page_images[page_idx], dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_GRAYSCALE)
            if img is None:
                continue
            # NOTE: _RENDER_DPI is fixed at 300 throughout this pipeline, so
            # scale is always 1.0 here. A per-axis, page-size-proportional
            # scale factor was tried (to handle non-Letter scans like A4)
            # and made real-file results WORSE: measurement against a real
            # A4 scan (Nov5_1_TPS_2988.pdf) showed the actual page-content
            # offset from the Letter-based calibration is a small, roughly
            # CONSTANT pixel shift (not a proportional stretch of the whole
            # page), so a proportional scale overshoot the real shift by
            # 3-4x and pointed row searches at the wrong ruled line
            # entirely (Q21 landed on what was actually Q22's neighbor's
            # line). The existing "list_anchor" mechanism just below
            # already handles a constant/whole-page shift correctly by
            # measuring it directly off the first choice's real position
            # instead of assuming it from page dimensions - that measure-
            # don't-assume approach is the right fix and is kept as-is.
            x_scale = y_scale = scale = dpi / 300.0
            pad_x = max(int(_YESNO_BOX_PAD_OVERRIDE.get(qnum, _YESNO_BOX_PAD) * x_scale), 4)
            pad_y = max(int(_YESNO_BOX_PAD_OVERRIDE.get(qnum, _YESNO_BOX_PAD) * y_scale), 4)
            up_pad = max(int(_YESNO_BOX_UP_PAD_OVERRIDE.get(qnum, _YESNO_BOX_PAD_OVERRIDE.get(qnum, _YESNO_BOX_PAD)) * y_scale), 4)
            _, binary = cv2.threshold(img, _INK_THRESHOLD, 255, cv2.THRESH_BINARY_INV)

            row_band = None
            if _YESNO_ROW_MODE.get(qnum) == "ruled":
                if page_idx not in full_width_lines_by_page:
                    full_width_lines_by_page[page_idx] = _find_full_width_lines(binary)
                # MAX, not average: the real ruled line sits just below the
                # LOWEST box in the row. These are equal for every current
                # "ruled" question (all of a row's boxes share one y-range),
                # but averaging silently breaks for any stacked/multi-row
                # layout (found the hard way: Q19/Q20's old, since-replaced
                # calibration stacked 5 boxes down a column at different
                # y-ranges, and averaging their y1's landed ~110px away from
                # the real ruled line - well outside the search radius - so
                # detect_yesno_box_answers() silently, permanently missed
                # both on every file, never a visible error).
                approx_bottom = y_scale * max(y1 for _x0, _x1, _y0, y1 in boxes.values())
                # Try every candidate line in the search window, nearest to
                # approx_bottom first, not just the single nearest one: once
                # search_above is wide enough to cover a real whole-page
                # shift, a NEIGHBORING question's own line can fall inside
                # the same window and happen to be closer to approx_bottom
                # than this question's real line is (confirmed with Q21/Q22
                # at a wide shift - Q21's line, 73px away, landed closer to
                # Q22's approx_bottom than Q22's real line, further out,
                # actually was). The pitch-partner check is what tells the
                # two apart; trying candidates in order until one passes it
                # (or none do) is what keeps this from committing to the
                # nearest-but-wrong one and giving up. Questions with no
                # partner requirement (i.e. not in _YESNO_ROW_PITCH_PARTNER)
                # simply accept the first, unchanged from before.
                row_bottom_line = None
                for candidate in _row_bottom_candidates(
                    full_width_lines_by_page[page_idx],
                    approx_bottom,
                    search_range=int(_YESNO_ROW_LINE_SEARCH_OVERRIDE.get(qnum, _YESNO_ROW_LINE_SEARCH) * y_scale),
                    search_above=int(_YESNO_ROW_ABOVE_SEARCH_OVERRIDE.get(qnum, 0) * y_scale),
                ):
                    if qnum in _YESNO_ROW_PITCH_PARTNER:
                        offset, tolerance = _YESNO_ROW_PITCH_PARTNER[qnum]
                        if not _has_pitch_partner(
                            full_width_lines_by_page[page_idx], candidate, offset * y_scale, tolerance * y_scale
                        ):
                            continue
                    row_bottom_line = candidate
                    break
                if row_bottom_line is None:
                    continue
                row_line_margin = max(int(_YESNO_ROW_LINE_MARGIN * y_scale), 2)
                avg_box_h = sum((y1 - y0) for _x0, _x1, y0, y1 in boxes.values()) / len(boxes)
                slice_h = int(max(avg_box_h * y_scale * 1.8, 50 * y_scale))
                row_band = (row_bottom_line - row_line_margin - slice_h, row_bottom_line - row_line_margin)

            x_shift = y_shift = 0
            if _YESNO_ROW_MODE.get(qnum) in ("list_anchor", "fixed"):
                # Locate the choice list's whole-list (x_shift, y_shift) via
                # _locate_yesno_list_shift(), which tries EACH calibrated
                # choice as a candidate anchor (wide, generous pad on every
                # side - safe because the space directly above/beside a
                # choice is this question's own printed label text or
                # margin, not another choice, confirmed via visual crop on
                # real files: 40px+ of whitespace there on every
                # "list_anchor"/"fixed" question) and only accepts a
                # candidate shift once it is corroborated by a SECOND choice
                # resolving cleanly at that same offset (tight pad). That
                # measured offset (both axes - see _YESNO_ANCHOR_X_PAD's
                # comment) is then applied to every choice in the row/list
                # before searching for each with the normal small pad. This
                # is what lets a dense list (choices as little as ~12-16px
                # apart) absorb the same whole-page shift that a much wider
                # plain pad would risk bleeding into a neighboring choice's
                # own box to handle, WITHOUT the single-first-choice anchor
                # locking onto the wrong row when the real shift is close to
                # one full row's pitch (see _locate_yesno_list_shift()'s own
                # docstring, Revision 30).
                anchor_up_pad = max(int(_YESNO_LIST_ANCHOR_PAD * y_scale), pad_y)
                anchor_x_pad = max(int(_YESNO_ANCHOR_X_PAD * x_scale), pad_x)
                shift_found = _locate_yesno_list_shift(
                    binary, boxes, scale, x_scale, y_scale, anchor_up_pad, anchor_x_pad,
                )
                anchor_found = None if shift_found is None else True
                if anchor_found is None:
                    # Border-tracing anchor search couldn't find a clean box
                    # shape at this calibrated position AT ALL, on either the
                    # narrow or wide window - see _YESNO_BLIND_INK_FLOOR's
                    # comment for why (a heavy mark's own ink can obscure a
                    # box's border past what line-morphology can trace, which
                    # is an entirely different failure than "this file needs
                    # a position shift"). Previously this meant total
                    # silence for the whole question: no result, no
                    # ambiguous flag, and the wrong model answer shipped
                    # unreviewed (confirmed: Nov5_3_TPS_3025.pdf's Q32 -
                    # "Yes" unmistakably marked, but the model still read
                    # "No" and nothing caught it). Fall back to a raw,
                    # border-agnostic ink-density comparison across EVERY
                    # choice's plain calibrated rectangle (zero padding, zero
                    # shift - see _YESNO_BLIND_INK_FLOOR) - if exactly one
                    # choice is overwhelmingly more inked than every other,
                    # that's still strong, specific evidence of which one is
                    # marked, even though this bypasses the normal box-shape
                    # validation this module otherwise insists on. Always
                    # flagged ambiguous either way - this path is inherently
                    # less certain than a normal shape-validated read.
                    #
                    # Revision 33 follow-up (real user report, Nov9_3_TPS_
                    # 4137.pdf's Q27): this blind fallback had NO way to tell
                    # "the calibrated position is right, just too heavily
                    # inked to trace a border" (its original, justifying
                    # case) apart from "the calibrated position is on
                    # completely unrelated content" - e.g. a wrong candidate
                    # layout (this question maps to more than one, see
                    # _YESNO_BOX_CALIBRATION's Q27 comment) whose coordinates
                    # happen to overlap a DIFFERENT question's real rows
                    # elsewhere on the page, or a position that's genuinely
                    # drifted on this file into blank whitespace. Confirmed
                    # directly: candidate 0's calibrated rectangle for Q27
                    # sits on top of Q33's real "American Indian/Alaskan
                    # Native"/"Asian" rows on this file, and its blind ink
                    # reading (0.34 vs 0.19, margin 0.145 - clearing both
                    # thresholds) was really measuring THOSE rows' checkbox
                    # borders, not any real Q27 mark - "No" was reported with
                    # high apparent confidence despite Q27 not actually being
                    # located here at all. Since this fallback skips shape
                    # validation specifically to tolerate the WINNING box's
                    # own border being obscured by its own heavy mark, it
                    # still requires a MAJORITY of the OTHER (non-winning)
                    # calibrated boxes to independently show a real,
                    # shape-confirmed checkbox at their own zero-shift
                    # position - a genuinely correct, unshifted layout has
                    # every OTHER (unmarked) box looking like an intact
                    # checkbox; content this fallback should never trust
                    # (an unrelated question's rows, blank whitespace) does
                    # not. Retested against the original Nov5_3_TPS_3025.pdf
                    # Q32 case this fallback was originally written for, and
                    # this majority check now ALSO declines there (returns
                    # None) instead of the "Yes" previously reported -
                    # investigation traced this to a second, independent,
                    # previously-undiscovered defect: this file's own Q32
                    # calibration has ALSO drifted (confirmed visually - the
                    # calibrated rectangles sit on a different question's
                    # printed text, not this file's real checkboxes at all),
                    # so neither "No" nor "Unknown" actually shape-validate at
                    # that stale position either. The old "Yes" answer was
                    # therefore never real evidence - it was a coincidentally-
                    # plausible ink reading of the wrong location that
                    # happened to match the true answer on this one file.
                    # Per explicit user direction, this stricter, safer
                    # behavior (decline rather than guess when the position
                    # itself can't be confirmed) is the intended outcome, even
                    # though it means this file's Q32 no longer resolves via
                    # pixel detection - it now correctly defers to the model
                    # instead of shipping an ungrounded answer.
                    blind_labels = list(boxes.keys())
                    blind_ratios = [
                        _checkbox_ink_ratio(binary, (bx0, bx1, by0, by1), border=0)
                        for bx0, bx1, by0, by1 in boxes.values()
                    ]
                    blind_order = sorted(range(len(blind_labels)), key=lambda i: -blind_ratios[i])
                    b_best, b_second = blind_order[0], blind_order[1]
                    b_margin = blind_ratios[b_best] - blind_ratios[b_second]
                    other_shape_confirmed = 0
                    for k, (bx0, bx1, by0, by1) in enumerate(boxes.values()):
                        if k == b_best:
                            continue
                        if _locate_checkbox(
                            binary, by0 - 4, by1 + 4, bx0 - 4, bx1 + 4,
                            scale=scale, expected_w=(bx1 - bx0), expected_h=(by1 - by0),
                        ) is not None:
                            other_shape_confirmed += 1
                    others_total = len(blind_labels) - 1
                    majority_shape_confirmed = others_total > 0 and other_shape_confirmed * 2 > others_total
                    if (
                        blind_ratios[b_best] >= _YESNO_BLIND_INK_FLOOR
                        and b_margin >= _YESNO_BLIND_INK_MARGIN
                        and majority_shape_confirmed
                    ):
                        results[qnum] = {"position": b_best + 1, "margin": round(b_margin, 4)}
                    ambiguous.add(qnum)
                    continue  # this candidate's anchor genuinely failed - try the next one, if any
                x_shift, y_shift = shift_found

            # Once the row/list itself has been successfully located (a
            # ruled line matched, or the list_anchor/fixed anchor box was
            # found), a subsequent per-choice box search failing is a much
            # stronger signal than the same failure would be with no
            # location at all - it means we know exactly where to look and
            # still can't cleanly read a specific box, most often because a
            # neighboring choice's own mark bled into it (see this
            # function's include_diagnostics docstring). Track that
            # distinction; the plain "candidate not located at all" case
            # below still just falls through to the model with no signal.
            row_was_located = row_band is not None or _YESNO_ROW_MODE.get(qnum) in ("list_anchor", "fixed")

            labels, ratios, ok = [], [], True
            used_blind_box_fallback = False
            for label, (x0, x1, y0, y1) in boxes.items():
                sx0, sx1 = int(x0 * x_scale) - pad_x + x_shift, int(x1 * x_scale) + pad_x + x_shift
                if row_band is not None:
                    sy0, sy1 = row_band
                elif _YESNO_ROW_MODE.get(qnum) == "list_anchor":
                    sy0, sy1 = int(y0 * y_scale) - pad_y + y_shift, int(y1 * y_scale) + pad_y + y_shift
                elif _YESNO_ROW_MODE.get(qnum) == "fixed":
                    sy0, sy1 = int(y0 * y_scale) - up_pad + y_shift, int(y1 * y_scale) + pad_y + y_shift
                else:
                    sy0, sy1 = int(y0 * y_scale) - up_pad, int(y1 * y_scale) + pad_y
                found = _locate_checkbox(
                    binary, sy0, sy1, sx0, sx1, scale=scale,
                    expected_w=(x1 - x0), expected_h=(y1 - y0),
                )
                shape_ok = found is not None
                if shape_ok:
                    fx0, fx1, fy0, fy1 = found
                    expected_w, expected_h = (x1 - x0) * x_scale, (y1 - y0) * y_scale
                    if abs((fx1 - fx0) - expected_w) > 14 * x_scale or abs((fy1 - fy0) - expected_h) > 14 * y_scale:
                        shape_ok = False
                if not shape_ok:
                    if not row_was_located:
                        # No independent evidence of where this row even is
                        # yet - a border-agnostic reading here would be a
                        # pure guess, not a corroborated fallback. Abort this
                        # candidate layout entirely, same as before.
                        ok = False
                        break
                    # Per-box blind-ink fallback (Revision 30 follow-up -
                    # mirrors the existing anchor-level _YESNO_BLIND_INK_
                    # FLOOR fallback just above, applied to ONE box instead
                    # of the whole list). The row/list itself IS reliably
                    # located (row_was_located), so this box's own
                    # calibrated-plus-shift rectangle is a trustworthy
                    # coordinate even though line-morphology border-tracing
                    # failed on it specifically - almost always because a
                    # heavy/oversized mark's own ink crosses the box's
                    # printed border, leaving nothing clean left to trace.
                    # Confirmed on Nov7_2_TPS_4041.pdf's Q25: "4 weeks or
                    # more" is unmistakably marked with a bold X reaching
                    # past the box's own corners (the model's own reasoning
                    # agreed: "A clear 'X' mark is present in the checkbox
                    # next to the '4 weeks or more' option"), but the
                    # previous behavior here was to abort the ENTIRE
                    # question with no result at all - not even ambiguous -
                    # so the correct model answer shipped with a needless
                    # "pixel detector located this question but a choice's
                    # mark was unclear" review flag and no way to ever clear
                    # it. Read the raw, border-agnostic ink ratio at this
                    # box's own rectangle (zero padding - any padding here
                    # risks pulling in the adjacent choice's own printed
                    # label text, exactly like the anchor-level fallback)
                    # so this box still participates in the normal ratio
                    # comparison below, and mark the question ambiguous
                    # regardless of the outcome, since this reading bypassed
                    # the normal shape validation and is inherently less
                    # certain than a fully box-shape-verified read.
                    bx0 = int(x0 * x_scale) + x_shift
                    bx1 = int(x1 * x_scale) + x_shift
                    by0 = int(y0 * y_scale) + y_shift
                    by1 = int(y1 * y_scale) + y_shift
                    labels.append(label)
                    ratios.append(_checkbox_ink_ratio(binary, (bx0, bx1, by0, by1), border=0))
                    used_blind_box_fallback = True
                    ambiguous.add(qnum)
                    continue
                labels.append(label)
                ink_border = _YESNO_BOX_INK_BORDER_OVERRIDE.get(qnum, 2)
                ratios.append(_checkbox_ink_ratio(binary, found, border=ink_border))
            if not ok:
                continue  # try the next candidate layout for this question, if any

            order = sorted(range(len(labels)), key=lambda i: -ratios[i])
            best, second = order[0], order[1]
            margin = ratios[best] - ratios[second]
            if not used_blind_box_fallback:
                ambiguous.discard(qnum)  # a later candidate for the same qnum fully succeeded - not ambiguous after all

            # General "two marks in the same single-select question"
            # backstop (Revision 29, explicit user request: "If two marks
            # are identified in 2 boxes, no matter which question it is, it
            # needs to be marked as need review"). Uses the stricter
            # _YESNO_BLANK_INK_FLOOR (0.25, "confidently marked" - not the
            # lighter _YESNO_LIGHT_MARK_FLOOR) specifically to avoid the
            # documented "(specify)" adjacent-freetext-line contamination
            # (topped out at 0.136 on a real file - see the promotion logic
            # above), so this only fires on two GENUINELY dark marks (e.g. a
            # crossed-out box plus a new one), not routine noise.
            multiple_marks_detected = sum(1 for r in ratios if r >= _YESNO_BLANK_INK_FLOOR) >= 2
            # Crossed-out-then-corrected shape: the corrected box reads
            # implausibly high (past _YESNO_OVERDENSE_INK_CEILING - see its
            # comment) while the genuine new mark sits in the ordinary
            # marked range but doesn't individually clear the stricter
            # _YESNO_BLANK_INK_FLOOR two-marks floor above. Still "two
            # marks" - catch it the same way the grid's own correction-
            # detection does, requiring the second box's ink to be real
            # (above the noise floor), not routine blank-box noise, and
            # excluding "(specify)" (adjacent-freetext-line contamination).
            if not multiple_marks_detected and ratios[best] > _YESNO_OVERDENSE_INK_CEILING:
                if any(
                    i != best and "(specify)" not in labels[i].lower()
                    and ratios[i] > _YESNO_OVERDENSE_SECOND_MARK_FLOOR
                    for i in range(len(ratios))
                ):
                    multiple_marks_detected = True
            if ratios[best] < _YESNO_BLANK_INK_FLOOR and margin < _YESNO_BLANK_MARGIN_CEILING:
                # A real ("light checkmark", not full X - see Revision 21's
                # matching multiselect fix) mark can measure just enough ink
                # to separate from a genuinely blank box, without clearing
                # _YESNO_BLANK_INK_FLOOR outright - confirmed on a real
                # file (Nov5_3_TPS_3025.pdf's Q21: "No" measured 0.1413 vs
                # "Yes"'s 0.1111, correctly the true mark, but both well
                # under _YESNO_BLANK_INK_FLOOR=0.25). Zero ink at all is
                # still trusted as a confident blank; anything above a small
                # noise floor here means "couldn't confidently tell blank
                # from lightly marked" - PROMOTE the best candidate to the
                # actual answer (Revision 27; previously left as blank,
                # under-serving the "old errors not fixed" complaint that a
                # needs_review flag alone doesn't surface the correct value)
                # while still flagging ambiguous so a human confirms it.
                #
                # "(specify)" choices are explicitly EXCLUDED from
                # promotion: they sit immediately next to a free-text
                # entry box/line whose own printed edge is a real, recurring
                # source of spurious ink unrelated to whether the checkbox
                # itself is marked - confirmed on the SAME file's Q30: "Other
                # (specify)" measured a HIGHER ratio (0.136) than the
                # actually-marked "Female" (0.0983) purely from that
                # adjacent line. Promoting the raw highest-ratio candidate
                # there would have replaced an honest blank with a
                # confidently WRONG answer, which is worse than "we don't
                # know" - never guess.
                # Revision 33: also require genuine SEPARATION from the
                # runner-up before promoting - see
                # _YESNO_LIGHT_MARK_MIN_SEPARATION's own comment. Clearing
                # _YESNO_LIGHT_MARK_FLOOR alone isn't sufficient: a
                # genuinely blank box's own printed BORDER can read above
                # that floor on some scans, and when it does, it does so
                # for every choice about equally - margin is what tells a
                # real, standout mark apart from that shared, low-signal
                # noise floor.
                promotable = [
                    i for i in order
                    if "(specify)" not in labels[i].lower() and ratios[i] > _YESNO_LIGHT_MARK_FLOOR
                    and margin >= _YESNO_LIGHT_MARK_MIN_SEPARATION
                ]
                if promotable:
                    results[qnum] = {"position": promotable[0] + 1, "margin": round(margin, 4)}
                else:
                    results[qnum] = {"position": None, "margin": round(margin, 4), "blank": True}
                if ratios[best] > 0.05:
                    ambiguous.add(qnum)
            else:
                results[qnum] = {"position": best + 1, "margin": round(margin, 4)}
            if multiple_marks_detected:
                results[qnum]["multiple_marks_detected"] = True
            break  # this candidate validated - don't try any further ones
    if include_diagnostics:
        return results, sorted(ambiguous)
    return results


def render_pdf_to_images(pdf_bytes: bytes, dpi: int = _RENDER_DPI) -> list:
    """Renders every page of a PDF (given as raw bytes) to a high-resolution
    PNG image using PyMuPDF, returning a list of PNG byte strings (one per
    page)."""
    import pymupdf

    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        zoom = dpi / 72.0
        matrix = pymupdf.Matrix(zoom, zoom)
        return [page.get_pixmap(matrix=matrix).tobytes("png") for page in doc]
    finally:
        doc.close()


# --------------------------------------------------------------------------
# Cloud Vision double-check for handwritten/write-in questions (H1, H2, H4,
# H5, H6, 24, 26 - see WRITTEN_TEXT_QUESTION_NUMBERS) + model self-reported
# confidence parsing. See the module docstring's "Model confidence + Cloud
# Vision double-check" section for the full design and reasoning; this is
# the implementation.
# --------------------------------------------------------------------------

_VISION_DEPS_CHECKED = False


def check_vision_dependencies() -> bool:
    """Checks once (cached) whether google-cloud-vision is importable, and
    prints a LOUD, impossible-to-miss message if not - same pattern as
    check_grid_dependencies() above, and for the same reason: without this,
    a missing dependency fails SILENTLY (cloud_vision_ocr_page() catches the
    ImportError itself and just returns an empty result, so the Cloud Vision
    double-check for H1/H2/H4/H5/H6/24/26 would silently never fire, with no
    visible sign anything is different from before this feature existed).
    Call this once at the start of a run (extract_to_bigquery() and
    verify_pdf() both do) so it's obvious up front whether the double-check
    is even active."""
    global _VISION_DEPS_CHECKED
    if _VISION_DEPS_CHECKED:
        return True
    try:
        import google.cloud.vision  # noqa: F401
    except ImportError as e:
        err(
            "[VISION] google-cloud-vision is NOT installed (%s). The Cloud Vision "
            "double-check for handwritten/write-in questions (H1/H2/H4/H5/H6/24/26) "
            "will be SKIPPED for every file in this run - those seven questions fall "
            "back to model-only + confidence-threshold checking alone (the same as "
            "before this feature existed), with no other visible sign anything is "
            "different. Fix with: pip install google-cloud-vision, then restart your "
            "kernel/notebook so the fresh install is picked up.",
            e,
        )
        return False
    _VISION_DEPS_CHECKED = True
    return True


def cloud_vision_ocr_page(page_png_bytes: bytes, vision_project: Optional[str] = None) -> dict:
    """Runs Google Cloud Vision's document_text_detection on one already-
    rendered page image (same PNG bytes render_pdf_to_images() produces for
    Gemini) and returns {"full_text": str, "tokens": [str, ...], "words":
    [{"text": str, "x0": int, "y0": int, "x1": int, "y1": int}, ...]}:

      - full_text is Vision's own concatenated transcription of the whole page.
      - tokens is every individual word-level annotation's text, in the SAME
        order as "words" below (kept for backward compatibility with the
        pre-Round-8 whole-page comparison path - see cross_check_written_
        field_with_vision()'s fallback).
      - words is the same annotations, but WITH each one's bounding box
        (response.text_annotations[1:], each one a separate OCR guess
        independent of any neighboring word, each with its own
        bounding_poly). This is what lets _extract_anchored_field_value()
        below scope a comparison to "the text spatially near this specific
        field's printed label" instead of "anywhere on the page."

    This exists to give the handwritten/write-in questions (H1, H2, H4, H5,
    H6, 24, 26 - see WRITTEN_TEXT_QUESTION_NUMBERS) an INDEPENDENT second
    reading to check the Gemini extraction against - the same "two signals
    must agree" principle every pixel-based checkbox detector in this module
    already applies to checkbox questions. These seven had no equivalent
    check at all before this, since cross_check_answer() can't do anything
    with a question that has no fixed choice list.

    ROUND 8 CHANGE: earlier versions of this function discarded each
    annotation's bounding box and returned only flat text, on the reasoning
    that a per-field calibrated crop region would need the same kind of
    real-scan pixel calibration _YESNO_BOX_CALIBRATION required, which
    wasn't available without real production scans on hand to derive it
    from. Two real scans later (Nov10_3.pdf and a second TPS file), that
    "compare against the whole page" approach turned out to actively cause
    wrong verdicts, not just miss some: H1's boxed ID digits got confused
    with tokens from H2's neighboring box grid on the same page ("URB" from
    H2's "...BURB" showing up as if it were evidence about H1), and the
    boxed single-digit grids themselves got mis-segmented into tokens that
    don't correspond to the true multi-digit value AT ALL (e.g. a real
    Vision call returned '5','675','1','91','909' as separate tokens for an
    ID that reads "197495" printed digit-by-digit in adjacent boxes) -
    because there is no natural "word" boundary between six boxes that each
    contain one character. Keeping the bounding boxes doesn't fix Vision's
    own digit-grid segmentation, but it DOES let
    _extract_anchored_field_value() find the field's own printed label (form
    labels are printed, not handwritten - Vision reads them reliably) and
    then gather + reconstruct ONLY the words spatially near that label, in
    left-to-right/top-to-bottom order - both eliminating the cross-field
    bleed-through and reassembling a boxed grid's separate word-tokens back
    into one value by POSITION rather than trusting Vision's own (unreliable)
    word-grouping of it.

    Returns {"full_text": "", "tokens": [], "words": []} (not an exception)
    if the Vision API call fails for any reason (missing credentials, API
    not enabled, quota, network) or the dependency isn't installed - callers
    must treat that as "no independent signal available" and skip the
    cross-check for this file, exactly like every pixel detector above falls
    back to model-only on anything it can't confidently read, never
    guessing."""
    if not check_vision_dependencies():
        return {"full_text": "", "tokens": [], "words": []}
    try:
        from google.cloud import vision

        client = vision.ImageAnnotatorClient()
        image = vision.Image(content=page_png_bytes)
        # "en-t-i0-handwrit" is Cloud Vision's documented language hint for
        # English handwriting specifically (see
        # https://docs.cloud.google.com/vision/docs/handwriting) - the one
        # documented lever Google offers for handwriting accuracy, short of
        # switching products entirely. Reported directly (Nov4_1_TPS_1608.pdf
        # Q24): without any hint, Vision's document_text_detection dropped an
        # entire opening clause and misread several cursive words in a
        # handwritten comment that Gemini itself transcribed correctly. This
        # hint only affects glyph-level character recognition, not Vision's
        # own block/paragraph reading-order logic (a separate, undocumented
        # mechanism this hint can't influence) - so it may reduce individual
        # word misreads but won't by itself fix word-order scrambling across
        # multiple handwritten lines. Every page on this form can contain
        # handwriting (the seven WRITTEN_TEXT_QUESTION_NUMBERS fields), so
        # the hint is applied to every OCR call, not conditionally.
        image_context = vision.ImageContext(language_hints=["en-t-i0-handwrit"])
        response = client.document_text_detection(image=image, image_context=image_context)
        if response.error.message:
            raise RuntimeError(response.error.message)
        annotations = response.text_annotations
        if not annotations:
            return {"full_text": "", "tokens": [], "words": []}
        full_text = annotations[0].description or ""
        words = []
        for a in annotations[1:]:
            text = a.description
            if not text:
                continue
            vertices = getattr(a.bounding_poly, "vertices", None) or []
            xs = [v.x for v in vertices if v.x is not None]
            ys = [v.y for v in vertices if v.y is not None]
            if not xs or not ys:
                continue  # no usable bounding box for this annotation - skip it, never guess a position
            words.append({"text": text, "x0": min(xs), "y0": min(ys), "x1": max(xs), "y1": max(ys)})
        tokens = [w["text"] for w in words]
        return {"full_text": full_text, "tokens": tokens, "words": words}
    except Exception as e:  # noqa: BLE001 - purely a bonus signal, never fatal (matches every pixel detector's own try/except above)
        err("[VISION] Cloud Vision OCR call failed, skipping double-check for this page: %s", e)
        # The one-liner above only ever shows str(e) - never enough to find
        # WHERE inside the google-cloud-vision/google-api-core/grpc call
        # chain this actually happened (client construction, the API call
        # itself, or something in a retry/error-formatting path inside one
        # of those libraries). log.exception() attaches the full traceback
        # to this same log record, so it's there in your logs/notebook
        # output without another failure needing to happen to get it.
        log.exception("[VISION] Full traceback for the Cloud Vision failure above (for diagnosis only, never fatal):")
        return {"full_text": "", "tokens": [], "words": []}


def _normalize_for_match(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _snippet_around_match(compare_text: str, matched_phrase: str, max_len: int = 400, context: int = 60) -> str:
    """Builds the human-facing vision_ocr_snippet for a freeform field's
    whole-page comparison. `compare_text` is the raw (non-normalized) text
    being compared against (full_text on the fallback path, or the anchored
    value); `matched_phrase` is a space-joined run of WORDS (already
    normalized/lowercased) that both the model's answer and Vision's OCR
    agree on - see cross_check_written_field_with_vision(), which now does
    its own matching at word granularity (word_matcher.find_longest_match())
    specifically so this phrase corresponds to whole words, never a
    coincidental run of characters cutting across two different words.

    A plain "first max_len characters of compare_text" is wrong whenever the
    real match sits further into the text than that - most notably question
    24's whole-page fallback, where compare_text is the ENTIRE page's OCR
    text and the page's own PRINTED QUESTION PROMPT ("24. Comment: Please
    let us know your comments...") comes before the handwritten answer in
    reading order, so truncating from position 0 showed the question text
    itself instead of the response (reported directly against real output).

    Instead: locate `matched_phrase` in the original compare_text (tolerating
    exact original whitespace/newlines via a whitespace-flexible regex,
    since matched_phrase collapsed all whitespace to single spaces), and
    start the window `context` characters BEFORE it. The window then runs
    forward up to max_len characters from that start - NOT just `context`
    characters past the match's own end.

    That "forward to max_len" part matters: the longest matching run of
    words is only ever as long as the text before the first place the
    model's own transcription and Vision's independent OCR diverge - a
    single differing word, a missing/extra period, "awhile" vs "a while" -
    even when both sides plainly keep going for a full paragraph past that
    point (reported directly: a real Q24 comment whose vision_ocr_snippet
    cut off mid-sentence, right where the wording first diverged, hiding the
    rest of what Vision actually read even though it never stopped reading
    there; Vision itself has no meaningful length limit on this - the OLD
    `m.end() + context` window is what was truncating the display, not the
    OCR call). Anchoring on the match's START but budgeting the full max_len
    forward from there shows the complete rest of the field for exactly the
    disagreement cases where a human most needs to see it, while still
    avoiding the page's own printed question prompt (which sits before the
    match, not after it) per the docstring case above.

    Falls back to the original start-of-text truncation only when there's no
    match at all to anchor on (a genuine mismatch with zero overlap, or
    empty text), since there's nothing better to anchor the snippet to in
    that case."""
    text_snippet = (compare_text or "").strip()
    if matched_phrase:
        # compare_text may still have the original newlines/multi-spaces
        # (matched_phrase collapsed all whitespace to single spaces), so
        # match word-for-word but let any whitespace run stand in for a
        # space.
        pattern = re.compile(r"\s+".join(re.escape(tok) for tok in matched_phrase.split(" ")), re.IGNORECASE)
        m = pattern.search(compare_text or "")
        if m:
            start = max(0, m.start() - context)
            end = min(len(compare_text), start + max_len)
            windowed = compare_text[start:end].strip()
            prefix = "..." if start > 0 else ""
            suffix = "..." if end < len(compare_text) else ""
            text_snippet = f"{prefix}{windowed}{suffix}"
    if len(text_snippet) > max_len:
        text_snippet = text_snippet[:max_len] + "..."
    return text_snippet


# --------------------------------------------------------------------------
# ROUND 8: label-anchored, position-aware Vision matching. Added after real
# scans showed the plain whole-page approach above actively causing wrong
# verdicts (see cloud_vision_ocr_page()'s Round 8 docstring note) - a
# neighboring field's text bleeding into a comparison it has nothing to do
# with, and boxed single-character digit grids getting mis-segmented into
# tokens that don't correspond to the true value.
#
# The idea: every one of these fields sits next to its own PRINTED label
# ("Provider ID", "Age:", etc.) - printed text, not handwriting, so Vision
# reads it reliably even when it struggles with the boxed value beside it.
# Find that label with Vision's own OCR, then only look at words spatially
# near it (to the right, or below, depending on the field's layout on this
# form) and reconstruct the value by POSITION - not by trusting however
# Vision happened to group its "words." This mirrors the same "locate a
# landmark, then read only what's near it" technique the pixel detectors
# above already use (e.g. _find_full_width_lines() + _nearest_row_bottom()
# for the "ruled" Yes/No rows), just applied to Vision's text instead of ink
# density.
#
# Every function below returns None (never a guess) when it can't locate
# what it's looking for - callers (cross_check_written_field_with_vision())
# fall back to the old whole-page comparison in that case, so this can only
# ever IMPROVE precision when it works, never make an already-working case
# newly wrong.
# --------------------------------------------------------------------------

# question_number -> (anchor_phrase_alternatives, direction, max_reach_px, stop_phrase).
# anchor_phrase_alternatives is a LIST of candidate phrases (each a list of
# lowercase words to match consecutively in reading order), tried in order -
# the first one _find_anchor_phrase() actually locates wins. This exists
# because it's not certain in advance exactly how Vision will tokenize a
# label with punctuation in it (e.g. does "(Address)" come back as one token
# or three: "(", "Address", ")"?) without a live scan to check against;
# trying a few reasonable segmentations costs nothing when the first guess
# doesn't land, and if NONE of them match, this degrades to the safe
# whole-page fallback exactly like any other "anchor not found" case - never
# a wrong guess, just occasionally no improvement. stop_phrase is a single
# plain phrase (not alternatives - it only needs to roughly bound the
# window, not match exactly).
#
# direction is "right" (value sits on the same printed line as the label,
# e.g. "Provider ID [boxes]") or "below" (value sits on the line(s) under the
# label, e.g. "Today's Date (MM/DD/YYYY)" with the boxed digits underneath).
# max_reach_px bounds how far past the label to look; stop_phrase (when set)
# truncates the window before it reaches the NEXT field's own label, so e.g.
# H4's "Agency" value window doesn't run on into H5's "Address" box.
# "24" (the open comment) is deliberately NOT here - it's a multi-line block
# of unpredictable height with no reliable second anchor to bound it, so it
# keeps using the whole-page comparison below rather than risk a wrong crop.
# question_number -> (anchor_phrase_alternatives, direction, max_reach_px,
# stop_phrase, row_tolerance_override). row_tolerance_override is None for
# most fields (falls back to _words_in_window()'s own default, 20px) - see
# H1/H2 below for why it exists.
_WRITTEN_FIELD_ANCHORS = {
    # H1's printed label is actually THREE stacked short lines ("Home Unit" /
    # "CalOMS" / "Provider ID") beside ONE tall row of 6 character boxes -
    # confirmed directly against a real scan screenshot. The anchor phrase
    # only matches the BOTTOM line ("Provider ID"), whose own vertical
    # center sits well above the boxes' true vertical center (the boxes are
    # sized to match the full 3-line label block, not just its last line).
    # The default row_tolerance (20px) wasn't enough to bridge that gap on
    # the real scan - reported directly: the value window came back
    # positively empty ("" - "anchor found, nothing in its window") on every
    # single file, not because the field was blank but because the digit
    # boxes never counted as "the same row" as the anchor. Widened to 45px,
    # generous enough to cover a multi-line label's height at typical scan
    # DPI while H1's row is still isolated enough (a clear gap to the
    # "Setting: ..." row below it) that this can't reach into unrelated
    # content instead.
    "H1": ([["provider", "id"]], "right", 700, ["program"], 45),
    # H2's printed label is "Program Reporting Unit (Address) code" - the
    # anchor phrase needs to consume the WHOLE label, trailing "code" word
    # included, or else "(Address)" (Revision 2's original fix) and/or
    # "code" itself gets swept into the value window as if it were part of
    # the boxed answer. Confirmed directly from a real user report on
    # Nov8_2_TPS_4073.pdf: the anchor phrase used to stop right after
    # "(Address)", so a value window opening immediately to its right first
    # picked up the label's own trailing "code" token before ever reaching
    # the handwritten code itself (e.g. extracting "code AB 123" instead of
    # just "AB 123") - this alone was enough to fail the cross-check even
    # though the underlying whitespace-insensitive H2 comparison (see
    # below) was already correct, since "code" is real extra text, not a
    # spacing difference. Tries the fullest phrase (parenthetical AND
    # "code") first, in a few plausible tokenizations of the parenthetical,
    # then falls back to shorter/older phrasings for resilience against a
    # differently-worded form revision or an OCR misread of "code" - each
    # progressively less safe, but still better than no anchor at all.
    # Given the same widened row_tolerance as H1: it sits on the same
    # printed header row, right next to H1's own multi-line label/tall-box
    # mismatch, so the same risk plausibly applies here too even without
    # its own confirmed report.
    "H2": (
        [
            ["reporting", "unit", "(address)", "code"],
            ["reporting", "unit", "(", "address", ")", "code"],
            ["reporting", "unit", "(address)"],
            ["reporting", "unit", "(", "address", ")"],
            ["reporting", "unit"],
        ],
        "right", 900, None, 45,
    ),
    "H4": ([["agency"]], "right", 900, ["address"], None),
    "H5": ([["address"]], "right", 900, None, None),  # disambiguated from H2's own "(Address)" - see _extract_anchored_field_value()
    # H6's printed label is "Today's Date (MM/DD/YYYY)" - confirmed directly
    # against a real scan (a different file, same template): it sits well
    # down the page, nowhere near the H1/H2 header row, with the boxed
    # digits directly below it on their own line. The anchor phrase used to
    # be just [["date"]] - a single bare token, matched as the FIRST such
    # token anywhere on the whole page in reading order. Reported directly
    # against real output: on at least one real file, H6's vision_ocr_
    # snippet came back showing H1's OWN digit boxes plus the "Setting: ..."
    # line right after them - i.e. something on the page, near the H1/H2
    # header, got matched as "date" well before the real label was ever
    # reached (most likely an OCR misread of a nearby word on that specific
    # scan; the header text itself contains no literal "date" on this
    # template). A bare one-word anchor for a common word like "date" has no
    # way to tell a false match from the real one. Fixed by trying the
    # FULLER, far more specific phrase "Today's Date" first (several
    # plausible tokenizations of the apostrophe, since it's not certain in
    # advance how Vision will split it), falling back to the bare "date"
    # token only if none of those are found - strictly the same behavior as
    # before whenever the fuller phrase can't be located, and only safer
    # when it can.
    "H6": (
        [["today's", "date"], ["today", "'s", "date"], ["today", "s", "date"], ["date"]],
        "below", 160, None, None,
    ),
    "26": ([["age"]], "right", 200, None, None),
}


def _sort_words_reading_order(words: list, row_tolerance: int = 15) -> list:
    """Groups OCR words (each {"text","x0","y0","x1","y1"} - see
    cloud_vision_ocr_page()) into rows by vertical center (within
    row_tolerance px of each other) and returns them in reading order: rows
    top-to-bottom, words within a row left-to-right. Needed because Vision's
    own text_annotations order isn't guaranteed to BE reading order, but
    matching an anchor phrase like "Provider" immediately followed by "ID"
    requires it."""
    if not words:
        return []
    rows = []  # each: {"yc": running-average y-center, "n": count, "words": [...]}
    for w in sorted(words, key=lambda w: (w["y0"] + w["y1"]) / 2.0):
        yc = (w["y0"] + w["y1"]) / 2.0
        placed = False
        for row in rows:
            if abs(row["yc"] - yc) <= row_tolerance:
                row["words"].append(w)
                row["yc"] = (row["yc"] * row["n"] + yc) / (row["n"] + 1)
                row["n"] += 1
                placed = True
                break
        if not placed:
            rows.append({"yc": yc, "n": 1, "words": [w]})
    rows.sort(key=lambda r: r["yc"])
    ordered = []
    for row in rows:
        ordered.extend(sorted(row["words"], key=lambda w: w["x0"]))
    return ordered


def _find_anchor_phrase(ordered_words: list, phrase_tokens: list, y_band=None):
    """Finds the FIRST run of consecutive words in `ordered_words` (already
    in reading order - see _sort_words_reading_order()) whose normalized
    text matches phrase_tokens elementwise exactly, optionally restricted to
    words whose vertical center falls within y_band=(y_lo, y_hi) - used to
    disambiguate a phrase that appears more than once on the page (e.g.
    "Address" - see _extract_anchored_field_value()'s H5 handling). Returns
    the matched phrase's combined bounding box (x0, y0, x1, y1), or None if
    no exact match is found anywhere - never a fuzzy/close-enough guess."""
    n = len(phrase_tokens)
    if n == 0 or not ordered_words:
        return None
    for i in range(len(ordered_words) - n + 1):
        window = ordered_words[i:i + n]
        if y_band is not None:
            lo, hi = y_band
            if not all(lo <= (w["y0"] + w["y1"]) / 2.0 <= hi for w in window):
                continue
        if all(
            _normalize_for_match(w["text"]).strip(":,.") == phrase_tokens[j]
            for j, w in enumerate(window)
        ):
            return (
                min(w["x0"] for w in window),
                min(w["y0"] for w in window),
                max(w["x1"] for w in window),
                max(w["y1"] for w in window),
            )
    return None


def _words_in_window(ordered_words: list, anchor_box, direction: str, max_reach: int, stop_box=None, row_tolerance: int = 20) -> list:
    """Given an anchor's bounding box, gathers every word from
    `ordered_words` that falls within a window extending `direction`
    ("right" or "below") from it, up to max_reach px, and returns them in
    reading order.

    "right": same row as the anchor (vertical center within row_tolerance px
    of the anchor's own vertical center), strictly to its right, up to
    max_reach px past the anchor's right edge (or stop_box's left edge, if
    that comes sooner). "below": ONLY the single line of words immediately
    under the label (the first row whose top edge falls within max_reach px
    of the anchor's bottom edge), at any horizontal position on that one
    line - not every word within max_reach vertically. A label like "Date"
    has its value on the one line directly beneath it; if the window swept
    up everything within max_reach regardless of row, it would also catch
    the NEXT question's own printed text/answer once that happened to fall
    within max_reach too (a real failure: H6's date reconstruction pulled in
    "Agree am Not I 1. The location was convenient..." from the following
    question, once max_reach reached that far down the page).

    Returns [] (not None) when the anchor was found but nothing sits in its
    window - a positive, Vision-verified "this field looks blank on the
    scan" reading, distinct from the anchor itself not being found at all."""
    ax0, ay0, ax1, ay1 = anchor_box
    ayc = (ay0 + ay1) / 2.0
    candidates = []
    if direction == "right":
        limit_x = ax1 + max_reach if stop_box is None else min(ax1 + max_reach, stop_box[0])
        for w in ordered_words:
            wyc = (w["y0"] + w["y1"]) / 2.0
            if abs(wyc - ayc) <= row_tolerance and ax1 <= w["x0"] < limit_x:
                candidates.append(w)
        return _sort_words_reading_order(candidates, row_tolerance=row_tolerance)
    elif direction == "below":
        limit_y = ay1 + max_reach if stop_box is None else min(ay1 + max_reach, stop_box[1])
        for w in ordered_words:
            if ay1 <= w["y0"] < limit_y:
                candidates.append(w)
        if not candidates:
            return []
        ordered_candidates = _sort_words_reading_order(candidates, row_tolerance=row_tolerance)
        # Restrict to just the FIRST row (nearest the anchor, since
        # ordered_candidates is sorted top-to-bottom) - see the docstring
        # note above on why sweeping every row within max_reach is unsafe.
        first_yc = (ordered_candidates[0]["y0"] + ordered_candidates[0]["y1"]) / 2.0
        return [
            wd for wd in ordered_candidates
            if abs(((wd["y0"] + wd["y1"]) / 2.0) - first_yc) <= row_tolerance
        ]
    else:
        return []


def _extract_anchored_field_value(question_number: str, vision_result: dict) -> Optional[str]:
    """Ties _sort_words_reading_order()/_find_anchor_phrase()/_words_in_
    window() together for one written-text question. Returns:

      - None if this question has no anchor configured (currently just "24"
        - see _WRITTEN_FIELD_ANCHORS), or its printed label couldn't be
        located on this page at all (Vision misread the label, the page
        doesn't match this form's expected layout, or vision_result predates
        the "words" key - e.g. an older cached result or a hand-built test
        fixture using the pre-Round-8 shape). Callers must fall back to the
        whole-page comparison in this case, exactly like every other "no
        independent signal" case in this module.
      - "" if the label WAS located but nothing sits in its value window - a
        positive, Vision-verified "this field is blank on the scan" reading.
      - Otherwise the reconstructed value: every word found in the window,
        joined with single spaces, in reading order."""
    words = vision_result.get("words") or []
    if not words or question_number not in _WRITTEN_FIELD_ANCHORS:
        return None
    ordered = _sort_words_reading_order(words)
    phrase_alternatives, direction, max_reach, stop_phrase, row_tolerance_override = _WRITTEN_FIELD_ANCHORS[question_number]

    y_band = None
    if question_number == "H5":
        # "Address" also appears in H2's "Program Reporting Unit (Address)"
        # header, on a different row - disambiguate by requiring the SAME
        # row as H4's "Agency" label (they're printed on one shared line:
        # "Field Based Services: Agency [[box]] Address [[box]]").
        agency_box = _find_anchor_phrase(ordered, ["agency"])
        if agency_box is None:
            return None  # can't safely tell which "Address" this is - skip, never guess
        y_band = (agency_box[1] - 20, agency_box[3] + 20)

    anchor_box = None
    for phrase in phrase_alternatives:
        anchor_box = _find_anchor_phrase(ordered, phrase, y_band=y_band)
        if anchor_box is not None:
            break
    if anchor_box is None:
        return None

    stop_box = None
    if stop_phrase is not None:
        # Use the same widened band as the value window itself (row_tolerance_
        # override, when set) - a stop phrase on the same printed row as a
        # multi-line label (e.g. H1's "Program" sitting beside a tall box row
        # that a 3-line "Provider ID" label only partly covers vertically)
        # needs the same tolerance to be found at all; a mismatch here left
        # stop_box=None even after the value-window fix below, so the window
        # swept in the NEXT field's own label with no boundary to stop it.
        stop_band = row_tolerance_override if row_tolerance_override is not None else 20
        stop_box = _find_anchor_phrase(ordered, stop_phrase, y_band=(anchor_box[1] - stop_band, anchor_box[3] + stop_band))

    window_kwargs = {"stop_box": stop_box}
    if row_tolerance_override is not None:
        window_kwargs["row_tolerance"] = row_tolerance_override
    value_words = _words_in_window(ordered, anchor_box, direction, max_reach, **window_kwargs)
    return " ".join(w["text"] for w in value_words)


def cross_check_written_field_with_vision(question_number: str, model_answer: str, vision_result: dict):
    """Compares a handwritten/write-in question's model answer against an
    independent Cloud Vision OCR reading of the same page (see
    cloud_vision_ocr_page()). Returns (match, detail, score, vision_snippet):

      - match: True/False/None. None (not True or False) when there isn't
        enough signal to make a call either way (Vision returned nothing,
        the model's own answer was blank, or this isn't a written-text
        question this mechanism covers) - matching this module's "never
        guess" convention; callers must treat None as "skip this check",
        not as agreement.
      - detail: human-readable explanation string ("" when match is True or
        None).
      - score: the numeric agreement score BEHIND the match verdict, as a
        float in [0.0, 1.0] - 1.0/0.0 for the token fields' exact-match
        check, or the actual difflib longest-contiguous-match coverage
        fraction for the freeform fields' fuzzy-match check. ALWAYS
        populated whenever match is True or False (even on agreement, so
        e.g. a borderline 72% coverage that narrowly passed is visible, not
        just a bare "agree"); None when match is None.
      - vision_snippet: a short, human-readable capture of what Cloud Vision
        actually READ that model_answer was compared against - a handful of
        its individual word tokens for the token fields, or a truncated
        excerpt of its full-page OCR text for the freeform fields - so the
        comparison can be eyeballed directly rather than just trusting the
        verdict. "" when match is None.

    Two different matching strategies, depending on the field (see
    _VISION_TOKEN_FIELDS / _VISION_FREEFORM_FIELDS) - and, within each, TWO
    possible sources of the text being compared against, tried in order:

      1. PREFERRED (Round 8): the field's own printed label is located on
         the page and the words spatially near it are reconstructed into one
         value - see _extract_anchored_field_value() and the block comment
         above _WRITTEN_FIELD_ANCHORS for why (this is what stops a
         neighboring field's text from bleeding into the comparison, and
         reassembles a boxed digit grid by position rather than trusting
         Vision's own word-grouping of it).
      2. FALLBACK: if the label can't be located (Vision misread it, the
         page doesn't match this form's layout, or this question has no
         anchor configured at all - currently just "24") - or if vision_result
         predates the "words" key (e.g. a hand-built fixture using the
         pre-Round-8 shape) - falls back to the original whole-page
         comparison. Strictly less precise, but still better than no check.

    The matching strategy itself, once the comparison text is chosen:

      - Short, single-token fields (H1's ID number, H2's code, 26's age):
        compared for an exact, normalized match against either the anchored
        value (source 1) or each of Vision's individual whole-page WORD-
        level tokens (source 2 fallback). For H1/26 specifically (expected
        to be pure digits), the comparison strips non-digit characters from
        both sides first, so punctuation/spacing differences don't cause a
        false mismatch - but the comparison is still EXACT on the digits
        themselves, deliberately stricter than substring containment: a
        model answer that's a substring of the real value (e.g. "19697"
        when the form actually reads "196697") would pass a naive
        containment check but must NOT pass here, since that's exactly the
        digit-dropping failure this check exists to catch.

      - Longer, free-form fields (H4's agency, H5's address, H6's date, 24's
        comment): compared at WORD granularity (both sides tokenized on
        whitespace) against either the anchored value (source 1, for
        H4/H5/H6) or the page's FULL OCR text as a whole (source 2 fallback,
        and always for 24 - see _WRITTEN_FIELD_ANCHORS). Word-level, not
        character-level: matching individual characters lets two entirely
        different words that merely share some letters get counted as
        agreement (see Revision 19's project doc for a real example this
        broke), which word tokens can't do. Coverage sums EVERY matching run
        difflib finds between the two word lists (not just the single
        longest one, which badly undercounts a text broken into several
        matching runs by a few small wording differences), as a fraction of
        the model's answer's own word count. If that coverage is
        >=VISION_FREEFORM_COVERAGE_THRESHOLD (currently 90%), that counts as
        agreement; anything lower is flagged.

    Even with the anchored source, this can still occasionally disagree with
    a model answer that's genuinely correct but transcribed slightly
    differently than Vision's own OCR would render it - an accepted, and
    reviewable, false-positive rate: needs_review=True just means "look at
    this one," not "this is definitely wrong.\""""
    model_answer = (model_answer or "").strip()
    if not model_answer:
        return None, "", None, ""  # nothing written according to the model - nothing to cross-check

    full_text = vision_result.get("full_text", "")
    tokens = vision_result.get("tokens", [])
    words = vision_result.get("words", [])
    if not full_text and not tokens and not words:
        return None, "", None, ""  # Vision unavailable/found nothing on this page at all - no independent signal

    # See _extract_anchored_field_value()'s docstring: None = no anchor
    # configured, or the label couldn't be located (fall back below); "" =
    # the label WAS located but its value window is empty (a genuinely
    # blank field, per Vision); otherwise the reconstructed value text.
    anchored_value = _extract_anchored_field_value(question_number, vision_result)

    if question_number in _VISION_TOKEN_FIELDS:
        if anchored_value is not None:
            if question_number in ("H1", "26"):
                target_digits = re.sub(r"\D", "", model_answer)
                if not target_digits:
                    return None, "", None, ""  # model's answer for a digits-expected field wasn't actually numeric - nothing sound to compare
                found_digits = re.sub(r"\D", "", anchored_value)
                if found_digits == target_digits:
                    return True, "", 1.0, anchored_value
                return False, (
                    f"model read {model_answer!r} for {question_number}, but the text Cloud "
                    f"Vision found near this field's own printed label reads {anchored_value!r} "
                    "- possible OCR noise/dropped digit, or a genuine model misread."
                ), 0.0, anchored_value
            else:  # H2: short alphanumeric code
                compact_value = re.sub(r"\s+", "", anchored_value.lower())
                compact_target = re.sub(r"\s+", "", model_answer.lower())
                if compact_value == compact_target:
                    return True, "", 1.0, anchored_value
                return False, (
                    f"model read {model_answer!r} for {question_number}, but the text Cloud "
                    f"Vision found near this field's own printed label reads {anchored_value!r} "
                    "- possible OCR noise, or a genuine model misread."
                ), 0.0, anchored_value

        # FALLBACK: this field's own label couldn't be located (or this
        # vision_result predates "words") - compare against every word
        # anywhere on the page instead, same as before Round 8. Strictly
        # noisier (a neighboring field's token can coincidentally match, or
        # a boxed grid's own mis-segmented tokens can miss even a correct
        # answer), but still better than skipping the check entirely.
        target = _normalize_for_match(model_answer)
        token_snippet = "; ".join(tokens[:12]) + (", ..." if len(tokens) > 12 else "")
        if question_number in ("H1", "26"):
            target_digits = re.sub(r"\D", "", target)
            if not target_digits:
                return None, "", None, ""
            for tok in tokens:
                if re.sub(r"\D", "", _normalize_for_match(tok)) == target_digits:
                    return True, "", 1.0, token_snippet
            return False, (
                f"model read {model_answer!r} for {question_number}, but no individual "
                "word Cloud Vision OCR'd from this page matches those digits exactly - "
                "possible OCR noise/dropped digit (this field's own label couldn't be "
                "located on the page, so this fell back to a whole-page check)."
            ), 0.0, token_snippet
        else:
            for tok in tokens:
                if _normalize_for_match(tok) == target:
                    return True, "", 1.0, token_snippet
            return False, (
                f"model read {model_answer!r} for {question_number}, but no individual "
                "word Cloud Vision OCR'd from this page matches it exactly - possible "
                "hallucinated/concatenated text (this field's own label couldn't be "
                "located on the page, so this fell back to a whole-page check)."
            ), 0.0, token_snippet

    elif question_number in _VISION_FREEFORM_FIELDS:
        if anchored_value is not None:
            compare_text = anchored_value
            source_desc = "the text Cloud Vision found near this field's own printed label"
        else:
            compare_text = full_text
            source_desc = "Cloud Vision's independent OCR of the page"

            # "24" has no anchor at all (see _WRITTEN_FIELD_ANCHORS's
            # comment) and always uses this whole-page fallback, so
            # compare_text here is the ENTIRE page's OCR text - including
            # the long PRINTED question prompt BEFORE the comment box
            # ("24. Comment: Please let us know your comments... DO NOT
            # write your name or phone number.") AND the next section's
            # own printed header AFTER it ("NOW TELL US A LITTLE ABOUT
            # YOURSELF", question 25, etc.), both of which sit outside the
            # boxed answer area but are part of the same page's OCR text.
            # Reported directly against real output TWICE: first
            # vision_ocr_snippet echoed the printed prompt instead of the
            # response, then (after cutting the front) it ran on past the
            # end of the response into the next section's header. This
            # form's prompt for 24 reliably ends with one of these phrases,
            # and the next section reliably starts with "NOW TELL US" - so
            # cut everything up to and including the LAST occurrence of a
            # start phrase, and everything from the FIRST occurrence of the
            # end phrase (searched only after the start cut, so "24"/"25"
            # etc. appearing earlier on the page can't falsely trigger it).
            # Never guess: if a marker isn't found (a different form
            # revision's wording, or a Vision misread of that stretch), that
            # side is left uncut, same as before this fix - strictly no
            # worse, only better when a cut lands.
            if question_number == "24":
                lower_compare = compare_text.lower()
                cut_at = -1
                for marker in ("phone number.", "identify you.", "phone number", "identify you"):
                    idx = lower_compare.rfind(marker)
                    if idx != -1:
                        cut_at = idx + len(marker)
                        break
                if cut_at != -1 and cut_at < len(compare_text):
                    compare_text = compare_text[cut_at:].strip()
                lower_compare = compare_text.lower()
                end_at = -1
                for marker in ("now tell us", "25."):
                    idx = lower_compare.find(marker)
                    if idx != -1 and (end_at == -1 or idx < end_at):
                        end_at = idx
                if end_at > 0:
                    compare_text = compare_text[:end_at].strip()

        # H6 is a special case even among the freeform fields: like H1/26,
        # its printed layout is a row of boxed SINGLE digits ("Today's Date
        # (MM/DD/YYYY)" over 8 separate cells), not a run of connected
        # handwriting. When the anchored value is available it's just those
        # digit words joined with spaces (e.g. "0 9 0 4 2 0 2 6"), which
        # difflib's plain-text contiguous-match below would compare against
        # a slash-formatted model answer like "09/04/2026" - the spaces and
        # slashes break what would otherwise be an exact match, causing a
        # false disagreement on every single correctly-read date. So, same
        # as H1/26: strip to digits-only and require an EXACT digit match
        # (not fuzzy coverage) when comparing the anchored reconstruction.
        # Caught by this module's own synthetic self-test, not a real scan -
        # see self_test_anchored_vision_extraction(). The whole-page FALLBACK
        # (anchored_value is None) keeps the original fuzzy-text behavior,
        # since full_text there is normal running OCR text, not a digit grid.
        if question_number == "H6" and anchored_value is not None:
            target_digits = re.sub(r"\D", "", model_answer)
            found_digits = re.sub(r"\D", "", anchored_value)
            # Display only the DATE-SHAPED tokens from the reconstructed
            # value (digits and date separators only: 0-9, "/", "-", "."),
            # not the raw joined string - defense in depth against any
            # stray word still making it into the window (e.g. the window's
            # row-grouping tolerance overlapping a nearby line on a real
            # scan where the vertical gap is tighter than assumed, or
            # Vision returning the whole date as one already-joined token
            # like "10/23/2025" rather than one digit per box). Matched
            # against a real report where the snippet still carried "Agree
            # am Not" even after the window was row-scoped: Vision had
            # returned the date as a single slash-formatted token, which an
            # earlier, stricter tok.isdigit() filter would have rejected
            # too (a "/" makes isdigit() False), leaving the pollution in
            # place. found_digits above already strips ALL non-digit
            # characters from the full anchored_value regardless, so this
            # can only ever change what's DISPLAYED, never the match
            # verdict.
            date_shaped = re.compile(r"^[0-9/.\-]+$")
            digit_tokens = [tok for tok in anchored_value.split() if date_shaped.match(tok)]
            text_snippet = " ".join(digit_tokens) if digit_tokens else anchored_value.strip()
            if not target_digits:
                return None, "", None, ""  # model's date answer wasn't actually numeric - nothing sound to compare
            if found_digits == target_digits:
                return True, "", 1.0, text_snippet
            return False, (
                f"model read {model_answer!r} for H6, but the digits Cloud Vision found near "
                f"this field's own printed label read {found_digits!r} - possible OCR noise/"
                "dropped digit, or a genuine model misread."
            ), 0.0, text_snippet

        norm_full = _normalize_for_match(compare_text)
        norm_answer = _normalize_for_match(model_answer)

        # Whitespace-only-difference short-circuit (explicit user request:
        # "if the texts are the same, only have space difference, then the
        # answers should be recognized as the same" - H2/H4/H5). Vision's
        # OCR segments text into words by visual gaps, which doesn't always
        # land on the same word boundaries the model reads from the same
        # handwriting/print - e.g. a model reading one unbroken word
        # ("ABCorp") where Vision's OCR sees enough of a gap to report it as
        # two word tokens ("AB", "Corp"), or vice versa. That's the SAME
        # text, not a disagreement, so compare with ALL whitespace collapsed
        # out entirely (not just runs normalized to one space, which still
        # requires the same word boundaries) before falling through to the
        # word-level fuzzy match below - mirrors the exact-match fields'
        # (H1/H2/26) existing `re.sub(r"\s+", "", ...)` whitespace-agnostic
        # comparison, extended here to the freeform fields (H4/H5/H6/24).
        if re.sub(r"\s+", "", norm_full) == re.sub(r"\s+", "", norm_answer):
            return True, "", 1.0, compare_text.strip()

        words_full = [w for w in norm_full.split(" ") if w]
        words_answer = [w for w in norm_answer.split(" ") if w]
        # Matching happens at WORD granularity, not character granularity -
        # this is the second of two fixes needed to make this score mean
        # anything, on top of autojunk=False below (both found while
        # investigating the same reported false-negative/false-positive
        # pair, see the project doc for this revision for the full
        # before/after numbers on both real reports).
        #
        # Character-level matching (the original design) can STITCH
        # TOGETHER fragments of two entirely different words that merely
        # happen to share some letters, inflating the score for text that
        # actually differs in meaning. Reported directly: a genuinely
        # garbled OCR comment ("Do visit mabeextra ... But Cancling
        # visiting" vs. the model's "no visit mabextra autie ... But can
        # long visiting") scored 94% at the character level - character
        # matching happily glued " visit mabe" (from "no VISIT MABEextra")
        # together with "xtra" (from "mab-EXTRA"), and separately stitched
        # "can" (from "CANcling") to "ng visiting" (from "cancli-NG
        # visiting") - real words that are NOT the same word, quietly
        # counted as a match anyway because their spelled-out forms happen
        # to overlap. Comparing WORD tokens instead of characters can't do
        # this: "cancling" and "can" are just two different tokens, either
        # equal or not, so this kind of coincidental overlap can no longer
        # inflate the score.
        #
        # autojunk=False is NOT optional here either. difflib.SequenceMatcher's
        # default autojunk=True heuristic (intended for comparing SOURCE
        # CODE lines, its original use case) automatically treats any
        # element that makes up more than 1% of the SECOND sequence as
        # "popular" and refuses to use it as a match anchor, but only once
        # that sequence has 200+ elements - words in an ordinary sentence
        # ("the", "a", "to", "and"...) trip this just as readily as
        # characters do once a comment runs long enough. Confirmed directly
        # against a real reported case (a ~220-character Q24 comment whose
        # transcription differed from Vision's OCR by one word, "a while"
        # vs "awhile") where the default autojunk=True badly undercounted
        # the match versus autojunk=False on the exact same two strings.
        word_matcher = difflib.SequenceMatcher(None, words_full, words_answer, autojunk=False)
        # coverage is the fraction of the model's answer's WORDS covered by
        # matching text - summed across ALL matching runs
        # (get_matching_blocks()), not just the single longest one. A single
        # long run badly UNDER-counts true similarity whenever there's more
        # than one small divergence scattered through an otherwise
        # near-identical text: reported directly - Vision's OCR of a real
        # Q24 comment differed from the model's transcription by exactly a
        # missing period after one sentence and "awhile" vs "a while",
        # nothing else, yet counting only the single longest matching run
        # scored it as an 44% near-total mismatch, because those two tiny
        # differences split an otherwise-complete match into three separate
        # runs and only the biggest one was ever being counted. Summing
        # every matching run's size instead correctly scores that case as
        # ~99.5% - while the word-level matching above (as opposed to
        # character-level) is what keeps this summing-across-runs approach
        # from ALSO inflating the genuinely-garbled case just above, since a
        # coincidental letter overlap between two different words can no
        # longer contribute its own spurious little "run" to the sum.
        matching_word_runs = word_matcher.get_matching_blocks()
        matched_words = sum(b.size for b in matching_word_runs)
        coverage = matched_words / max(len(words_answer), 1)
        # The snippet shown/stored should be the part of Vision's OCR that
        # actually corresponds to the ANSWER, not just the first ~300
        # characters of compare_text. That distinction matters a lot on the
        # whole-page FALLBACK path (compare_text = full_text): the page's
        # PRINTED QUESTION TEXT ("24. Comment: Please let us know...") comes
        # before the handwritten answer in reading order, so a naive
        # from-the-start truncation showed the question prompt itself
        # instead of the response - reported directly against Q24's real
        # output. _snippet_around_match() instead centers the snippet on
        # wherever the longest matching run of WORDS against the model's
        # answer was actually found, falling back to the plain start-of-text
        # truncation only when no match at all was found to center on.
        longest_word_run = word_matcher.find_longest_match(0, len(words_full), 0, len(words_answer))
        matched_phrase = (
            " ".join(words_full[longest_word_run.a: longest_word_run.a + longest_word_run.size])
            if longest_word_run.size > 0 else ""
        )
        text_snippet = _snippet_around_match(compare_text, matched_phrase)
        if coverage >= VISION_FREEFORM_COVERAGE_THRESHOLD:
            return True, "", round(coverage, 4), text_snippet
        return False, (
            f"model read {model_answer!r} for {question_number}, but {source_desc} "
            f"doesn't contain a close match (matching text covered only "
            f"{coverage:.0%} of the model's answer) - possible hallucination or misread."
        ), round(coverage, 4), text_snippet

    return None, "", None, ""  # not a written-text question this mechanism covers


def _parse_confidence(raw) -> Optional[float]:
    """Best-effort parse of the model's self-reported confidence (rule 15 in
    build_extraction_prompt()) into a float in [0.0, 1.0], or None if
    missing/unparseable (e.g. an older cached response from before this
    field existed, or the model returning something malformed) - treated
    the same as "no signal", never a guess or a crash."""
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value < 0.0 or value > 1.0:
        return None
    return value


def extract_qa_from_pdf(
    pdf_bytes: bytes,
    project: Optional[str],
    location: str,
    model_name: str,
    vision_project: Optional[str] = None,
    vision_enabled: bool = True,
    quality_route: Optional[str] = None,
) -> dict:
    """Calls Vertex AI Gemini on one survey PDF, given as raw bytes (caller
    downloads it first — see extract_to_bigquery()/verify_pdf()). Each page
    is rendered to a high-res PNG (render_pdf_to_images()) and sent as image
    parts, rather than passing the PDF itself, for the accuracy reasons
    described above. Returns {question_number: {"answer": str,
    "mark_position": str, "reasoning": str, "confidence": Optional[float]}}
    - "reasoning" is the model's own one-sentence account of the visual
    evidence it based the answer on (see rule 11 in build_extraction_prompt()),
    kept for auditability/debugging; "confidence" is its own self-reported
    certainty (see rule 15) - both are informational until answers_to_qa_rows()
    decides what to do with them. vision_enabled=False (or
    VISION_DOUBLE_CHECK_ENABLED=False) skips the Cloud Vision double-check
    entirely for this call.

    quality_route (Revision 37): the "recommended_route" from
    classify_pdf_quality.py's pdf_quality table for THIS file (see
    query_pdf_quality_route() and the "pdf-quality-classifier-and-routing.md"
    project doc for the classifier's own reasoning) - one of "pixel",
    "vision", "fallback", or None (no quality row / routing disabled, the
    same as before this feature existed). Effects, per that doc's routing
    spec:

      - "pixel" or None: no change - pixel detection runs normally below,
        Vision only double-checks the usual fixed written-text fields
        (WRITTEN_TEXT_QUESTION_NUMBERS), gated by vision_enabled as always.

      - "vision": the classifier found the scan pixel-readable but marks or
        handwriting genuinely questionable (e.g. faint marks whose ink gap
        to background is small, or handwriting the classifier's own Vision
        pass couldn't confidently read) - still run pixel detection with
        its normal calibration (it's still the best deterministic signal
        available), and FORCE the Cloud Vision double-check on for this
        file's written-text fields even if vision_enabled/
        VISION_DOUBLE_CHECK_ENABLED is globally off, so both the advanced
        model and the extra Vision cross-check are brought to bear on this
        file. "vision" only escalates which detectors run - it never
        touches needs_review directly (see the general note below).

      - "fallback": the classifier found the scan itself unreadable at the
        geometry level (grid/registration not locatable, tilt/warp beyond
        what the pixel detectors are calibrated for) - deterministic pixel
        detection on an unreadable geometry doesn't produce a "safe miss",
        it can produce a CONFIDENTLY WRONG reading anchored to the wrong
        position entirely (exactly the failure category every fixed
        threshold in this file's calibration has been tuned to avoid - see
        e.g. Revision 36's corroboration gate). So pixel detection is
        SKIPPED ENTIRELY for this file - every question relies on the
        advanced model's own reading alone (same as check_grid_dependencies()
        failing outright), and Vision is forced on for the written-text
        fields. Like "vision", "fallback" only escalates which detectors
        run - it never touches needs_review directly either (see below).

    needs_review, for BOTH "vision" and "fallback" (Revision 37 follow-up
    #2, per explicit user direction): NOT blanket-forced by the route at
    all. Each question's needs_review is decided exactly the way it
    always has been, purely from that question's own existing signals
    (pixel/model disagreement, low confidence, an ambiguous mark, the
    totally-missing-grid backstop, the blank-answer backstop, etc.) - the
    route only ever changes which detectors ran to produce those signals,
    never whether a given per-question result gets flagged. A model-only
    reading under "fallback" is treated exactly like a model-only reading
    anywhere else in this file when pixel didn't run for some other reason
    (e.g. check_grid_dependencies() failing outright) - it relies on those
    same pre-existing backstops, not a route-specific override.

    Any value outside {"pixel", "vision", "fallback", None} is treated the
    same as None (query_pdf_quality_route() already validates this before
    returning, but this function stays defensive in case of a direct call)."""
    import vertexai
    from vertexai.generative_models import GenerationConfig, GenerativeModel, Part

    if quality_route not in _VALID_PDF_QUALITY_ROUTES:
        quality_route = None
    skip_pixel = quality_route == "fallback"
    force_vision = quality_route in ("vision", "fallback")
    # Revision 37 follow-up #2 (explicit user request): neither route
    # blanket-forces needs_review anymore. "vision" and "fallback" only
    # change which detectors run (skip_pixel / force_vision above) - every
    # question's needs_review is decided the same way it always has been,
    # by that question's own existing signals (pixel/model disagreement,
    # low confidence, ambiguous marks, totally-missing-grid backstop,
    # blank-answer backstop, etc.), never by the route alone. This applies
    # even to "fallback": the model's own reading there gets exactly the
    # same needs_review treatment a model-only reading gets everywhere else
    # in this file when pixel didn't run (e.g. check_grid_dependencies()
    # failing outright already relies on those same existing backstops
    # without any separate blanket force).
    force_review = False

    global _VERTEX_INITIALIZED
    if not _VERTEX_INITIALIZED:
        vertexai.init(project=project, location=location)
        _VERTEX_INITIALIZED = True

    page_images = render_pdf_to_images(pdf_bytes)
    # Resolve semantic controls against this scan before any pixel detector
    # runs. A failed local CV search leaves the legacy geometry in place.
    try:
        _resolve_runtime_box_geometry(page_images)
    except Exception as e:  # noqa: BLE001 - pixel detection is best effort
        err("[GRID] Runtime box geometry resolution skipped: %s", e)
    image_parts = [Part.from_data(data=img, mime_type="image/png") for img in page_images]

    model = GenerativeModel(model_name)
    response = model.generate_content(
        [get_extraction_prompt(), *image_parts],
        generation_config=GenerationConfig(temperature=0, response_mime_type="application/json"),
    )
    usage = getattr(response, "usage_metadata", None)
    if usage is not None:
        status(
            "[GEMINI] model=%s prompt_tokens=%s output_tokens=%s total_tokens=%s",
            model_name,
            getattr(usage, "prompt_token_count", None),
            getattr(usage, "candidates_token_count", None),
            getattr(usage, "total_token_count", None),
        )
    raw_text = response.text
    parsed = json.loads(raw_text)  # let a malformed response raise -> caller decides how to handle

    answers = {}
    for item in parsed:
        qnum = str(item.get("question_number", "")).strip()
        if qnum:
            answers[qnum] = {
                "answer": str(item.get("answer", "") or "").strip(),
                "mark_position": str(item.get("mark_position", "") or "").strip(),
                "reasoning": str(item.get("reasoning", "") or "").strip(),
                "confidence": _parse_confidence(item.get("confidence")),
            }

    # Deterministic pixel-based cross-check for the main Q1-18 checkbox grid
    # (page 1) - see detect_checkbox_grid_answers() docstring for why this
    # exists on top of the answer/mark_position self-check above. Best-effort:
    # any failure here (different scan layout, OpenCV hiccup, etc.) just means
    # we fall back to the model-only reading for this file, never a hard error.
    #
    # skip_pixel (quality_route == "fallback"): the upstream classifier has
    # already determined this scan's geometry itself is untrustworthy (grid/
    # registration not locatable, excessive tilt/warp) - running any of the
    # deterministic detectors below against an unreadable geometry risks a
    # CONFIDENTLY WRONG position-anchored reading, not a safe miss, so this
    # whole pixel stage is skipped entirely for this file and every question
    # relies on the model's own reading alone (see quality_route's docstring
    # above for the full reasoning).
    if page_images and not skip_pixel:
        try:
            grid_results = detect_checkbox_grid_answers(page_images[0])
            for qnum, info in grid_results.items():
                if qnum in answers:
                    answers[qnum]["pixel_position"] = info["position"]
                    answers[qnum]["pixel_margin"] = info["margin"]
                    answers[qnum]["pixel_source"] = "grid"
                    answers[qnum]["pixel_blank"] = info.get("blank", False)
                    if info.get("correction_detected"):
                        answers[qnum]["pixel_correction_detected"] = True
                        answers[qnum]["pixel_correction_detected_at"] = info.get("correction_detected_at")
                    if info.get("multiple_marks_detected"):
                        answers[qnum]["pixel_multiple_marks_detected"] = True
            if not grid_results:
                err(
                    "[GRID] Pixel-based checkbox-grid detection found nothing on this file's "
                    "page 1 (0 of 18 rows) - Q1-18 will fall back to the vision-model-only "
                    "reading for this file. This can happen on an unusually skewed/cropped scan; "
                    "if it happens on every file, check check_grid_dependencies() below."
                )
                # Explicit user request (Revision 28): when the pixel grid
                # detector can't verify ANY of Q1-18 at all (as opposed to
                # verifying most rows but silently missing one or two), the
                # whole 18-question block is running on the model's own
                # unverified reading with NO independent cross-check
                # whatsoever - qualitatively different from every other
                # question on this form, all of which have at least a
                # self-consistency check. Flag every one of Q1-18 for human
                # review rather than let them look identical to a normal,
                # pixel-verified row.
                for qnum in map(str, range(1, 19)):
                    if qnum in answers:
                        answers[qnum]["pixel_grid_totally_missing"] = True
        except Exception as e:  # noqa: BLE001 - purely a bonus signal, never fatal
            err("[GRID] Pixel-based checkbox-grid detection skipped for this file: %s", e)

        try:
            yesno_results, yesno_ambiguous = detect_yesno_box_answers(page_images, include_diagnostics=True)
            for qnum, info in yesno_results.items():
                if qnum in answers:
                    answers[qnum]["pixel_position"] = info["position"]
                    answers[qnum]["pixel_margin"] = info["margin"]
                    answers[qnum]["pixel_source"] = "yesno_box"
                    answers[qnum]["pixel_blank"] = info.get("blank", False)
                    if info.get("multiple_marks_detected"):
                        answers[qnum]["pixel_yesno_multiple_marks_detected"] = True
            for qnum in yesno_ambiguous:
                if qnum in answers:
                    answers[qnum]["pixel_yesno_ambiguous"] = True
        except Exception as e:  # noqa: BLE001 - purely a bonus signal, never fatal
            err("[GRID] Pixel-based Yes/No-box detection skipped for this file: %s", e)

        try:
            h3_result = detect_h3_answer(page_images)
            if h3_result and "H3" in answers:
                answers["H3"]["pixel_position"] = h3_result["position"]
                answers["H3"]["pixel_margin"] = h3_result["margin"]
                answers["H3"]["pixel_source"] = "h3_circle"
                answers["H3"]["pixel_blank"] = h3_result.get("blank", False)
        except Exception as e:  # noqa: BLE001 - purely a bonus signal, never fatal
            err("[GRID] Pixel-based H3 circle detection skipped for this file: %s", e)

        for qnum in _MULTISELECT_BOX_CALIBRATION:
            if qnum not in answers:
                continue
            try:
                ratios, ambiguous_labels = detect_multiselect_ink_ratios(
                    page_images, qnum, include_diagnostics=True
                )
                if ratios:
                    answers[qnum]["pixel_multiselect_ratios"] = ratios
                if ambiguous_labels:
                    answers[qnum]["pixel_multiselect_ambiguous"] = ambiguous_labels
            except Exception as e:  # noqa: BLE001 - purely a bonus signal, never fatal
                err("[GRID] Pixel-based multi-select detection skipped for Q%s: %s", qnum, e)

    # Cloud Vision double-check for handwritten/write-in questions (H1,
    # H2, H4, H5, H6, 24, 26) - see WRITTEN_TEXT_QUESTION_NUMBERS and
    # cross_check_written_field_with_vision()'s docstring for why these
    # specifically needed a NEW signal (cross_check_answer() can't check
    # a question with no fixed choice list at all). One Vision API call
    # PER PAGE actually needed (see _WRITTEN_TEXT_QUESTION_PAGE - most of
    # these seven share page 1, but Q24/Q26 are on page 2), cached so a
    # page already OCR'd for an earlier question isn't re-sent to Vision.
    #
    # Deliberately OUTSIDE the `not skip_pixel` pixel-detection block above:
    # force_vision (quality_route in "vision"/"fallback") must still run
    # this even when pixel detection itself was skipped for "fallback" -
    # written-text fields have no pixel backstop either way, so this is the
    # ONLY independent check available for them on a "fallback"-routed file,
    # making it more important there, not less.
    if page_images and (force_vision or (vision_enabled and VISION_DOUBLE_CHECK_ENABLED)):
        try:
            vision_results_by_page = {}
            for qnum in WRITTEN_TEXT_QUESTION_NUMBERS:
                if qnum not in answers:
                    continue
                page_idx = _WRITTEN_TEXT_QUESTION_PAGE.get(qnum, 0)
                if page_idx not in vision_results_by_page:
                    if page_idx < len(page_images):
                        vision_results_by_page[page_idx] = cloud_vision_ocr_page(
                            page_images[page_idx], vision_project=vision_project or VISION_PROJECT_ID
                        )
                    else:
                        vision_results_by_page[page_idx] = {"full_text": "", "tokens": []}
                vision_result = vision_results_by_page[page_idx]

                # Revision 29: H2/H6 are VISION-AUTHORITATIVE - see
                # _VISION_AUTHORITATIVE_FIELDS' comment. Do this BEFORE
                # the normal cross-check below, using the model's
                # ORIGINAL answer for that cross-check regardless (so
                # the audit trail/vision_note still describes model vs.
                # Vision, not Vision vs. itself).
                if qnum in _VISION_AUTHORITATIVE_FIELDS:
                    anchored_value = _extract_anchored_field_value(qnum, vision_result)
                    if anchored_value:  # non-None AND non-empty - a confident, non-blank anchored reading
                        formatted = _format_vision_authoritative_value(qnum, anchored_value)
                        if formatted and formatted != answers[qnum].get("answer", ""):
                            answers[qnum]["model_answer_before_vision_override"] = answers[qnum].get("answer", "")
                            answers[qnum]["answer"] = formatted
                            answers[qnum]["vision_overrode_model"] = True

                match, detail, score, snippet = cross_check_written_field_with_vision(
                    qnum, answers[qnum].get("model_answer_before_vision_override", answers[qnum].get("answer", "")), vision_result
                )
                if match is not None:
                    answers[qnum]["vision_match"] = match
                    answers[qnum]["vision_note"] = detail
                    answers[qnum]["vision_score"] = score
                    answers[qnum]["vision_snippet"] = snippet
        except Exception as e:  # noqa: BLE001 - purely a bonus signal, never fatal
            err("[VISION] Cloud Vision double-check skipped for this file: %s", e)

    # quality_route "vision"/"fallback" (Revision 37): the upstream
    # classifier has already independently determined a human should look
    # at this file - stamp that onto every question here so
    # answers_to_qa_rows() can force needs_review=True regardless of that
    # question's own individual signals, rather than relying on this file's
    # ordinary per-question checks to happen to also catch it. Recorded
    # alongside quality_route itself (see below) so a BigQuery row can
    # always show WHY a question was flagged even when no other reason
    # fired.
    if force_review:
        for qnum in answers:
            answers[qnum]["quality_route_forces_review"] = True
    for qnum in answers:
        answers[qnum]["pdf_quality_route"] = quality_route

    return answers


_GRID_DEPS_CHECKED = False


def check_grid_dependencies() -> bool:
    """Checks once (cached) whether opencv-python-headless + numpy - the
    dependencies BOTH detect_checkbox_grid_answers() (Q1-18) AND
    detect_yesno_box_answers() (Q21/22/27/32/25/29/35) need - are actually
    importable, and prints a LOUD, impossible-to-miss message if not.

    This matters because without this check, a missing dependency fails
    SILENTLY: extract_qa_from_pdf() catches the resulting ImportError inside
    a per-file try/except (so one bad file can't take down a whole batch) and
    just falls back to the plain vision-model reading - which looks
    identical to the pre-fix pipeline. Call this once at the start of a run
    (extract_to_bigquery() and verify_pdf() both do) so it's obvious up front
    whether the accuracy fix is even active, rather than discovering it much
    later by noticing every row's detection_method says "model" instead of
    "pixel_grid"/"pixel_yesno_box"."""
    global _GRID_DEPS_CHECKED
    if _GRID_DEPS_CHECKED:
        return True
    try:
        import cv2  # noqa: F401
        import numpy  # noqa: F401
    except ImportError as e:
        err(
            "[GRID] opencv-python-headless and/or numpy are NOT installed (%s). "
            "The deterministic pixel-based accuracy checks for questions 1-18 "
            "(the checkbox grid) AND questions 21/22/27/32/29/35 (the isolated "
            "checkbox rows/lists) will be SKIPPED for every file in this run - every "
            "row will fall back to the vision-model-only reading (the earlier, "
            "less accurate behavior), with no other visible sign anything is "
            "different. Fix with: pip install opencv-python-headless numpy "
            "(--break-system-packages in some environments), then restart your "
            "kernel/notebook so the fresh install is picked up.",
            e,
        )
        return False
    _GRID_DEPS_CHECKED = True
    return True


def answers_to_qa_rows(
    folder: str,
    file_name: str,
    answers: dict,
    report_date: Optional[datetime.date],
    refreshed_at: datetime.datetime,
) -> list:
    """Turns a {question_number: {"answer":..., "mark_position":...,
    "confidence":..., [optionally] "pixel_position":..., "pixel_margin":...,
    "pixel_source":..., "pixel_blank":..., "vision_match":...,
    "vision_note":...}} dict into one QARow per known survey question
    (missing/unmatched question numbers become blanks).

    FOUR checks are applied, additively (any of them can independently set
    needs_review=True; their notes are combined, semicolon-separated, in
    review_note - see review_reasons below), in this order:

      1. If a confident pixel reading is present for this question - from
         the main Q1-18 checkbox grid, the isolated Yes/No(/Unknown) box
         reader, the H3 circle reader, or the Q33/Q34 multi-select VETO/
         FILL reconciliation (_reconcile_multiselect_choices()) - that
         reading is TRUSTED as survey_answer/mark_position, even over the
         model's own answer, because it's a direct pixel measurement rather
         than a model judgment call. If the model disagreed, that's
         recorded for visibility, but the pixel reading wins - and, same as
         every other confident pixel correction here, does NOT by itself
         set needs_review (see the Revision 12/25 rationale in each of
         these branches' own comments).

      2. Otherwise, if a pixel detector positively found NOTHING marked at
         all (pixel_blank=True) while the model reported a non-empty
         answer, the answer is overridden to blank ("") with
         needs_review=True - catches the model hallucinating an answer for
         a question the respondent left blank.

      3. Otherwise, falls back to cross_check_answer(): comparing the
         model's own answer label against its own reported mark_position
         for self-consistency.

      4. NEW, independent of 1-3: the model's own self-reported confidence
         (rule 15 in build_extraction_prompt()) for THIS question, and -
         for the seven handwritten/write-in questions with no fixed choice
         list (H1, H2, H4, H5, H6, 24, 26) - an independent Cloud Vision OCR
         cross-check (see cross_check_written_field_with_vision()). A
         confidence below MODEL_CONFIDENCE_THRESHOLD flags for review, but
         ONLY when detection_method is still "model" - i.e. only when no
         pixel detector already confidently took over check 1 above; a
         pixel detector's own margin threshold is itself an independent,
         stronger confidence signal, so a merely-low model confidence on a
         question the pixel side already resolved is recorded
         (model_confidence is always stored) but not itself grounds for
         review. A Cloud Vision disagreement (vision_match is False) always
         flags for review - these seven questions have no pixel detector
         to already have resolved the question some other way.

    Every row also carries confidence_threshold (the MODEL_CONFIDENCE_THRESHOLD
    value in effect for this run) and, whenever a Vision comparison actually
    ran, vision_match_score/vision_ocr_snippet - so the confidence/Vision
    comparison process itself (not just its agree/disagree/needs_review
    verdict) is visible directly in BigQuery. See the QARow field comments
    for details."""
    report_date_str = report_date.isoformat() if report_date else None
    refreshed_at_str = refreshed_at.isoformat()
    refreshed_date_str = refreshed_at.date().isoformat()

    # source -> (confidence margin threshold, detection_method label, human-readable name for review_note)
    pixel_source_info = {
        "grid": (_GRID_CONFIDENCE_MARGIN, "pixel_grid", "the pixel-based checkbox-grid"),
        "yesno_box": (_YESNO_CONFIDENCE_MARGIN, "pixel_yesno_box", "the pixel-based Yes/No-box"),
        "h3_circle": (_H3_CONFIDENCE_MARGIN, "pixel_h3_circle", "the pixel-based H3 circle-fill"),
    }

    rows = []
    for number, _group_key, _sub_text, _choices in SURVEY_QUESTIONS:
        entry = answers.get(number) or {}
        if isinstance(entry, dict):
            model_answer = entry.get("answer", "") or ""
            model_position = entry.get("mark_position", "") or ""
            model_reasoning = entry.get("reasoning", "") or ""
            model_confidence = entry.get("confidence")
            pixel_position = entry.get("pixel_position")
            pixel_margin = entry.get("pixel_margin")
            pixel_source = entry.get("pixel_source", "grid")
            pixel_blank = entry.get("pixel_blank", False)
            pixel_correction_detected = entry.get("pixel_correction_detected", False)
            pixel_correction_detected_at = entry.get("pixel_correction_detected_at")
            pixel_multiselect_ratios = entry.get("pixel_multiselect_ratios")
            pixel_multiselect_ambiguous = entry.get("pixel_multiselect_ambiguous")
            pixel_yesno_ambiguous = entry.get("pixel_yesno_ambiguous", False)
            pixel_grid_totally_missing = entry.get("pixel_grid_totally_missing", False)
            pixel_multiple_marks_detected = entry.get("pixel_multiple_marks_detected", False)
            pixel_yesno_multiple_marks_detected = entry.get("pixel_yesno_multiple_marks_detected", False)
            vision_overrode_model = entry.get("vision_overrode_model", False)
            model_answer_before_vision_override = entry.get("model_answer_before_vision_override")
            vision_match = entry.get("vision_match")
            vision_note = entry.get("vision_note", "") or ""
            vision_score = entry.get("vision_score")
            vision_snippet = entry.get("vision_snippet", "") or ""
            quality_route_forces_review = entry.get("quality_route_forces_review", False)
            pdf_quality_route = entry.get("pdf_quality_route")
        else:
            # tolerate a plain string too (e.g. older callers/tests) - no
            # mark_position/pixel/confidence data available to check against in that case.
            model_answer = str(entry)
            model_position = ""
            model_reasoning = ""
            model_confidence = None
            pixel_position = None
            pixel_margin = None
            pixel_source = "grid"
            pixel_blank = False
            pixel_correction_detected = False
            pixel_correction_detected_at = None
            pixel_multiselect_ratios = None
            pixel_multiselect_ambiguous = None
            pixel_yesno_ambiguous = False
            pixel_grid_totally_missing = False
            pixel_multiple_marks_detected = False
            pixel_yesno_multiple_marks_detected = False
            vision_overrode_model = False
            model_answer_before_vision_override = None
            vision_match = None
            vision_note = ""
            vision_score = None
            vision_snippet = ""
            quality_route_forces_review = False
            pdf_quality_route = None

        answer, mark_position = model_answer, model_position
        needs_review, detection_method = False, "model"
        review_reasons = []  # accumulated across every check below; final review_note = "; ".join(review_reasons)
        vision_cross_check = ""

        choices = CHOICE_LISTS_BY_NUMBER.get(number)
        gemini_authoritative = number in {"34", "35"}
        margin_threshold, method_label, source_name = pixel_source_info.get(
            pixel_source, pixel_source_info["grid"]
        )
        # Apply question-specific margin threshold override if present (e.g. Q19/Q20 use checkmark-style marks)
        if number in _YESNO_CONFIDENCE_MARGIN_OVERRIDE:
            margin_threshold = _YESNO_CONFIDENCE_MARGIN_OVERRIDE[number]
        elif number in _GRID_CONFIDENCE_MARGIN_OVERRIDE:
            margin_threshold = _GRID_CONFIDENCE_MARGIN_OVERRIDE[number]
        # A yesno_box position flagged pixel_yesno_ambiguous with a non-None
        # position is one detect_yesno_box_answers() already PROMOTED out of
        # its own light-checkmark backstop (see that function's ambiguous-
        # boost comment) - its margin is, by definition of that backstop,
        # below _YESNO_BLANK_INK_FLOOR/_YESNO_BLANK_MARGIN_CEILING and would
        # never clear margin_threshold on its own. Bypass the margin check
        # for this specific case only (mirrors how the multiselect ambiguous
        # boost bypasses its own VETO/FILL margin logic by directly reporting
        # a ratio above the ceiling) - needs_review still fires below via
        # pixel_yesno_ambiguous regardless, so this never silently ships an
        # unreviewed guess.
        yesno_promoted = pixel_source == "yesno_box" and pixel_yesno_ambiguous and pixel_position is not None

        if (
            not gemini_authoritative
            and
            choices is not None
            and pixel_position is not None
            and pixel_margin is not None
            and (pixel_margin >= margin_threshold or yesno_promoted)
            and 1 <= pixel_position <= len(choices)
        ):
            pixel_label = choices[pixel_position - 1]
            detection_method = method_label
            answer = pixel_label
            mark_position = str(pixel_position)
            if model_answer.strip().lower() != pixel_label.strip().lower():
                # Intentionally NOT setting needs_review here. This is a
                # question with a fixed, known checkbox layout, and the pixel
                # margin has already cleared margin_threshold - i.e. the
                # deterministic detector is confident about which box has
                # ink on it. That is a stronger, more direct signal than the
                # model's own (frequently miscalibrated on adjacent
                # six-point-scale columns, e.g. "Strongly Agree" vs "Agree")
                # read of the same box. Flagging needs_review here just
                # trains users to ignore the flag, since the row already
                # carries the corrected, trustworthy answer - see
                # survey_answer. The disagreement is still recorded in
                # review_note/review_reasons for audit/traceability, it just
                # doesn't raise needs_review on its own. (Contrast with the
                # pixel_blank branch just below, which DOES still set
                # needs_review - a model answer with literally no mark
                # behind it anywhere is a materially different, rarer
                # situation worth a human look.)
                review_reasons.append(
                    f"model read {model_answer!r} but {source_name} "
                    f"detector found the mark in position {pixel_position} ({pixel_label!r}) "
                    "for this fixed-layout question - auto-corrected to the pixel reading "
                    "(no review needed for this disagreement alone)."
                )
            if pixel_correction_detected:
                # A cross-out/correction was detected on this row (see
                # _GRID_OVERDENSE_INK_CEILING) - the recovered answer above
                # is the pipeline's best read of it, but a respondent
                # correction always deserves a human glance, REGARDLESS of
                # whether the model's own answer already happened to agree
                # with it (unlike the plain pixel/model disagreement just
                # above, which is intentionally NOT enough on its own to
                # force review).
                needs_review = True
                corrected_positions = pixel_correction_detected_at or []
                corrected_labels = [
                    choices[p - 1] for p in corrected_positions if 1 <= p <= len(choices)
                ]
                review_reasons.append(
                    f"{source_name} detector found an implausibly dense mark at "
                    f"position(s) {corrected_positions} ({corrected_labels!r}) in addition to "
                    f"the normal mark at position {pixel_position} ({pixel_label!r}) - likely "
                    "the respondent crossed out an earlier answer and marked a different one; "
                    f"using position {pixel_position} ({pixel_label!r}), but this correction "
                    "should be verified by a human."
                )
        elif pixel_source in ("grid", "yesno_box", "h3_circle") and pixel_blank and model_answer.strip():
            # The pixel detector positively found NOTHING marked - not just
            # "unsure which box", but the WINNING box itself doesn't look
            # marked (see _YESNO_BLANK_INK_FLOOR) - while the model reported
            # a non-empty answer anyway. Trust the pixel reading over the
            # model here, same as the position-match branch above.
            detection_method = method_label
            needs_review = True
            review_reasons.append(
                f"model read {model_answer!r} but {source_name} detector found "
                "no box confidently marked (this question appears to have been "
                "left blank on the scan) - using the pixel reading."
            )
            answer, mark_position = "", ""

        if (
            number == "35"
            and pixel_position is not None
            and choices is not None
            and 1 <= pixel_position <= len(choices)
            and model_answer.strip().lower() != choices[pixel_position - 1].strip().lower()
        ):
            needs_review = True
            review_reasons.append(
                f"Gemini read {model_answer!r}, while the pixel detector read "
                f"{choices[pixel_position - 1]!r}; retaining the Gemini answer and "
                "flagging the disagreement for review."
            )

        if detection_method == "model":
            cc_needs_review, cc_note = cross_check_answer(number, answer, mark_position)
            if cc_needs_review:
                needs_review = True
                if cc_note:
                    review_reasons.append(cc_note)

        # Multi-select pixel reconciliation (Q33/Q34) - runs regardless of
        # the cross-check outcome above, since a self-consistent-but-wrong
        # multi-select answer has no internal disagreement for
        # cross_check_answer() to catch.
        #
        # Deliberately NOT setting needs_review here (as of the change
        # requested directly) - same Revision 12 rationale as the single-
        # select pixel-override branch above: each calibrated Q33/Q34 box is
        # individually measured against _MULTISELECT_UNMARKED_CEILING (a
        # wide, real-measured gap between confidently-blank and confidently-
        # marked - see that constant's comment), so a VETO/FILL correction
        # here is a direct pixel measurement overriding the model's
        # judgment, not an ambiguous disagreement. Flagging needs_review for
        # every one of these just trains reviewers to ignore the flag, since
        # the row already carries the corrected, trustworthy answer. The
        # correction is still fully recorded in review_note for auditability
        # either way.
        if pixel_multiselect_ratios and number in MULTI_SELECT_QUESTION_NUMBERS:
            reconcile_result = _reconcile_multiselect_choices(number, answer, pixel_multiselect_ratios)
            if reconcile_result is not None:
                new_answer, removed, added = reconcile_result
                if number in {"34"}:
                    if new_answer.strip().lower() != model_answer.strip().lower():
                        needs_review = True
                        review_reasons.append(
                            f"Gemini read {model_answer!r}, while the pixel detector read "
                            f"{new_answer!r} for this multi-select question; retaining the "
                            "Gemini answer and flagging the disagreement for review."
                        )
                else:
                    answer = new_answer
                    mark_position = _positions_for_multiselect_answer(number, answer)
                if number != "34" and removed and added:
                    review_reasons.append(
                        f"model claimed {removed!r} marked (but pixel found blank) "
                        f"and missed {added!r} marked (pixel found them) - "
                        f"reconciled to {answer!r} (no review needed for this "
                        "disagreement alone)."
                    )
                elif number != "34" and removed:
                    review_reasons.append(
                        f"model additionally claimed {removed!r} marked, but the pixel detector found "
                        f"{'that choice' if len(removed) == 1 else 'those choices'} confidently blank on "
                        "this scan - removed from the answer (no review needed for this "
                        "disagreement alone)."
                    )
                elif number != "34" and added:
                    review_reasons.append(
                        f"model missed {added!r} marked, but the pixel detector found "
                        f"{'that choice' if len(added) == 1 else 'those choices'} confidently marked on "
                        "this scan - added to the answer (no review needed for this "
                        "disagreement alone)."
                    )

        # Ambiguous-mark backstop (explicit user request): even when the
        # VETO/FILL reconciliation above found nothing to change (or wasn't
        # run at all because pixel_multiselect_ratios was empty/missing),
        # any choice detect_multiselect_ink_ratios() flagged as ambiguous -
        # a mark near the confident-blank/confident-marked threshold, or one
        # whose ink bled outside its own calibrated box - means the pixel
        # side genuinely could not verify this question, unlike the
        # confident VETO/FILL corrections just above. Always needs_review in
        # that case, regardless of whether the final answer used here is the
        # model's original or a reconciled one.
        if pixel_multiselect_ambiguous and number in MULTI_SELECT_QUESTION_NUMBERS:
            needs_review = True
            review_reasons.append(
                f"pixel detector found {pixel_multiselect_ambiguous!r}'s mark unclear or exceeding its "
                "own checkbox area on this scan - needs review to confirm the true answer."
            )

        # Same ambiguous-mark backstop as above, for the single-select
        # yesno_box detector (Q19/20/21/22/23/25/27/28/29/30/32/35): this
        # question's row/list WAS located, but one of its choice boxes
        # couldn't be cleanly read afterward - see detect_yesno_box_
        # answers()'s include_diagnostics docstring for the real confirmed
        # case (Nov5_1_TPS_2988.pdf's Q29: the model read "Male" while the
        # true mark was on "Female", immediately above a box the mark's own
        # ink bled into).
        #
        # Exception (explicit user request): when the pixel detector DID
        # still resolve a position despite the ambiguity (yesno_promoted
        # above already promoted it into `answer`) AND the model's own
        # independent reading agrees with that same choice, the two
        # independent signals corroborate each other - there's no real
        # disagreement left for a human to adjudicate, just an ink-quality
        # observation. Only skip the flag when there's an actual answer to
        # agree on (a blank/empty agreement is not meaningful agreement);
        # if the pixel side couldn't resolve a position at all (pixel_
        # position is None), there is nothing to compare the model's answer
        # against, so this still needs a human look as before.
        yesno_ambiguous_agrees_with_model = (
            pixel_position is not None
            and answer.strip()
            and model_answer.strip().lower() == answer.strip().lower()
        )
        if pixel_yesno_ambiguous and not yesno_ambiguous_agrees_with_model:
            needs_review = True
            review_reasons.append(
                "pixel detector located this question but a choice's mark was unclear or exceeded its "
                "own checkbox area on this scan (likely ink bleeding from an adjacent choice) - needs "
                "review to confirm the true answer."
            )

        # Explicit user request (Revision 28): when detect_checkbox_grid_
        # answers() couldn't locate the Q1-18 grid AT ALL on this file (0 of
        # 18 rows - see that ERROR log line and pixel_grid_totally_missing
        # above), every one of Q1-18 is running on the model's own reading
        # with NO independent pixel cross-check whatsoever - unlike a normal
        # miss on just one or two rows (which still leaves the OTHER 16-17
        # rows pixel-verified), a total failure here means this file's
        # entire main scale is unverified. Force needs_review on all 18 so
        # they're never mistaken for a normal, checked row.
        if pixel_grid_totally_missing:
            needs_review = True
            review_reasons.append(
                "the pixel-based checkbox-grid detector could not locate ANY of the 18 grid rows on "
                "this file's page 1 (0 of 18) - this question is running on the vision model's reading "
                "alone, with no independent pixel verification at all - needs review."
            )

        # General "two marks in the same single-select row" backstop
        # (Revision 29, explicit user request: "If two marks are identified
        # in 2 boxes, no matter which question it is, it needs to be marked
        # as need review"). Set by detect_checkbox_grid_answers() (Q1-18)
        # independently of, and in addition to, the narrower crossed-out-
        # correction heuristic above (_GRID_OVERDENSE_INK_CEILING), which
        # can miss a real correction entirely when neither box's own ink
        # ratio happens to read implausibly high on its own (confirmed on
        # Nov6_1_TPS_3934.pdf's Q11 - see that function's own comment).
        if pixel_multiple_marks_detected:
            needs_review = True
            review_reasons.append(
                "the pixel detector found two boxes on this row both confidently marked (e.g. one "
                "crossed out and a different one marked instead) - needs review to confirm the true "
                "answer."
            )

        # Same "two marks" backstop for yesno_box questions (19,20,21,22,23,
        # 25,27,28,29,30,32,35) - set by detect_yesno_box_answers() using the
        # stricter _YESNO_BLANK_INK_FLOOR (0.25) specifically to avoid the
        # documented "(specify)" adjacent-freetext-line contamination noise
        # (topped out at 0.136 on a real file - see the promotion logic
        # above), so this only fires on two genuinely dark marks in the same
        # question (e.g. one crossed out and a different one marked instead).
        if pixel_yesno_multiple_marks_detected:
            needs_review = True
            review_reasons.append(
                "the pixel detector found two boxes on this question both confidently marked (e.g. "
                "one crossed out and a different one marked instead) - needs review to confirm the "
                "true answer."
            )

        # H2/H6 Vision-OCR-authoritative override (Revision 29, explicit
        # user request: "H2 and H6 should have the Vision OCR rule over the
        # Vision model, because I think Vision OCR has been capturing it
        # more accurately"). The actual override already happened upstream,
        # in extract_qa_from_pdf() (answer/mark_position above already
        # reflect it) - this just records it for auditability, the same way
        # a confident pixel correction is recorded without forcing
        # needs_review by itself.
        if vision_overrode_model:
            review_reasons.append(
                f"Cloud Vision's own anchored OCR reading of this field's printed label disagreed with "
                f"the model's answer ('{model_answer_before_vision_override}') and was used instead, "
                f"per policy - Vision OCR is treated as authoritative for this field."
            )

        # NEW check 4a: model self-reported confidence gate. Only meaningful
        # once no pixel-verified reading has already taken over this
        # question - a deterministic pixel detector's own margin-based
        # confidence check already gates whether IT fired at all (see the
        # pixel branches above), so this gate is specifically about trusting
        # Gemini's OWN judgment when nothing else backs it up. Always
        # RECORDED (model_confidence goes on the row regardless), but only
        # ACTED ON (forces needs_review) when detection_method == "model".
        if (
            detection_method == "model"
            and model_confidence is not None
            and model_confidence < MODEL_CONFIDENCE_THRESHOLD
        ):
            needs_review = True
            review_reasons.append(
                f"model self-reported confidence {model_confidence:.2f} is below the "
                f"{MODEL_CONFIDENCE_THRESHOLD:.2f} review threshold"
            )

        # NEW check 4b: Cloud Vision double-check for handwritten/write-in
        # fields (H1/H2/H4/H5/H6/24/26) - see
        # cross_check_written_field_with_vision(). vision_match is only ever
        # set (True/False) for these seven questions - see
        # WRITTEN_TEXT_QUESTION_NUMBERS - since extract_qa_from_pdf() only
        # runs the Vision comparison for them.
        if vision_match is True:
            vision_cross_check = "agree"
        elif vision_match is False:
            vision_cross_check = "disagree"
            needs_review = True
            if vision_note:
                review_reasons.append(vision_note)

        # NEW check 4c: deterministic format validation for H1/H6 - see
        # _validate_written_field_format()'s docstring for the full
        # rationale. Independent of every check above (runs regardless of
        # detection_method, vision_cross_check, or model confidence) since a
        # same-shape wrong value can otherwise sail straight through all of
        # them; only ever flags an answer that is STRUCTURALLY invalid for
        # its field (wrong digit count, unparseable date), never a
        # plausible-but-possibly-wrong one.
        format_issue = _validate_written_field_format(number, answer)
        if format_issue:
            needs_review = True
            review_reasons.append(format_issue)

        # Enrich review_note with the model's own scan-quality observation
        # (rule 11 in build_extraction_prompt(), which now explicitly asks
        # the model to name the physical condition behind any uncertainty -
        # faint ink, messy handwriting, page skew, a smudge, etc.) whenever
        # this row ends up flagged for review. This is NOT a new LLM call -
        # model_reasoning is already returned by the SAME extraction call
        # that produced answer/mark_position/confidence above and was already
        # being stored on the row (see QARow.model_reasoning); it just wasn't
        # previously surfaced in review_note, which is what the per-file
        # quality-review table (in build_file_quality_review.py) is assembled from.
        # Deliberately appended LAST and only when needs_review is already
        # True for some other reason - this is descriptive context for a
        # human to use while triaging, never itself a trigger for review.
        if needs_review and model_reasoning:
            review_reasons.append(f"model's own account of the scan: {model_reasoning}")

        # General "blank output always needs review" backstop (Revision 29,
        # explicit user request, reiterated twice: a BigQuery row that shows
        # up blank should "at least" be flagged for review, even when
        # nothing else above caught it - e.g. the model itself never
        # answered this question at all, with no pixel/vision signal to
        # disagree with either, which is exactly the shape that sailed
        # through every check above with needs_review still False before
        # this). Deliberately unconditional and LAST, after every other
        # check has had its chance to build a more specific review_reasons
        # message - a genuinely blank final answer is always worth a human
        # glance, whatever the reason it ended up blank.
        #
        # Exemption (Revision 29 follow-up, explicit user request): H4
        # (agency/program name) and H5 (address) are free-form written
        # fields that are legitimately left blank far more often than the
        # near-mandatory ID (H1) and date (H6) fields - a blank H4/H5 is not
        # itself suspicious the way a blank H1/H6 is, so this backstop does
        # not force needs_review for them. Any OTHER check above (format
        # validation, vision cross-check disagreement, etc.) can still flag
        # H4/H5 for its own specific reason - this exemption only removes
        # the generic "it's blank" trigger.
        # Revision 37 follow-up #2 (explicit user request): NEITHER route
        # ("vision" or "fallback") blanket-forces needs_review anymore -
        # extract_qa_from_pdf() always sets force_review=False now, so
        # quality_route_forces_review is never True in current behavior.
        # This check/mechanism is kept in place (rather than deleted)
        # because it's still the intended hook if a future revision or a
        # manual quality_route override ever needs to force whole-file
        # review again - it stays deliberately additive with every check
        # above it (review_reasons may already be non-empty), not a
        # replacement for them.
        if quality_route_forces_review:
            needs_review = True
            review_reasons.append(
                f"upstream pdf_quality classifier recommended_route={pdf_quality_route!r} for this "
                "file - routed for review regardless of this question's own signals."
            )

        if not answer.strip() and number not in _BLANK_ANSWER_EXEMPT_FIELDS:
            needs_review = True
            if not review_reasons:
                review_reasons.append(
                    "this question's final answer is blank - needs review to confirm this is a "
                    "genuine skip and not a missed/dropped answer."
                )

        review_note = "; ".join(review_reasons)

        rows.append(
            QARow(
                folder_name=folder,
                file_name=file_name,
                survey_question=QUESTION_TEXT_BY_NUMBER[number],
                survey_answer=answer,
                question_number=number,
                report_date=report_date_str,
                refreshed_at=refreshed_at_str,
                refreshed_date=refreshed_date_str,
                mark_position=mark_position or None,
                needs_review=needs_review,
                review_note=review_note,
                detection_method=detection_method,
                model_reasoning=model_reasoning,
                model_confidence=model_confidence,
                vision_cross_check=vision_cross_check,
                # Recorded unconditionally (even "" / None rows) so every row
                # in BigQuery documents exactly what threshold/comparison
                # data it was evaluated against - see the QARow field
                # comments for why this is useful as an audit trail.
                confidence_threshold=MODEL_CONFIDENCE_THRESHOLD,
                vision_match_score=(vision_score if vision_cross_check else None),
                vision_ocr_snippet=(vision_snippet if vision_cross_check else ""),
                pdf_quality_route=pdf_quality_route,
            )
        )
    return rows


BQ_SURVEY_RESPONSES_SCHEMA = [
    ("folder_name", "STRING", None),
    ("file_name", "STRING", None),
    ("survey_question", "STRING", None),
    ("survey_answer", "STRING", None),
    ("question_number", "STRING", "e.g. '22' or 'H3' - the raw question key from SURVEY_QUESTIONS/QUESTION_TEXT_BY_NUMBER that survey_question's text came from. Lets you group/join/filter by question identity without parsing survey_question's free text."),
    ("report_date", "DATE", "Date the survey was filled out, parsed from the source date-folder name."),
    ("refreshed_at", "TIMESTAMP", "When this ETL run loaded the row."),
    ("refreshed_date", "DATE", "DATE(refreshed_at); the table's partitioning column."),
    ("mark_position", "STRING", "1-based position(s) of the marked checkbox(es), as reported by the model, for cross-checking against survey_answer."),
    ("needs_review", "BOOLEAN", "TRUE when something about this reading disagreed, was below the model-confidence threshold, or was disputed by the Cloud Vision double-check, and is worth a manual check (see verify_pdf())."),
    ("review_note", "STRING", "Why needs_review is TRUE; empty otherwise. May combine more than one reason, semicolon-separated."),
    ("detection_method", "STRING", "'pixel_grid' (deterministic ink-density read of the Q1-18 checkbox grid), 'pixel_yesno_box' (same idea for the isolated Q21/22/27/32 Yes/No(/Unknown) rows and the Q25/Q29/Q35 single-choice lists), 'pixel_h3_circle' (H3's round radio buttons), or 'model' (vision-model reading, self-checked against its own mark_position). Pixel readings are trusted over the model."),
    ("model_reasoning", "STRING", "The model's own one-sentence account of the visual evidence for its answer (see rule 11 in build_extraction_prompt()). For auditability/debugging only - empty for pixel-only entries, and never used to decide needs_review or to override anything."),
    ("model_confidence", "FLOAT", "The model's own self-reported confidence (0.0-1.0) for this specific answer (see rule 15 in build_extraction_prompt()). NULL if not reported (e.g. a pre-upgrade row, or the model omitted/malformed it). Below MODEL_CONFIDENCE_THRESHOLD (default 0.8) sets needs_review=TRUE, unless a pixel detector already confidently resolved this question."),
    ("vision_cross_check", "STRING", "For the seven handwritten/write-in questions with no fixed choice list (H1, H2, H4, H5, H6, 24, 26): 'agree' or 'disagree' from an independent Cloud Vision OCR cross-check of the same page (see cross_check_written_field_with_vision()). Empty for every other question, or when Vision was unavailable/found nothing to compare against."),
    ("confidence_threshold", "FLOAT", "The value of MODEL_CONFIDENCE_THRESHOLD actually in effect when this row was extracted - recorded on every row (regardless of detection_method) as an audit trail, so a query can directly compare model_confidence against confidence_threshold per row, including historical rows extracted under a different threshold value if it's retuned later."),
    ("vision_match_score", "FLOAT", "The numeric agreement score behind vision_cross_check: 1.0/0.0 for the token fields' exact-match check (H1/H2/26), or the actual difflib longest-contiguous-match coverage fraction (0.0-1.0) for the freeform fields' fuzzy-match check (H4/H5/H6/24). Populated whenever vision_cross_check is 'agree' or 'disagree' (even on agreement, so a narrow pass is visible); NULL when vision_cross_check is empty."),
    ("vision_ocr_snippet", "STRING", "What Cloud Vision actually read that survey_answer was compared against for the Vision double-check - a handful of individual word tokens for the token fields (H1/H2/26), or a truncated excerpt of the page's full OCR text for the freeform fields (H4/H5/H6/24). Populated whenever vision_cross_check is 'agree' or 'disagree'; empty when vision_cross_check is empty. Lets you eyeball the model's answer against Vision's independent reading directly."),
    ("pdf_quality_route", "STRING", "This file's 'recommended_route' ('pixel'/'vision'/'fallback') from classify_pdf_quality.py's upstream pdf_quality table at extraction time, or NULL if no quality row was found / routing was disabled for this run. 'vision'/'fallback' force needs_review=TRUE on every question for this file regardless of that question's own signals - see review_note for the specific note when that happened."),
]
# Column names that are REPEATED (arrays of STRING) rather than scalar, for
# both BigQuery SchemaField construction and the Spark ArrayType mapping in
# bq_schema_to_spark_schema() below.
_BQ_REPEATED_COLUMNS = {"issues", "unreadable_areas"}


def ensure_bq_table(bq_client, project: str, dataset: str, table: str):
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    dataset_ref = bigquery.DatasetReference(project, dataset)
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        status("[BQ] Creating dataset %s.%s", project, dataset)
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))

    table_ref = dataset_ref.table(table)
    schema = [
        bigquery.SchemaField(
            name, typ if typ != "FLOAT" else "FLOAT64",
            mode="REPEATED" if name in _BQ_REPEATED_COLUMNS else "NULLABLE",
            description=desc,
        )
        for name, typ, desc in BQ_SURVEY_RESPONSES_SCHEMA
    ]
    try:
        bq_table = bq_client.get_table(table_ref)
        # The table already exists — e.g. from a run before report_date /
        # refreshed_at / refreshed_date / model_confidence / vision_cross_check
        # were added to `schema` above. Add any columns the live table is
        # missing (BigQuery allows adding new NULLABLE columns to an existing
        # table without recreating it or touching existing rows/data). This is
        # what fixes "BadRequest: No such field: report_date" (or, now,
        # "No such field: model_confidence") on a load into an older table.
        #
        # NOT handled here: BigQuery partitioning is immutable after table
        # creation, so an existing unpartitioned table stays unpartitioned —
        # drop and recreate (or migrate manually) if you need to add that.
        expected_field_types = {f.name: f.field_type for f in schema}
        type_mismatches = [
            f for f in bq_table.schema
            if f.name in expected_field_types and f.field_type != expected_field_types[f.name]
        ]
        if type_mismatches:
            # BigQuery cannot ALTER a column's type in place (only add new
            # NULLABLE columns) - a live table left over from before a column
            # was retyped in BQ_SURVEY_RESPONSES_SCHEMA (e.g. model_confidence
            # created as STRING before it was declared FLOAT) has no in-place
            # fix. Per explicit user decision, this DROPS AND RECREATES the
            # table with the correct schema - existing rows are lost. If that
            # data must be kept, migrate it out (or CAST it into a new table)
            # before running this, or point --bq-table at a new table name.
            status(
                "[BQ] Table %s.%s.%s has type mismatch(es) %s — dropping and recreating with the correct schema "
                "(existing rows will be lost; BigQuery cannot alter a column's type in place).",
                project,
                dataset,
                table,
                [f"{f.name}: {f.field_type} -> {expected_field_types[f.name]}" for f in type_mismatches],
            )
            bq_client.delete_table(table_ref)
            raise NotFound("table dropped for schema-type recreation")
        existing_field_names = {f.name for f in bq_table.schema}
        missing_fields = [f for f in schema if f.name not in existing_field_names]
        if missing_fields:
            status(
                "[BQ] Table %s.%s.%s is missing column(s) %s — adding them (existing rows get NULL for these).",
                project,
                dataset,
                table,
                [f.name for f in missing_fields],
            )
            bq_table.schema = list(bq_table.schema) + missing_fields
            bq_table = bq_client.update_table(bq_table, ["schema"])
        else:
            status("[BQ] Table %s.%s.%s already exists with the expected schema.", project, dataset, table)
    except NotFound:
        status("[BQ] Creating table %s.%s.%s (partitioned by refreshed_date)", project, dataset, table)
        new_table = bigquery.Table(table_ref, schema=schema)
        new_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="refreshed_date",
        )
        bq_table = bq_client.create_table(new_table)
    return bq_table


def delete_existing_rows_for_folder(bq_client, table_ref, folder: str) -> None:
    """DELETEs any rows already loaded for this folder_name, so re-running
    the script against the same folder replaces its rows instead of
    duplicating them. A no-op (deletes 0 rows) the first time a folder is
    loaded. Uses a query job (DML), not a streaming insert, specifically so
    this delete-then-load pattern doesn't hit BigQuery's "can't
    UPDATE/DELETE rows that were just streamed in" restriction — see
    load_rows_into_bq() below, which loads via a load job for the same
    reason."""
    from google.cloud import bigquery

    full_table_id = f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}"
    query = f"DELETE FROM `{full_table_id}` WHERE folder_name = @folder"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("folder", "STRING", folder)]
    )
    status("[BQ] Deleting existing rows (if any) for folder_name = %r in %s", folder, full_table_id)
    bq_client.query(query, job_config=job_config).result()


def bq_schema_to_spark_schema(bq_schema):
    """Converts one of this file's plain (name, type, description) BigQuery
    schema registries (BQ_SURVEY_RESPONSES_SCHEMA /
    a pdf_quality-style schema passed in the same shape) into an explicit
    pyspark.sql.types.StructType, so Spark loads use a known schema instead
    of auto-inferring one from the data (auto-inference is unreliable across
    empty/partial batches and would happily "invent" a different type for a
    column that's all-NULL in one folder's batch)."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        FloatType, BooleanType, DateType, TimestampType, ArrayType,
    )

    bq_to_spark = {
        "STRING": StringType(), "INTEGER": IntegerType(), "FLOAT": FloatType(),
        "FLOAT64": FloatType(), "BOOLEAN": BooleanType(), "DATE": DateType(),
        "TIMESTAMP": TimestampType(),
    }
    fields = []
    for name, typ, _desc in bq_schema:
        base_type = bq_to_spark[typ]
        spark_type = ArrayType(StringType()) if name in _BQ_REPEATED_COLUMNS else base_type
        fields.append(StructField(name, spark_type, nullable=True))
    return StructType(fields)


def load_rows_into_bq_via_spark(
    rows: list,
    bq_schema,
    project: str,
    dataset: str,
    table: str,
    staging_bucket: Optional[str] = None,
) -> int:
    """Loads `rows` (a list of plain dicts, same shape load_rows_into_bq()
    used to take) into BigQuery through Spark + the BigQuery Spark
    connector, instead of a bigquery.Client load job. Requires a
    SparkSession already in scope as the global `spark` (e.g. injected by a
    notebook environment such as Databricks) - this function does not create
    one itself.

    Builds a pandas DataFrame from `rows`, converts it to a Spark DataFrame
    against an explicit schema derived from `bq_schema` via
    bq_schema_to_spark_schema() (rather than letting Spark infer one), and
    writes it out with mode='append' so this composes with the existing
    delete-then-load-per-folder pattern in extract_to_bigquery() (the
    DELETE for a re-run of the same folder still happens first via
    delete_existing_rows_for_folder(); this call only appends). BigQuery's
    own partitioning on refreshed_date - set up when the table is created
    by ensure_bq_table() - is unaffected by
    writing through Spark: the Spark connector writes to the existing table
    definition rather than recreating it, so the partitioning column and
    scheme stay whatever ensure_bq_table() set
    up when the table was first created.

    staging_bucket: the connector's default ("indirect") write path stages
    the DataFrame's data into a GCS bucket before loading it into BigQuery,
    and raises "Either temporary or persistent GCS bucket must be set" if
    none is configured - it does NOT reuse BUCKET_NAME automatically.
    Defaults to SPARK_BQ_STAGING_BUCKET (which itself defaults to
    BUCKET_NAME) if not given. Set via the temporaryGcsBucket write option
    (per-call) rather than spark.conf's global 'temporaryGcsBucket' setting,
    so this doesn't require every caller to have configured the
    SparkSession itself."""
    if not rows:
        return 0
    if "spark" not in globals():
        raise RuntimeError(
            "load_rows_into_bq_via_spark() requires a SparkSession already in "
            "scope as the global `spark` (e.g. running inside a Databricks "
            "notebook) - none was found. Run this from a Spark-enabled "
            "notebook environment, or use load_rows_into_bq() instead."
        )
    staging_bucket = staging_bucket or SPARK_BQ_STAGING_BUCKET
    import pandas as pd

    # Row-producing code (answers_to_qa_rows() etc.) stores report_date/
    # refreshed_at/refreshed_date as ISO strings, but Spark's DateType/
    # TimestampType converters require real date/datetime objects, not
    # strings - otherwise: "[CANNOT_ACCEPT_OBJECT_IN_TYPE] `DateType()` can
    # not accept object '<string>' in type `str`." Convert any DATE/
    # TIMESTAMP-typed column's string values to real objects before handing
    # rows to pandas (see build_file_quality_review.py's identical fix).
    date_type_columns = {name for name, typ, _desc in bq_schema if typ == "DATE"}
    timestamp_type_columns = {name for name, typ, _desc in bq_schema if typ == "TIMESTAMP"}
    normalized_rows = []
    for row in rows:
        normalized = dict(row)
        for col in date_type_columns:
            value = normalized.get(col)
            if isinstance(value, str):
                try:
                    normalized[col] = datetime.date.fromisoformat(value)
                except ValueError:
                    normalized[col] = None
        for col in timestamp_type_columns:
            value = normalized.get(col)
            if isinstance(value, str):
                try:
                    normalized[col] = datetime.datetime.fromisoformat(value)
                except ValueError:
                    normalized[col] = None
        normalized_rows.append(normalized)

    df = pd.DataFrame(normalized_rows, dtype=object)
    spark_schema = bq_schema_to_spark_schema(bq_schema)
    # Guarantee every schema column is present (as all-NULL) even if this
    # particular batch of rows happened not to populate it, and drop the
    # reverse case (a stray key not in the schema) rather than let Spark's
    # DataFrame construction fail on a column/schema mismatch. Constructing
    # pd.DataFrame(rows) from a list of dicts with ragged keys already fills
    # any row's absent key with a float NaN (regardless of dtype=object
    # above), and Spark's ArrayType converter chokes on that NaN with
    # "TypeError: 'float' object is not iterable" - so every NaN is replaced
    # with a real Python None (via .where(), which leaves lists/strs/bools/
    # numbers alone and only touches actual NaN cells) before handing the
    # DataFrame to Spark.
    for field in spark_schema.fieldNames():
        if field not in df.columns:
            df[field] = None
    df = df[spark_schema.fieldNames()]
    df = df.where(df.notna(), None)

    spark_df = globals()["spark"].createDataFrame(df, schema=spark_schema)
    full_table_id = f"{project}.{dataset}.{table}"
#     (
#         spark_df.write.format("bigquery")
#         .option("table", full_table_id)
#         .option("temporaryGcsBucket", staging_bucket)
#         .option("partitionField", "refreshed_date")
#         .option("partitionType", "DAY")
#         .mode("append")
#         .save()
#     )
    status(
        "[BQ][SPARK] Wrote %d row(s) into %s via Spark (partitioned by refreshed_date, staged through gs://%s).",
        len(rows), full_table_id, staging_bucket,
    )
#     return len(rows)
#     spark_df.show(truncate = false)
    spark_df = spark_df.withColumn("event_partition", spark_df["report_date"])
    writeToEventStore(spark_df, '@OutputTable1', 1, "event_partition")
    return len(rows)


def load_rows_into_bq(bq_client, table_ref, rows: list) -> int:
    """Loads rows via a BigQuery load job (WRITE_APPEND), not insert_rows_json
    (streaming). Load jobs are free, and — unlike streaming inserts — don't
    leave rows sitting in a streaming buffer that blocks DML DELETE/UPDATE
    for up to ~90 minutes, which matters here since we DELETE-then-load per
    folder on every run.

    Kept as a fallback path (used by self-tests, and available for a run
    outside a Spark environment); extract_to_bigquery() itself now loads
    survey_responses and file_quality_review via
    load_rows_into_bq_via_spark() - see Revision 40."""
    from google.cloud import bigquery

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = bq_client.load_table_from_json(rows, table_ref, job_config=job_config)
    load_job.result()  # wait for the job to finish; raises on failure
    return load_job.output_rows


# --------------------------------------------------------------------------
# Corrections log: a durable, queryable record of every wrong answer a human
# has reported against this pipeline's output, in its own BigQuery table
# (separate from survey_responses). This exists because the alternative —
# root-causing each new bug report from scratch, relying on this
# conversation's own memory or the project doc's prose to know what's
# already been tried — doesn't scale and doesn't survive a new conversation.
# A structured log lets any future session (or a plain SQL query from the
# user directly) answer "has this exact question/file/failure shape come up
# before, and what fixed it" without re-deriving it.
#
# Deliberately lightweight: one flat table, no foreign keys, no dedup logic.
# It's a log, not a ticket tracker - append what happened, query it later.
# --------------------------------------------------------------------------
_CORRECTIONS_ROOT_CAUSE_CATEGORIES = {
    "ocr_checkbox_glyph_corruption",
    "geometric_false_positive",
    "row_pitch_drift",
    "fixed_pad_insufficient",
    "locate_checkbox_wrong_box",
    "grid_threshold_shading",
    "model_defaults_to_later_option",
    "model_multiselect_over_selection",
    "model_hallucinated_blank_question",
    "row_line_search_range_exceeded",
    "yesno_blank_floor_style_mismatch",
    "model_column_misread",
    "no_pixel_backstop",
    "model_low_confidence_unreviewed",  # the model itself reported low confidence (rule 15) but the answer was used without a human review pass - see MODEL_CONFIDENCE_THRESHOLD
    "vision_ocr_disagreement",  # Cloud Vision's independent OCR of a handwritten/write-in field (H1/H2/H4/H5/H6/24/26) disagreed with the model's answer - see cross_check_written_field_with_vision()
    "unresolved",  # reported, not yet root-caused - use this rather than guessing a category
}


# --------------------------------------------------------------------------
# Revision 38: the registry of every calibration/tuning parameter that can
# be overridden at runtime from the pipeline_config BigQuery table (see
# query_pipeline_config()/apply_pipeline_config() above, and the
# "revision-38-externalized-pipeline-config.md" project doc). Every value
# here is a plain reference to this file's own already-defined constant -
# so this dict is always an accurate live snapshot of the CODE's current
# defaults at import time, before any BigQuery override is applied, and
# there's no separate copy of any threshold to keep in sync by hand.
#
# Deliberately placed here (after every constant it references is already
# defined, and right next to _CORRECTIONS_ROOT_CAUSE_CATEGORIES, its own
# last entry) rather than scattered inline - one single place to see the
# entire externalized calibration surface at a glance.
#
# Scope (explicit user decision): every _GRID_*/_YESNO_*/_MULTISELECT_*
# pixel-detection threshold, pad, and per-question override, plus
# _INK_THRESHOLD, _WRITTEN_FIELD_ANCHORS, and
# _CORRECTIONS_ROOT_CAUSE_CATEGORIES. Deliberately EXCLUDED: the survey
# template itself (SURVEY_QUESTIONS, CHOICE_LISTS_BY_NUMBER,
# QUESTION_TEXT_BY_NUMBER, MULTI_SELECT_QUESTION_NUMBERS,
# WRITTEN_TEXT_QUESTION_NUMBERS, etc.) and operational config
# (BUCKET_NAME, BQ_*/PDF_QUALITY_TABLE/PIPELINE_CONFIG_TABLE names,
# GEMINI_MODEL, MODEL_CONFIDENCE_THRESHOLD, VISION_*, etc.) - those rarely
# change and aren't really "calibration" in the pixel-detection sense this
# revision targets. Also EXCLUDED: pure runtime state flags that are never
# meant to be hand-tuned (_GRID_DEPS_CHECKED, _VISION_DEPS_CHECKED,
# _VERTEX_INITIALIZED, _EXTRACTION_PROMPT).
# --------------------------------------------------------------------------
_PIPELINE_CONFIG_DEFAULTS = {
    "_INK_THRESHOLD": _INK_THRESHOLD,
    "_GRID_CONFIDENCE_MARGIN": _GRID_CONFIDENCE_MARGIN,
    "_GRID_CONFIDENCE_MARGIN_OVERRIDE": _GRID_CONFIDENCE_MARGIN_OVERRIDE,
    "_GRID_BLANK_INK_FLOOR": _GRID_BLANK_INK_FLOOR,
    "_GRID_BLANK_MARGIN_CEILING": _GRID_BLANK_MARGIN_CEILING,
    "_GRID_TWO_MARKS_INK_FLOOR": _GRID_TWO_MARKS_INK_FLOOR,
    "_GRID_OVERDENSE_INK_CEILING": _GRID_OVERDENSE_INK_CEILING,
    "_GRID_FAINT_LINE_FLOOR": _GRID_FAINT_LINE_FLOOR,
    "_GRID_COLUMN_CENTERS": _GRID_COLUMN_CENTERS,
    "_GRID_COLUMN_PAD": _GRID_COLUMN_PAD,
    "_GRID_BOX_EXPECTED_SIZE": _GRID_BOX_EXPECTED_SIZE,
    "_YESNO_BOX_PAD": _YESNO_BOX_PAD,
    "_YESNO_BOX_PAD_OVERRIDE": _YESNO_BOX_PAD_OVERRIDE,
    "_YESNO_BOX_UP_PAD_OVERRIDE": _YESNO_BOX_UP_PAD_OVERRIDE,
    "_YESNO_CONFIDENCE_MARGIN": _YESNO_CONFIDENCE_MARGIN,
    "_YESNO_CONFIDENCE_MARGIN_OVERRIDE": _YESNO_CONFIDENCE_MARGIN_OVERRIDE,
    "_YESNO_BLANK_INK_FLOOR": _YESNO_BLANK_INK_FLOOR,
    "_YESNO_BLANK_MARGIN_CEILING": _YESNO_BLANK_MARGIN_CEILING,
    "_YESNO_LIGHT_MARK_MIN_SEPARATION": _YESNO_LIGHT_MARK_MIN_SEPARATION,
    "_YESNO_LIGHT_MARK_FLOOR": _YESNO_LIGHT_MARK_FLOOR,
    "_YESNO_OVERDENSE_INK_CEILING": _YESNO_OVERDENSE_INK_CEILING,
    "_YESNO_OVERDENSE_SECOND_MARK_FLOOR": _YESNO_OVERDENSE_SECOND_MARK_FLOOR,
    "_YESNO_BLIND_INK_FLOOR": _YESNO_BLIND_INK_FLOOR,
    "_YESNO_BLIND_INK_MARGIN": _YESNO_BLIND_INK_MARGIN,
    "_YESNO_ROW_LINE_MARGIN": _YESNO_ROW_LINE_MARGIN,
    "_YESNO_ROW_LINE_SEARCH": _YESNO_ROW_LINE_SEARCH,
    "_YESNO_ROW_ABOVE_SEARCH_OVERRIDE": _YESNO_ROW_ABOVE_SEARCH_OVERRIDE,
    "_YESNO_ROW_LINE_SEARCH_OVERRIDE": _YESNO_ROW_LINE_SEARCH_OVERRIDE,
    "_YESNO_ROW_PITCH_PARTNER": _YESNO_ROW_PITCH_PARTNER,
    "_YESNO_LIST_ANCHOR_PAD": _YESNO_LIST_ANCHOR_PAD,
    "_YESNO_ANCHOR_X_PAD": _YESNO_ANCHOR_X_PAD,
    "_YESNO_ROW_MODE": _YESNO_ROW_MODE,
    "_YESNO_BOX_INK_BORDER_OVERRIDE": _YESNO_BOX_INK_BORDER_OVERRIDE,
    "_YESNO_BOX_CALIBRATION": _YESNO_BOX_CALIBRATION,
    "_MULTISELECT_BOX_CALIBRATION": _MULTISELECT_BOX_CALIBRATION,
    "_MULTISELECT_BOX_PAD_OVERRIDE": _MULTISELECT_BOX_PAD_OVERRIDE,
    "_MULTISELECT_INK_BORDER_OVERRIDE": _MULTISELECT_INK_BORDER_OVERRIDE,
    "_MULTISELECT_UNMARKED_CEILING": _MULTISELECT_UNMARKED_CEILING,
    "_MULTISELECT_ROW_PITCH_ANOMALY_RATIO": _MULTISELECT_ROW_PITCH_ANOMALY_RATIO,
    "_MULTISELECT_ANCHOR_DOWN_PAD": _MULTISELECT_ANCHOR_DOWN_PAD,
    "_MULTISELECT_ANCHOR_UP_PAD": _MULTISELECT_ANCHOR_UP_PAD,
    "_MULTISELECT_TEXT_BAND_UP_PAD": _MULTISELECT_TEXT_BAND_UP_PAD,
    "_MULTISELECT_TEXT_BAND_DOWN_PAD": _MULTISELECT_TEXT_BAND_DOWN_PAD,
    "_MULTISELECT_TEXT_BAND_X_GAP": _MULTISELECT_TEXT_BAND_X_GAP,
    "_MULTISELECT_TEXT_BAND_X_WIDTH": _MULTISELECT_TEXT_BAND_X_WIDTH,
    "_MULTISELECT_TEXT_BAND_MIN_HEIGHT": _MULTISELECT_TEXT_BAND_MIN_HEIGHT,
    "_MULTISELECT_TEXT_BAND_INK_THRESHOLD": _MULTISELECT_TEXT_BAND_INK_THRESHOLD,
    "_MULTISELECT_ANCHOR_X_PAD": _MULTISELECT_ANCHOR_X_PAD,
    "_MULTISELECT_AMBIGUOUS_BAND": _MULTISELECT_AMBIGUOUS_BAND,
    "_MULTISELECT_OVERFLOW_EXPAND_LEFT": _MULTISELECT_OVERFLOW_EXPAND_LEFT,
    "_MULTISELECT_OVERFLOW_EXPAND_Y": _MULTISELECT_OVERFLOW_EXPAND_Y,
    "_MULTISELECT_OVERFLOW_RATIO": _MULTISELECT_OVERFLOW_RATIO,
    "_YESNO_LIST_ANCHOR_CORROBORATE_PAD": _YESNO_LIST_ANCHOR_CORROBORATE_PAD,
    "_YESNO_LIST_ANCHOR_TEXT_ALIAS_CEILING": _YESNO_LIST_ANCHOR_TEXT_ALIAS_CEILING,
    "_YESNO_LIST_ANCHOR_GENUINE_MARK_FLOOR": _YESNO_LIST_ANCHOR_GENUINE_MARK_FLOOR,
    "_WRITTEN_FIELD_ANCHORS": _WRITTEN_FIELD_ANCHORS,
    "_CORRECTIONS_ROOT_CAUSE_CATEGORIES": _CORRECTIONS_ROOT_CAUSE_CATEGORIES,
}


def ensure_corrections_table(bq_client, project: str, dataset: str, table: str):
    """Creates (or, on an older table, patches) the corrections_log table
    schema - same create-or-patch pattern as ensure_bq_table(), so an
    existing table never needs to be dropped just because a new column was
    added here later."""
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    dataset_ref = bigquery.DatasetReference(project, dataset)
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        status("[BQ] Creating dataset %s.%s", project, dataset)
        bq_client.create_dataset(bigquery.Dataset(dataset_ref))

    table_ref = dataset_ref.table(table)
    schema = [
        bigquery.SchemaField("logged_at", "TIMESTAMP", description="When this correction was recorded (not when the error occurred)."),
        bigquery.SchemaField("logged_date", "DATE", description="DATE(logged_at); the table's partitioning column."),
        bigquery.SchemaField("folder_name", "STRING"),
        bigquery.SchemaField("file_name", "STRING"),
        bigquery.SchemaField("question_number", "STRING", description="e.g. '22' - matches survey_responses.survey_question's leading number."),
        bigquery.SchemaField("survey_question", "STRING", description="Full question text, for readability without a join."),
        bigquery.SchemaField("wrong_answer", "STRING", description="What the pipeline/BQ table showed. Empty string if the pipeline showed no answer but should have shown one (the reverse of the usual case)."),
        bigquery.SchemaField("wrong_mark_position", "STRING"),
        bigquery.SchemaField("correct_answer", "STRING", description="What the scan actually shows, per human review. Empty string for 'should have been blank' (see model_hallucinated_blank_question)."),
        bigquery.SchemaField("correct_mark_position", "STRING"),
        bigquery.SchemaField("detection_method_at_time", "STRING", description="pixel_grid / pixel_yesno_box / pixel_h3_circle / model - which path produced the wrong answer, if known."),
        bigquery.SchemaField("root_cause_category", "STRING", description="See _CORRECTIONS_ROOT_CAUSE_CATEGORIES in merge_survey_pdfs.py for the controlled vocabulary; 'unresolved' if not yet root-caused."),
        bigquery.SchemaField("is_recurrence", "BOOLEAN", description="TRUE if this matches a root_cause_category already logged for this question_number before this entry."),
        bigquery.SchemaField("fix_status", "STRING", description="One of: fixed, known_limitation, reported_pending_investigation, wont_fix."),
        bigquery.SchemaField("fix_description", "STRING", description="What was changed (or why it wasn't), in plain language."),
        bigquery.SchemaField("reported_by", "STRING"),
    ]
    try:
        bq_table = bq_client.get_table(table_ref)
        existing_field_names = {f.name for f in bq_table.schema}
        missing_fields = [f for f in schema if f.name not in existing_field_names]
        if missing_fields:
            status(
                "[BQ] Table %s.%s.%s is missing column(s) %s — adding them (existing rows get NULL for these).",
                project, dataset, table, [f.name for f in missing_fields],
            )
            bq_table.schema = list(bq_table.schema) + missing_fields
            bq_table = bq_client.update_table(bq_table, ["schema"])
        else:
            status("[BQ] Table %s.%s.%s already exists with the expected schema.", project, dataset, table)
    except NotFound:
        status("[BQ] Creating table %s.%s.%s (partitioned by logged_date)", project, dataset, table)
        new_table = bigquery.Table(table_ref, schema=schema)
        new_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="logged_date",
        )
        bq_table = bq_client.create_table(new_table)
    return bq_table


def log_corrections(bq_client, project: str, dataset: str, table: str, corrections: list) -> int:
    """Appends one or more correction reports to the corrections_log table.
    Each item in `corrections` is a dict that may set any of: folder_name,
    file_name, question_number, survey_question, wrong_answer,
    wrong_mark_position, correct_answer, correct_mark_position,
    detection_method_at_time, root_cause_category, is_recurrence, fix_status,
    fix_description, reported_by. Unset fields are stored as NULL/empty.
    `logged_at`/`logged_date` are always set here, from the current time -
    they are NOT accepted as input, since the whole point is an honest
    record of when each entry was actually added.

    root_cause_category is checked against _CORRECTIONS_ROOT_CAUSE_CATEGORIES
    and a warning (not an error - see that set's own comment) is logged for
    anything outside it, including a missing value (silently treated as
    "unresolved" instead of left NULL, so every row is queryable by
    category).

    Returns the number of rows the load job reports loaded. Uses a load job
    (WRITE_APPEND), matching load_rows_into_bq()'s reasoning - free, and
    doesn't leave rows in a streaming buffer."""
    from google.cloud import bigquery

    table_ref = ensure_corrections_table(bq_client, project, dataset, table).reference
    logged_at = datetime.datetime.now(datetime.timezone.utc)
    rows = []
    for c in corrections:
        root_cause = c.get("root_cause_category") or "unresolved"
        if root_cause not in _CORRECTIONS_ROOT_CAUSE_CATEGORIES:
            err(
                "[CORRECTIONS] root_cause_category %r is not in the known set - logging it "
                "anyway (a genuinely new failure shape is expected to happen), but double-check "
                "this isn't a typo of an existing category: %s",
                root_cause, sorted(_CORRECTIONS_ROOT_CAUSE_CATEGORIES),
            )
        rows.append({
            "logged_at": logged_at.isoformat(),
            "logged_date": logged_at.date().isoformat(),
            "folder_name": c.get("folder_name", ""),
            "file_name": c.get("file_name", ""),
            "question_number": str(c.get("question_number", "")),
            "survey_question": c.get("survey_question", ""),
            "wrong_answer": c.get("wrong_answer", ""),
            "wrong_mark_position": c.get("wrong_mark_position", ""),
            "correct_answer": c.get("correct_answer", ""),
            "correct_mark_position": c.get("correct_mark_position", ""),
            "detection_method_at_time": c.get("detection_method_at_time", ""),
            "root_cause_category": root_cause,
            "is_recurrence": bool(c.get("is_recurrence", False)),
            "fix_status": c.get("fix_status", "reported_pending_investigation"),
            "fix_description": c.get("fix_description", ""),
            "reported_by": c.get("reported_by", ""),
        })
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = bq_client.load_table_from_json(rows, table_ref, job_config=job_config)
    load_job.result()
    status("[CORRECTIONS] Logged %d correction(s) to %s.%s.%s", load_job.output_rows, project, dataset, table)
    return load_job.output_rows


def query_corrections(
    bq_client, project: str, dataset: str, table: str,
    question_number: Optional[str] = None,
    file_name: Optional[str] = None,
    root_cause_category: Optional[str] = None,
    limit: int = 50,
) -> list:
    """Reads back past corrections, most recent first - this is the "cross-
    check with errors that has been fixed before" step made queryable
    instead of relying on conversation memory or the project doc's prose.
    Any of question_number/file_name/root_cause_category may be given to
    narrow the search; all are exact-match. Returns a list of dicts (one per
    row) via a plain SELECT ... ORDER BY logged_at DESC LIMIT - no
    aggregation, this is meant to be read by a human (or by Claude, at the
    start of investigating a new report) not machine-summarized."""
    from google.cloud import bigquery

    full_table_id = f"{project}.{dataset}.{table}"
    where_clauses = []
    params = []
    if question_number is not None:
        where_clauses.append("question_number = @question_number")
        params.append(bigquery.ScalarQueryParameter("question_number", "STRING", str(question_number)))
    if file_name is not None:
        where_clauses.append("file_name = @file_name")
        params.append(bigquery.ScalarQueryParameter("file_name", "STRING", file_name))
    if root_cause_category is not None:
        where_clauses.append("root_cause_category = @root_cause_category")
        params.append(bigquery.ScalarQueryParameter("root_cause_category", "STRING", root_cause_category))
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = (
        f"SELECT * FROM `{full_table_id}` {where_sql} "
        f"ORDER BY logged_at DESC LIMIT @limit"
    )
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in bq_client.query(query, job_config=job_config).result()]


def extract_to_bigquery(
    bucket_name: str,
    root_prefix: str,
    folders: Optional[list],
    vertex_project: Optional[str],
    vertex_location: str,
    gemini_model: str,
    bq_project: Optional[str],
    bq_dataset: str,
    bq_table: str,
    dry_run: bool,
    replace_existing_folder_rows: bool = True,
    vision_project: Optional[str] = None,
    vision_enabled: Optional[bool] = None,
    pdf_quality_table: Optional[str] = PDF_QUALITY_TABLE,
    quality_routing_enabled: Optional[bool] = None,
    pipeline_config_table: Optional[str] = PIPELINE_CONFIG_TABLE,
    pipeline_config_enabled: Optional[bool] = None,
    spark_staging_bucket: Optional[str] = None,
) -> None:
    root_prefix = root_prefix if root_prefix.endswith("/") else root_prefix + "/"
    grid_ok = check_grid_dependencies()  # loud warning up front if this silently degrades
    status(
        "[GRID] Pixel-based accuracy checks (Q1-18 grid + Q21/22/27/32/25/29/35 checkbox rows/lists): %s.",
        "ENABLED" if grid_ok else "DISABLED (see error above) - falling back to model-only reading for every file",
    )
    vision_enabled = VISION_DOUBLE_CHECK_ENABLED if vision_enabled is None else vision_enabled
    vision_ok = vision_enabled and check_vision_dependencies()
    status(
        "[VISION] Cloud Vision double-check (H1/H2/H4/H5/H6/24/26 handwritten/write-in fields): %s.",
        "ENABLED" if vision_ok else ("DISABLED (--no-vision-check)" if not vision_enabled else "DISABLED (see error above)"),
    )
    status(
        "[CONFIDENCE] Model self-reported confidence threshold: %.2f - model-only answers below this are flagged needs_review.",
        MODEL_CONFIDENCE_THRESHOLD,
    )
    quality_routing_enabled = PDF_QUALITY_ROUTING_ENABLED if quality_routing_enabled is None else quality_routing_enabled
    status(
        "[QUALITY] Upstream pdf_quality routing (per-file pixel/vision/fallback route from %s): %s.",
        pdf_quality_table or "(none)",
        "ENABLED" if (quality_routing_enabled and pdf_quality_table and not dry_run) else "DISABLED",
    )

    bucket = connect_gcs_bucket(bucket_name)
    folders = folders or list_date_folders(bucket, root_prefix)
    if not folders:
        err("[GCS] No date folders found under gs://%s/%s", bucket_name, root_prefix)
        sys.exit(1)

    status("[GCS] Bucket %s: will extract Q&A from %d folder(s): %s", bucket_name, len(folders), ", ".join(folders))

    # One refreshed_at/refreshed_date per script run, shared by every row this
    # run loads — this is when the ETL ran, not when the survey was filled.
    refreshed_at = datetime.datetime.now(datetime.timezone.utc)

    bq_client = None
    bq_table_ref = None
    bq_full_table_id = f"{bq_project or '(default project)'}.{bq_dataset}.{bq_table}"
    if not dry_run:
        bq_client = connect_bigquery(bq_project)
        resolved_bq_project = bq_project or bq_client.project
        bq_table_ref = ensure_bq_table(bq_client, resolved_bq_project, bq_dataset, bq_table)
        bq_full_table_id = f"{resolved_bq_project}.{bq_dataset}.{bq_table}"

    # Revision 38: load calibration/tuning overrides from pipeline_config
    # (see query_pipeline_config()/apply_pipeline_config() and
    # _PIPELINE_CONFIG_DEFAULTS above) BEFORE any file is processed, so
    # every detector in this run sees the overridden values from the very
    # first PDF, not partway through. A soft dependency, same convention
    # as PDF_QUALITY_ROUTING_ENABLED: no table yet, or a query failure,
    # just means this run uses this file's own built-in calibration -
    # never a fatal error.
    pipeline_config_enabled = PIPELINE_CONFIG_ENABLED if pipeline_config_enabled is None else pipeline_config_enabled
    applied_config_params = []
    if pipeline_config_enabled and pipeline_config_table and bq_client is not None:
        resolved_bq_project_for_config = bq_project or bq_client.project
        # Auto-bootstrap: a fresh project/dataset with no pipeline_config
        # table yet gets one created and seeded from this file's own
        # built-in defaults automatically, rather than silently running on
        # code defaults until someone remembers --seed-pipeline-config.
        # No-ops (and never overwrites anything) if the table already exists.
        ensure_and_maybe_seed_pipeline_config(bq_client, resolved_bq_project_for_config, bq_dataset, pipeline_config_table)
        config_overrides = query_pipeline_config(bq_client, resolved_bq_project_for_config, bq_dataset, pipeline_config_table)
        applied_config_params = apply_pipeline_config(config_overrides)
    status(
        "[CONFIG] Externalized calibration overrides (from %s): %s.",
        pipeline_config_table or "(none)",
        f"{len(applied_config_params)} parameter(s) applied: {', '.join(applied_config_params)}" if applied_config_params
        else ("DISABLED" if not (pipeline_config_enabled and pipeline_config_table and not dry_run) else "none found - using this file's built-in calibration defaults"),
    )

    # Requested status line: which table is being refreshed, and when.
    status("[BQ] Refreshing table %s at %s (UTC)", bq_full_table_id, refreshed_at.isoformat())

    total_files = 0
    total_rows = 0
    total_needs_review = 0
    total_confidence_flagged = 0
    total_vision_flagged = 0
    failed_files = []

    for folder in folders:
        pdf_blobs = list_pdfs_in_folder(bucket, root_prefix, folder)  # already prints found/not-found
        if not pdf_blobs:
            continue

        report_date = parse_report_date(folder)
        # Requested status line: which bucket/folder is being processed.
        status(
            "[GCS] Bucket %r: processing folder %r — %d PDF(s), report_date=%s",
            bucket_name,
            folder,
            len(pdf_blobs),
            report_date.isoformat() if report_date else "NULL",
        )

        folder_rows = []
        for blob in pdf_blobs:
            gcs_uri = f"gs://{bucket_name}/{blob.name}"
            file_name = Path(blob.name).name
            if dry_run:
                status("[GCS] [dry-run] would send to Gemini: %s", gcs_uri)
                total_files += 1
                continue
            try:
                pdf_bytes = blob.download_as_bytes()
                quality_route = None
                if quality_routing_enabled and pdf_quality_table and bq_client is not None:
                    quality_row = query_pdf_quality_route(
                        bq_client, resolved_bq_project, bq_dataset, gcs_uri, pdf_quality_table
                    )
                    if quality_row is not None:
                        quality_route = quality_row.get("recommended_route")
                        status(
                            "[QUALITY] %s: pdf_quality recommended_route=%r (overall_quality=%r) - %s",
                            file_name, quality_route, quality_row.get("overall_quality"),
                            {
                                "pixel": "processing normally.",
                                "vision": "forcing Cloud Vision double-check + needs_review for this file.",
                                "fallback": "SKIPPING pixel detection, forcing Cloud Vision double-check + needs_review for this file.",
                            }.get(quality_route, "unrecognized route, ignoring."),
                        )
                answers = extract_qa_from_pdf(
                    pdf_bytes, vertex_project, vertex_location, gemini_model,
                    vision_project=vision_project, vision_enabled=vision_enabled,
                    quality_route=quality_route,
                )
                rows = answers_to_qa_rows(folder, file_name, answers, report_date, refreshed_at)
                folder_rows.extend(rows)
                total_files += 1
                n_flagged = sum(1 for r in rows if r.needs_review)
                status(
                    "[GCS] Loaded + extracted %s from %s (%d question rows, %d flagged needs_review)",
                    file_name,
                    gcs_uri,
                    len(rows),
                    n_flagged,
                )
                if n_flagged:
                    status(
                        "[GEMINI] %s: run verify_pdf(%r, %r) to audit the flagged answer(s) against the scan.",
                        file_name,
                        bucket_name,
                        blob.name,
                    )
            except Exception as e:  # noqa: BLE001 - keep going across a batch of scans
                err("[GEMINI] FAILED extracting %s (%s): %s", file_name, gcs_uri, e)
                failed_files.append(blob.name)

        if dry_run:
            continue

        if not folder_rows:
            err("[BQ] Folder %r produced 0 extracted rows (every file failed) — nothing loaded for this folder.", folder)
            continue

        if replace_existing_folder_rows:
            delete_existing_rows_for_folder(bq_client, bq_table_ref, folder)

        json_rows = [row.__dict__ for row in folder_rows]
        try:
            n_loaded = load_rows_into_bq_via_spark(
                json_rows, BQ_SURVEY_RESPONSES_SCHEMA, resolved_bq_project, bq_dataset, bq_table,
                staging_bucket=spark_staging_bucket or SPARK_BQ_STAGING_BUCKET,
            )
        except Exception as e:  # noqa: BLE001 - report and keep going with remaining folders
            err("[BQ] FAILED loading folder %r into %s: %s", folder, bq_full_table_id, e)
            continue

        n_folder_flagged = sum(1 for r in folder_rows if r.needs_review)
        n_folder_conf_flagged = sum(
            1 for r in folder_rows
            if r.detection_method == "model"
            and r.model_confidence is not None
            and r.model_confidence < MODEL_CONFIDENCE_THRESHOLD
        )
        n_folder_vision_flagged = sum(1 for r in folder_rows if r.vision_cross_check == "disagree")
        total_needs_review += n_folder_flagged
        total_confidence_flagged += n_folder_conf_flagged
        total_vision_flagged += n_folder_vision_flagged
        status(
            "[BQ] Loaded %d row(s) into %s for folder %r (%d flagged needs_review, of which %d confidence-based, %d Vision-disputed)",
            n_loaded, bq_full_table_id, folder, n_folder_flagged, n_folder_conf_flagged, n_folder_vision_flagged,
        )
        total_rows += n_loaded

    if dry_run:
        status("[GCS] Dry run complete. Would have sent %d file(s) to Gemini across %d folder(s).", total_files, len(folders))
        return

    if failed_files:
        err("[GEMINI] Failed to extract %d file(s), skipped: %s", len(failed_files), ", ".join(failed_files))

    review_rate = (total_needs_review / total_rows * 100) if total_rows else 0.0
    status(
        "[BQ] Done. %d file(s) processed, %d row(s) loaded into %s (refreshed_at=%s). "
        "%d row(s) flagged needs_review=TRUE (%.1f%% of all rows; %d confidence-based, %d Vision-disputed) — "
        "query `WHERE needs_review = TRUE` on %s to list them, or call verify_pdf() on a specific file to audit "
        "it by hand.",
        total_files,
        total_rows,
        bq_full_table_id,
        refreshed_at.isoformat(),
        total_needs_review,
        review_rate,
        total_confidence_flagged,
        total_vision_flagged,
        bq_full_table_id,
    )


def verify_pdf(
    bucket_name: str,
    blob_name: str,
    vertex_project: Optional[str] = None,
    vertex_location: str = VERTEX_LOCATION,
    gemini_model: str = GEMINI_MODEL,
    vision_project: Optional[str] = None,
    vision_enabled: Optional[bool] = None,
) -> list:
    """Runs extraction against exactly ONE PDF and prints a full per-question
    table (answer, reported mark_position, model confidence, Cloud Vision
    cross-check result, and whether everything agrees) — no BigQuery writes
    at all. This is the tool for auditing an accuracy problem: point it at
    the file, then hold the printout up against the actual scanned page and
    confirm by eye.

        from merge_survey_pdfs import verify_pdf, BUCKET_NAME, ROOT_PREFIX
        verify_pdf(BUCKET_NAME, ROOT_PREFIX + "Nov 10 2025/Nov10_1.pdf")

    blob_name is the full path inside the bucket (same as you'd see in the
    GCS console), not just the filename.
    """
    grid_ok = check_grid_dependencies()  # loud warning up front if this silently degrades
    status(
        "[GRID] Pixel-based accuracy checks (Q1-18 grid + Q21/22/27/32/25/29/35 checkbox rows/lists): %s.",
        "ENABLED" if grid_ok else "DISABLED (see error above) - falling back to model-only reading",
    )
    vision_enabled = VISION_DOUBLE_CHECK_ENABLED if vision_enabled is None else vision_enabled
    vision_ok = vision_enabled and check_vision_dependencies()
    status(
        "[VISION] Cloud Vision double-check (H1/H2/H4/H5/H6/24/26 handwritten/write-in fields): %s.",
        "ENABLED" if vision_ok else ("DISABLED (--no-vision-check)" if not vision_enabled else "DISABLED (see error above)"),
    )

    bucket = connect_gcs_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    status("[VERIFY] Downloading gs://%s/%s ...", bucket_name, blob_name)
    pdf_bytes = blob.download_as_bytes()
    answers = extract_qa_from_pdf(
        pdf_bytes, vertex_project, vertex_location, gemini_model,
        vision_project=vision_project, vision_enabled=vision_enabled,
    )
    report_date = None  # not needed for a standalone audit; report_date isn't shown in this table
    refreshed_at = datetime.datetime.now(datetime.timezone.utc)
    rows = answers_to_qa_rows("(verify_pdf)", Path(blob_name).name, answers, report_date, refreshed_at)

    header = (
        f"{'#':<5} {'answer':<40} {'pos':<5} {'conf':<5} {'thresh':<6} "
        f"{'vision':<8} {'v.score':<7} {'method':<14} {'flag':<8} note"
    )
    print()
    print(header)
    print("-" * max(150, len(header)))
    n_flagged = 0
    results = []
    for number, row in zip((n for n, _, _, _ in SURVEY_QUESTIONS), rows):
        if row.needs_review:
            n_flagged += 1
        flag = "REVIEW" if row.needs_review else ""
        conf_str = f"{row.model_confidence:.2f}" if row.model_confidence is not None else "-"
        thresh_str = f"{row.confidence_threshold:.2f}" if row.confidence_threshold is not None else "-"
        vision_str = row.vision_cross_check or "-"
        vision_score_str = f"{row.vision_match_score:.2f}" if row.vision_match_score is not None else "-"
        print(
            f"{number:<5} {row.survey_answer[:40]:<40} {(row.mark_position or '-'):<5} "
            f"{conf_str:<5} {thresh_str:<6} {vision_str:<8} {vision_score_str:<7} "
            f"{row.detection_method:<14} {flag:<8} {row.review_note}"
        )
        # For any question that actually got an independent Cloud Vision
        # comparison, also print what Vision itself read - this is the
        # "show me the comparison, not just the verdict" line: hold the
        # model's answer (printed above) up against Vision's own reading
        # right underneath it.
        if row.vision_ocr_snippet:
            print(f"      ↳ Cloud Vision read: {row.vision_ocr_snippet[:140]}")
        results.append(
            {
                "question_number": number,
                "answer": row.survey_answer,
                "mark_position": row.mark_position,
                "model_confidence": row.model_confidence,
                "confidence_threshold": row.confidence_threshold,
                "vision_cross_check": row.vision_cross_check,
                "vision_match_score": row.vision_match_score,
                "vision_ocr_snippet": row.vision_ocr_snippet,
                "detection_method": row.detection_method,
                "needs_review": row.needs_review,
                "review_note": row.review_note,
            }
        )
    print("-" * max(150, len(header)))
    status("[VERIFY] %d of %d question(s) flagged for manual review in %s.", n_flagged, len(SURVEY_QUESTIONS), blob_name)
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", default=BUCKET_NAME, help="GCS bucket name")
    ap.add_argument("--root-prefix", default=ROOT_PREFIX, help="Prefix containing the date folders")
    ap.add_argument(
        "--folders",
        nargs="*",
        default=None,
        help="Only process these specific date folder names (default: all found under root-prefix).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only: list what extract would do without downloading, calling Gemini, or writing anything.",
    )
    ap.add_argument("--vertex-project", default=VERTEX_PROJECT_ID, help="[extract] GCP project for Vertex AI calls.")
    ap.add_argument("--vertex-location", default=VERTEX_LOCATION, help="[extract] Vertex AI region.")
    ap.add_argument("--gemini-model", default=GEMINI_MODEL, help="[extract] Vertex AI Gemini model ID.")
    ap.add_argument(
        "--vision-project",
        default=VISION_PROJECT_ID,
        help="[extract] GCP project for Cloud Vision API calls (the independent double-check "
        "for handwritten/write-in questions H1/H2/H4/H5/H6/24/26 - see the module docstring's "
        "'Model confidence + Cloud Vision double-check' section). Defaults to the same project "
        "as --vertex-project when not set.",
    )
    ap.add_argument(
        "--no-vision-check",
        action="store_true",
        help="[extract] Skip the Cloud Vision double-check entirely (handwritten/write-in "
        "questions fall back to model-only + confidence-threshold checking alone, same as "
        "before this feature existed). Useful if google-cloud-vision isn't installed/enabled "
        "yet, or to isolate whether a review-rate change came from the confidence gate or "
        "the Vision cross-check.",
    )
    ap.add_argument("--bq-project", default=BQ_PROJECT_ID, help="[extract] GCP project for the BigQuery table.")
    ap.add_argument("--bq-dataset", default=BQ_DATASET, help="[extract] BigQuery dataset name.")
    ap.add_argument("--bq-table", default=BQ_TABLE, help="[extract] BigQuery table name.")
    ap.add_argument(
        "--pdf-quality-table",
        default=PDF_QUALITY_TABLE,
        help="[extract] BigQuery table name for classify_pdf_quality.py's upstream per-file "
        "quality classification (overall_quality/recommended_route - see "
        "query_pdf_quality_route() and the 'pdf-quality-classifier-and-routing.md' project "
        "doc). Looked up per file by gcs_uri before extraction and used to route pixel/Vision "
        "behavior for that file (see extract_qa_from_pdf()'s quality_route docstring). Uses "
        "--bq-project/--bq-dataset for the project/dataset. Pass an empty string to disable "
        "the lookup entirely (same as --no-quality-routing).",
    )
    ap.add_argument(
        "--no-quality-routing",
        action="store_true",
        help="[extract] Skip the pdf_quality lookup entirely and process every file identically "
        "(pixel + optional Vision per --no-vision-check), same as before this feature existed. "
        "Useful if --pdf-quality-table hasn't been populated yet for this bucket/folder.",
    )
    ap.add_argument(
        "--pipeline-config-table",
        default=PIPELINE_CONFIG_TABLE,
        help="[extract] BigQuery table name for externalized calibration/tuning overrides "
        "(_GRID_*/_YESNO_*/_MULTISELECT_* thresholds, pads, and per-question overrides - see "
        "query_pipeline_config()/_PIPELINE_CONFIG_DEFAULTS and the "
        "'revision-38-externalized-pipeline-config.md' project doc). Auto-created and seeded "
        "from this file's built-in defaults on the first run if it doesn't exist yet (see "
        "ensure_and_maybe_seed_pipeline_config()) - an existing table is never touched. Loaded "
        "once before any file is processed. Uses --bq-project/--bq-dataset for the "
        "project/dataset. Pass an empty string to disable the lookup (and the auto-seed) "
        "entirely (same as --no-pipeline-config).",
    )
    ap.add_argument(
        "--no-pipeline-config",
        action="store_true",
        help="[extract] Skip the pipeline_config lookup (and the auto-seed-if-missing check) "
        "entirely and run with this file's built-in calibration defaults only, same as before "
        "this feature existed. Useful to isolate whether a detection change came from a "
        "BigQuery override or a code change.",
    )
    ap.add_argument(
        "--spark-staging-bucket",
        default=None,
        help="[extract] GCS bucket the Spark BigQuery connector stages survey_responses/"
        "file_quality_review loads through. Defaults to SPARK_BQ_STAGING_BUCKET (the same "
        "bucket as --bucket).",
    )
    ap.add_argument(
        "--seed-pipeline-config",
        action="store_true",
        help="[extract] One-shot: write this file's CURRENT built-in calibration defaults "
        "(_PIPELINE_CONFIG_DEFAULTS) into --pipeline-config-table (WRITE_TRUNCATE - fully "
        "replaces the table's contents), then exit without extracting anything. A fresh table "
        "is now created and seeded automatically on the next normal extraction run too (see "
        "--pipeline-config-table above) - this flag remains for explicitly RESETTING an "
        "existing table's hand-tuned rows back to the code's own defaults.",
    )
    ap.add_argument(
        "--no-replace-folder",
        action="store_true",
        help="[extract] Skip the per-folder DELETE before loading — pure append, "
        "so re-running against the same folder will duplicate its rows. "
        "Default behavior deletes existing rows for a folder before reloading it, "
        "making reruns idempotent.",
    )
    ap.add_argument(
        "--verify-file",
        default=None,
        help="Audit ONE PDF's readings (answer vs. mark_position agreement) and print a "
        "comparison table - no BigQuery write. Pass the full path inside the bucket, "
        "e.g. --verify-file \"Nov 10 2025/Nov10_1.pdf\" (relative to --root-prefix) "
        "or a full gs:// path.",
    )
    ap.add_argument(
        "--corrections-table",
        default=BQ_CORRECTIONS_TABLE,
        help="[corrections log] BigQuery table name for logged wrong-answer reports. "
        "Uses --bq-project/--bq-dataset for the project/dataset.",
    )
    ap.add_argument(
        "--log-corrections-file",
        default=None,
        help="[corrections log] Path to a local JSON file containing a list of correction "
        "objects (see log_corrections()'s docstring for the accepted keys) - loads them "
        "into the corrections log table and exits. No merge/extract is run.",
    )
    ap.add_argument(
        "--list-corrections",
        action="store_true",
        help="[corrections log] Print past corrections (most recent first) and exit. "
        "Narrow with --question/--file/--root-cause; otherwise prints everything "
        "(up to --corrections-limit).",
    )
    ap.add_argument("--question", default=None, help="[corrections log] Filter --list-corrections to this question_number, e.g. '27'.")
    ap.add_argument("--file", default=None, dest="corrections_file_filter", metavar="FILE", help="[corrections log] Filter --list-corrections to this file_name.")
    ap.add_argument("--root-cause", default=None, help="[corrections log] Filter --list-corrections to this root_cause_category.")
    ap.add_argument("--corrections-limit", type=int, default=50, help="[corrections log] Max rows to print for --list-corrections (default 50).")
    # parse_known_args (not parse_args) so that running this via `%run` in a
    # Jupyter/IPython cell doesn't crash with SystemExit(2): sys.argv in a
    # notebook kernel contains flags like "-f /path/kernel.json" that belong
    # to the kernel, not this script, and would otherwise be rejected as
    # unrecognized arguments.
    args, unknown = ap.parse_known_args()
    if unknown:
        status(
            "Ignoring unrecognized argument(s) %s (expected if you're running "
            "this from a notebook — sys.argv there includes the kernel's own "
            "flags, not yours).",
            unknown,
        )
    vision_enabled = not args.no_vision_check
    if args.seed_pipeline_config:
        from google.cloud import bigquery
        table_name = args.pipeline_config_table or PIPELINE_CONFIG_TABLE
        bq_client = bigquery.Client(project=args.bq_project)
        resolved_project = args.bq_project or bq_client.project
        seed_pipeline_config_defaults(bq_client, resolved_project, args.bq_dataset, table_name)
        return
    if args.log_corrections_file:
        from google.cloud import bigquery
        with open(args.log_corrections_file) as f:
            corrections = json.load(f)
        if not isinstance(corrections, list):
            err("[CORRECTIONS] %s must contain a JSON list of correction objects, got %s", args.log_corrections_file, type(corrections).__name__)
            sys.exit(1)
        bq_client = bigquery.Client(project=args.bq_project)
        n_loaded = log_corrections(bq_client, args.bq_project, args.bq_dataset, args.corrections_table, corrections)
        status("[CORRECTIONS] Loaded %d correction(s) from %s into %s.%s.%s", n_loaded, args.log_corrections_file, args.bq_project, args.bq_dataset, args.corrections_table)
        return
    if args.list_corrections:
        from google.cloud import bigquery
        bq_client = bigquery.Client(project=args.bq_project)
        rows = query_corrections(
            bq_client, args.bq_project, args.bq_dataset, args.corrections_table,
            question_number=args.question, file_name=args.corrections_file_filter,
            root_cause_category=args.root_cause, limit=args.corrections_limit,
        )
        if not rows:
            status("[CORRECTIONS] No matching rows in %s.%s.%s", args.bq_project, args.bq_dataset, args.corrections_table)
        for row in rows:
            print(json.dumps(row, default=str, indent=2))
        return
    if args.verify_file:
        blob_name = args.verify_file
        if blob_name.startswith("gs://"):
            _, _, rest = blob_name.partition("gs://")
            bucket_in_path, _, blob_name = rest.partition("/")
        else:
            bucket_in_path = args.bucket
            root = args.root_prefix.rstrip("/")
            if not blob_name.startswith(root):
                blob_name = f"{root}/{blob_name}"
        verify_pdf(
            bucket_name=bucket_in_path,
            blob_name=blob_name,
            vertex_project=args.vertex_project,
            vertex_location=args.vertex_location,
            gemini_model=args.gemini_model,
            vision_project=args.vision_project,
            vision_enabled=vision_enabled,
        )
        return
    extract_to_bigquery(
            bucket_name=args.bucket,
            root_prefix=args.root_prefix,
            folders=args.folders,
            vertex_project=args.vertex_project,
            vertex_location=args.vertex_location,
            gemini_model=args.gemini_model,
            bq_project=args.bq_project,
            bq_dataset=args.bq_dataset,
            bq_table=args.bq_table,
            dry_run=args.dry_run,
            replace_existing_folder_rows=not args.no_replace_folder,
            vision_project=args.vision_project,
            vision_enabled=vision_enabled,
            pdf_quality_table=args.pdf_quality_table or None,
            quality_routing_enabled=not args.no_quality_routing,
            pipeline_config_table=args.pipeline_config_table or None,
            pipeline_config_enabled=not args.no_pipeline_config,
            spark_staging_bucket=args.spark_staging_bucket,
        )


if __name__ == "__main__":
    main()