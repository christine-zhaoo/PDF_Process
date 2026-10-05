#!/usr/bin/env python3
"""Generates the human-fillable "Pipeline Configuration" Excel workbook that
pipeline_config.py reads its settings and survey schema from.

Run this once to produce (or regenerate, e.g. after adding a new setting to
pipeline_config.py's own fallback defaults) the starting workbook, upload it
to GCS, then edit it there (via Sheets/Excel) for a new deployment - no code
change needed for a bucket/project/table/threshold change or a different
survey's questions.

Usage:
    ./.venv/bin/python generate_pipeline_config_doc.py [output.xlsx]

By default the workbook is PRE-FILLED with this deployment's CURRENT LIVE
values - read from the GCS-hosted workbook itself if it's reachable (so
regenerating never silently discards a value someone already changed there),
falling back to pipeline_config.py's own built-in defaults only for a value
that workbook doesn't have yet. To configure a NEW deployment from scratch,
edit the "Answer" column in Settings and the rows in "Survey Questions",
leaving anything you don't need to change alone.
"""
import argparse
import io
import json

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation

import pipeline_config as cfg

HEADER_FILL = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
HEADER_FONT = Font(color="FFFFFF", bold=True)
ANSWER_FILL = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
SECTION_FILL = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")
WRAP = Alignment(wrap_text=True, vertical="top")


def _style_header(ws, ncols, header_row=1):
    for c in range(1, ncols + 1):
        cell = ws.cell(row=header_row, column=c)
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
        cell.alignment = WRAP
    ws.freeze_panes = ws.cell(row=header_row + 1, column=1).coordinate


def _yn(value: bool) -> str:
    return "Y" if value else "N"


# --------------------------------------------------------------------------
# Pull whatever the LIVE workbook on GCS currently has, so regenerating this
# file (e.g. to add a new column) never overwrites a value someone already
# customized there with this file's own code-default fallback. Falls back to
# pipeline_config's built-in defaults for anything the live workbook doesn't
# have (a brand-new setting, or no workbook reachable at all yet).
# --------------------------------------------------------------------------
def _load_live_settings() -> dict:
    """Returns {Parameter: Answer} from the live GCS workbook's Settings
    sheet, or {} if it can't be read (first-ever run, no network, etc.) -
    never fatal, since this is only ever used to prefer a live value over
    this file's own hardcoded fallback."""
    try:
        from google.cloud import storage
        import pandas as pd

        client = storage.Client()
        data = client.bucket(cfg._CONFIG_GCS_BUCKET).blob(cfg._CONFIG_GCS_BLOB).download_as_bytes()
        df = pd.read_excel(io.BytesIO(data), sheet_name="Settings")
        out = {}
        for _, row in df.iterrows():
            name = str(row.get("Parameter", "")).strip()
            if not name or name == "nan":
                continue
            value = row.get("Answer", "")
            out[name] = "" if pd.isna(value) else value
        return out
    except Exception as e:  # noqa: BLE001 - best-effort only
        print(f"[generate_pipeline_config_doc] Could not read the live workbook to preserve its current values ({e}); using pipeline_config.py's built-in defaults instead.")
        return {}


_LIVE = _load_live_settings()


def _current(param_name: str, code_default):
    """The value to pre-fill a Settings row with: the live workbook's own
    current value if it has one (even if that's an intentionally blank
    string, e.g. VISION_PROJECT_ID left empty), else this file's built-in
    fallback default."""
    if param_name in _LIVE:
        return _LIVE[param_name]
    return code_default


# --------------------------------------------------------------------------
# Sheet: Settings - one row per pipeline_config.py scalar setting.
# "Parameter" is the exact name pipeline_config.py's loader matches on -
# don't rename it. "Answer" is the only column meant to be edited.
# "Where To Find This Value" tells a non-technical editor exactly where to
# look it up, or says plainly that it's an internal tuning knob with no
# real-world source (in which case: ask engineering, or don't touch it).
# --------------------------------------------------------------------------
SETTINGS_ROWS = [
    # --- Section: Where the scanned files live ---
    ("__SECTION__", "WHERE THE SCANNED FILES LIVE"),
    ("GCS_BUCKET", "What Google Cloud Storage bucket holds the scanned survey PDFs?",
     cfg.GCS_BUCKET, "Google Cloud Console → Cloud Storage → Buckets. The bucket name is the first segment after gs:// in any file's path there (e.g. gs://tps_survey/... → the bucket is 'tps_survey')."),
    ("GCS_RAW_PREFIX", "Inside that bucket, which folder holds the RAW scans as they come off the scanner, before this pipeline splits them into one file per survey (step 1's input)?",
     cfg.GCS_RAW_PREFIX, "Open the bucket in Cloud Storage and look at the folder structure; ask whoever uploads the scans which folder they drop new files into."),
    ("GCS_SPLIT_PREFIX", "Inside that bucket, which folder holds the SPLIT files - one PDF per survey - that step 1 produces and steps 2/3/4 read from?",
     cfg.GCS_SPLIT_PREFIX, "Same place as above - the folder step 1 writes its output into. Look for subfolders named by date (e.g. \"Nov 23 2025\")."),
    ("GCS_FEEDBACK_BUCKET", "What bucket holds the spreadsheet where a human reviewer's corrections are recorded (step 6's input)?",
     cfg.GCS_FEEDBACK_BUCKET, "Usually the same bucket as above, unless your team keeps reviewed feedback in a separate bucket - check with whoever runs the review step."),
    ("GCS_FEEDBACK_PREFIX", "Inside that bucket, which folder does step 4 write its reviewable feedback spreadsheets into, and step 6 read the latest one from?",
     cfg.GCS_FEEDBACK_PREFIX, "Cloud Storage → open the feedback bucket → look for a folder containing files named like \"tps_feedback_20251123_143000.xlsx\"; step 6 always picks the newest one automatically."),

    # --- Section: Where the processing runs ---
    ("__SECTION__", "WHERE THE PROCESSING RUNS (GOOGLE CLOUD PROJECT)"),
    ("GCP_PROJECT_ID", "Which Google Cloud project should all database (BigQuery), AI model (Vertex AI), and OCR (Cloud Vision) calls run against and be billed to?",
     cfg.GCP_PROJECT_ID, "Google Cloud Console, top navigation bar, next to the Google Cloud logo - click the project switcher and copy the Project ID (NOT the Project Name - they can differ)."),
    ("VERTEX_LOCATION", "Which Google Cloud region should the AI model calls run in?",
     cfg.VERTEX_LOCATION, "Vertex AI Console → top-right region/location selector. \"global\" is a valid, commonly-used choice and works without picking a specific region."),
    ("VISION_PROJECT_ID", "Which Google Cloud project should the Cloud Vision (OCR) calls run against? Leave blank to just reuse the project above.",
     cfg.VISION_PROJECT_ID or "", "Same place as GCP_PROJECT_ID above. Leave this cell blank unless your OCR calls specifically need to run under a different project/billing account."),

    # --- Section: Where results are stored ---
    ("__SECTION__", "WHERE RESULTS ARE STORED (BIGQUERY DATABASE)"),
    ("BQ_DATASET", "What BigQuery dataset (a named group of tables) holds every table this pipeline reads or writes?",
     cfg.BQ_DATASET, "Google Cloud Console → BigQuery → left-hand Explorer panel → expand your project → the dataset name shown there."),
    ("BQ_TABLE_MANIFEST", "Table name for step 1's list of which original scan produced which split survey file.",
     cfg.BQ_TABLE_MANIFEST, "BigQuery Console → Explorer panel → your project → your dataset → the list of table names."),
    ("BQ_TABLE_CALIBRATION", "Table name for step 2's per-file page-alignment/ink-darkness measurements.",
     cfg.BQ_TABLE_CALIBRATION, "Same place as above."),
    ("BQ_TABLE_QUALITY", "Table name for step 3's per-file scan-quality classification (clear / unclear / unreadable).",
     cfg.BQ_TABLE_QUALITY, "Same place as above."),
    ("BQ_TABLE_SURVEY_RESPONSES", "Table name for step 4's actual extracted answers - one row per question per survey. This is the pipeline's main output.",
     cfg.BQ_TABLE_SURVEY_RESPONSES, "Same place as above."),
    ("BQ_TABLE_CORRECTIONS", "Table name for the log of every correction a human reviewer has made to a wrong answer.",
     cfg.BQ_TABLE_CORRECTIONS, "Same place as above."),
    ("BQ_TABLE_PIPELINE_CONFIG", "Table name for a SEPARATE, engineering-only settings table step 4 checks at runtime (not the same thing as this workbook - don't confuse the two).",
     cfg.BQ_TABLE_PIPELINE_CONFIG, "Same place as above. Leave this at its default unless engineering asks you to change it."),
    ("BQ_TABLE_FILE_QUALITY", "Table name for step 5's per-file quality-review summary.",
     cfg.BQ_TABLE_FILE_QUALITY, "Same place as above."),
    ("BQ_TABLE_SURVEY_RESPONSES_WITH_FEEDBACK", "Table name for step 6's final output: the survey answers with every human correction already applied.",
     cfg.BQ_TABLE_SURVEY_RESPONSES_WITH_FEEDBACK, "Same place as above."),

    # --- Section: Which AI models to use ---
    ("__SECTION__", "WHICH AI MODELS TO USE"),
    ("TPS_EXTRACTION_MODEL", "Which Gemini model does step 1 use to read each scanned page's survey ID number and check whether the page is blank, declined, or in the wrong language?",
     cfg.TPS_EXTRACTION_MODEL, "Vertex AI Console → Model Garden → search \"Gemini\" → copy the exact model ID shown (e.g. gemini-3.8-flash). Ask engineering before changing this - a different model can change accuracy."),
    ("GEMINI_MODEL", "Which Gemini model does step 4 use to read every question's answer off the scanned form?",
     cfg.GEMINI_MODEL, "Same place as above. This is the single most important model in the whole pipeline - changing it changes the accuracy of every extracted answer."),
    ("STEP3_VISION_MODEL", "Which Gemini model does step 3 use for its own judgment call on whether a scan's handwriting is readable or the page is torn/damaged?",
     cfg.STEP3_VISION_MODEL, "Same place as above."),

    # --- Section: This survey's own details ---
    ("__SECTION__", "THIS SURVEY BATCH'S OWN DETAILS"),
    ("REPORT_YEAR", "What calendar year was this batch of surveys actually filled out/collected? Used to sanity-check handwritten dates (e.g. flag a date that reads as a clearly wrong year).",
     cfg.REPORT_YEAR, "Look at the scanned forms themselves, or ask whoever collected them. This should be a single 4-digit year, e.g. 2025."),

    # --- Section: Accuracy tuning (engineering knobs) ---
    ("__SECTION__", "ACCURACY TUNING (these have no real-world \"source\" - they are internal tuning knobs. Leave them at their defaults unless engineering specifically asks you to change one, e.g. because a particular question keeps getting flagged for review even when it's clearly correct)"),
    ("MODEL_CONFIDENCE_THRESHOLD", "If the AI model's own self-reported confidence for an answer (0 = totally unsure, 1 = totally sure) is BELOW this number, and nothing else confirms the answer, it gets flagged for human review.",
     cfg.MODEL_CONFIDENCE_THRESHOLD, "Internal tuning value - no external source. Raising it flags MORE answers for review; lowering it flags fewer."),
    ("VISION_FREEFORM_COVERAGE_THRESHOLD", "For any written answer (an address, a comment, a short code, an agency name, a handwritten date), how much of it (0-1) must Cloud Vision's independent OCR reading agree with before it's accepted as a match? One general threshold used for every written-text field.",
     cfg.VISION_FREEFORM_COVERAGE_THRESHOLD, "Internal tuning value - no external source."),
    ("VISION_DOUBLE_CHECK_ENABLED", "Should the pipeline use Cloud Vision (Google's OCR) as a second, independent check on every handwritten answer?",
     _yn(cfg.VISION_DOUBLE_CHECK_ENABLED), "Internal on/off switch - no external source. Leave as Y unless Cloud Vision access is broken or over quota."),
    ("PDF_QUALITY_ROUTING_ENABLED", "Should step 4 use step 3's scan-quality classification to decide how carefully to process each file?",
     _yn(cfg.PDF_QUALITY_ROUTING_ENABLED), "Internal on/off switch - no external source."),
    ("PIPELINE_CONFIG_ENABLED", "Should step 4 also check its own separate, engineering-only settings table at runtime?",
     _yn(cfg.PIPELINE_CONFIG_ENABLED), "Internal on/off switch - no external source. Leave as Y unless engineering asks otherwise."),
    ("FILE_EXTRACTION_WORKERS", "How many scanned files should step 4 process at the same time?",
     cfg.FILE_EXTRACTION_WORKERS, "Internal tuning value - no external source. A higher number finishes faster but uses more resources at once; ask engineering before increasing it a lot."),
]


# --------------------------------------------------------------------------
# Sheet: Survey Questions - one row per question. Rebuilding SURVEY_
# QUESTIONS plus every question-TYPE rule set from this sheet is what makes
# porting to a different survey PDF a spreadsheet edit instead of a code
# change.
#
# QUESTION TYPE (reviewed against every distinct way step4_process_pdf.py
# actually treats a question differently - not just "yes/no" vs "multi-
# select"):
#   - "Single Choice (pick one)" - a fixed list of choices, exactly one can
#     be marked (most questions on this form: Yes/No, a 5-point scale, etc.)
#   - "Multiple Choice (pick several)" - a fixed list of choices where more
#     than one can legitimately be marked at once (e.g. Race/Ethnicity).
#   - "Written Answer - Short Code or Number" - a handwritten field short
#     enough to be one word/token (an ID number, an age, a short code) -
#     Cloud Vision's OCR is compared word-for-word against it.
#   - "Written Answer - Longer Text" - a handwritten field that can span
#     multiple words (a name, an address, a date, an open comment) - Cloud
#     Vision's OCR is compared as a whole passage, tolerating minor
#     wording/spacing differences.
#   - "Written Answer - Not Checked by Cloud Vision" - a handwritten field
#     with no independent OCR double-check at all (the AI model's own
#     reading is trusted alone).
# Anything else about HOW a question's ink is physically located on the
# page (checkbox vs. circle, exact pixel position) is deliberately NOT
# configurable here - see pipeline_config.py's own module docstring for why.
# --------------------------------------------------------------------------
QUESTION_TYPES = [
    "Single Choice (pick one)",
    "Multiple Choice (pick several)",
    "Written Answer - Short Code or Number",
    "Written Answer - Longer Text",
    "Written Answer - Not Checked by Cloud Vision",
]

QUESTIONS_HEADER = [
    "Number", "Group Key", "Question Text",
    "Answer Choices (only for Single/Multiple Choice - separate each choice with a | pipe. A / slash INSIDE a choice is part of that choice's own text, e.g. 'Yes | No' but 'Unsure/Questioning/Don't know' is one single choice)",
    "Question Type",
    "Which page of the 2-page form is this on? (0 = first page, 1 = second page; leave blank for a Single/Multiple Choice question)",
    "If the AI's answer and the automatic checkbox-detector disagree, trust the AI's answer instead of the detector? (Y/N)",
    "OK to leave this question blank without flagging it for review? (Y/N)",
    "If Cloud Vision's OCR and the AI disagree on this written answer, trust Cloud Vision instead of the AI? (Y/N)",
]


def _question_type_for(number: str) -> str:
    if number in cfg.MULTI_SELECT_QUESTION_NUMBERS:
        return "Multiple Choice (pick several)"
    if number in cfg._VISION_TOKEN_FIELDS:
        return "Written Answer - Short Code or Number"
    if number in cfg._VISION_FREEFORM_FIELDS:
        return "Written Answer - Longer Text"
    choices = {n: c for n, _g, _s, c in cfg.SURVEY_QUESTIONS}.get(number, "")
    if not cfg.is_choice_list(choices):
        return "Written Answer - Not Checked by Cloud Vision"
    return "Single Choice (pick one)"


def _question_rows():
    for number, group_key, sub_text, choices in cfg.SURVEY_QUESTIONS:
        qtype = _question_type_for(number)
        yield [
            number,
            group_key or "",
            sub_text,
            # Always rewritten through join_choices() so a workbook generated
            # from a legacy " / " source comes out in the canonical, unambiguous
            # pipe form - see pipeline_config.CHOICE_SEPARATOR.
            cfg.join_choices(cfg.split_choices(choices)) if cfg.is_choice_list(choices) else "",
            qtype,
            cfg._WRITTEN_TEXT_QUESTION_PAGE.get(number, ""),
            _yn(number in cfg.MODEL_OVERRULES_PIXEL_QUESTION_NUMBERS),
            _yn(number in cfg._BLANK_ANSWER_EXEMPT_FIELDS),
            _yn(number in cfg._VISION_AUTHORITATIVE_FIELDS),
        ]


def build_workbook(path: str) -> None:
    wb = openpyxl.Workbook()

    # ---- Settings sheet ----
    ws = wb.active
    ws.title = "Settings"
    ws.append(["Parameter", "Question", "Answer", "Where To Find This Value"])
    for entry in SETTINGS_ROWS:
        if entry[0] == "__SECTION__":
            ws.append(["", entry[1], "", ""])
            r = ws.max_row
            for c in range(1, 5):
                ws.cell(row=r, column=c).fill = SECTION_FILL
                ws.cell(row=r, column=c).font = Font(bold=True)
            continue
        parameter, question, code_default, where = entry
        answer = _current(parameter, code_default)
        ws.append([parameter, question, answer, where])
        ws.cell(row=ws.max_row, column=3).fill = ANSWER_FILL
    widths = [38, 68, 26, 55]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            cell.alignment = WRAP
    _style_header(ws, len(widths))

    # ---- Survey Questions sheet ----
    ws2 = wb.create_sheet("Survey Questions")
    ws2.append(QUESTIONS_HEADER)
    for row in _question_rows():
        ws2.append(row)
        for col in (5, 7, 8, 9):
            ws2.cell(row=ws2.max_row, column=col).fill = ANSWER_FILL
    widths2 = [10, 14, 48, 48, 30, 20, 20, 18, 20]
    for i, w in enumerate(widths2, start=1):
        ws2.column_dimensions[get_column_letter(i)].width = w
    for row in ws2.iter_rows(min_row=2, max_row=ws2.max_row):
        for cell in row:
            cell.alignment = WRAP
    _style_header(ws2, len(widths2))

    # Dropdowns so a non-technical editor can't mistype a Question Type or a
    # Y/N answer - Excel shows a picker instead of a free-text cell.
    max_data_row = ws2.max_row + 200  # headroom for rows added later
    type_dv = DataValidation(
        type="list", formula1='"' + ",".join(QUESTION_TYPES) + '"', allow_blank=True, showDropDown=False,
    )
    type_dv.error = "Please pick one of the five listed question types."
    type_dv.errorTitle = "Not a valid Question Type"
    ws2.add_data_validation(type_dv)
    type_dv.add(f"E2:E{max_data_row}")

    yn_dv = DataValidation(type="list", formula1='"Y,N"', allow_blank=True, showDropDown=False)
    yn_dv.error = "Please pick Y or N."
    yn_dv.errorTitle = "Not a valid answer"
    ws2.add_data_validation(yn_dv)
    for col_letter in ("G", "H", "I"):
        yn_dv.add(f"{col_letter}2:{col_letter}{max_data_row}")

    # ---- Read Me First sheet ----
    ws3 = wb.create_sheet("Read Me First", 0)

    def _row(text="", bold=False, size=11, fill=None):
        ws3.append([text])
        cell = ws3.cell(row=ws3.max_row, column=1)
        cell.font = Font(bold=bold, size=size)
        cell.alignment = Alignment(wrap_text=True, vertical="top")
        if fill:
            cell.fill = fill
        return cell

    _row("Pipeline Configuration Workbook", bold=True, size=16)
    _row("")
    _row("WHAT THIS DOCUMENT IS", bold=True, size=12, fill=SECTION_FILL)
    _row(
        "This workbook controls how the survey-scanning pipeline runs, without anyone needing "
        "to edit any code. It is read directly from this exact file, every single time any part "
        "of the pipeline runs - there is no separate \"publish\" or \"deploy\" step. Save your "
        "changes to this file in this same location, and the very next run picks them up."
    )
    _row(
        "It has two parts: the 'Settings' tab (where files live, which Google Cloud project/"
        "database/AI models to use, and a handful of accuracy tuning numbers), and the 'Survey "
        "Questions' tab (the full list of every question on the form, its answer choices, and "
        "how it should be read/checked)."
    )
    _row("")
    _row("HOW THE PIPELINE USES THIS DOCUMENT, STEP BY STEP", bold=True, size=12, fill=SECTION_FILL)
    _row(
        "The pipeline turns a stack of scanned paper surveys into a clean spreadsheet-style "
        "database of every answer, in six steps. Each step is its own program, run one after "
        "another; every one of them reads its own settings straight out of this workbook first."
    )
    steps = [
        ("Step 1 - Split & Sort", "Takes the raw combined scans and splits them into one 2-page PDF per survey, sorted into dated folders.",
         "GCS_BUCKET, GCS_RAW_PREFIX, GCS_SPLIT_PREFIX, GCP_PROJECT_ID, BQ_TABLE_MANIFEST, TPS_EXTRACTION_MODEL, REPORT_YEAR"),
        ("Step 2 - Measure Each Scan", "Measures each split survey's page alignment and ink darkness, so later steps know exactly where each checkbox sits on THIS particular scan.",
         "GCS_SPLIT_PREFIX, GCP_PROJECT_ID, BQ_TABLE_CALIBRATION"),
        ("Step 3 - Grade Scan Quality", "Classifies each file as clearly readable, unclear, or unreadable, so step 4 knows which files need extra scrutiny.",
         "GCS_SPLIT_PREFIX, GCP_PROJECT_ID, BQ_TABLE_QUALITY, BQ_TABLE_CALIBRATION"),
        ("Step 4 - Extract Every Answer", "The main step: reads every question's answer off every scan using an AI model, double-checks handwritten answers with Google's OCR, and cross-checks against the measured checkbox ink from step 2. This is where almost every setting in this workbook, and the entire 'Survey Questions' tab, get used.",
         "Nearly everything - especially GEMINI_MODEL, all the accuracy tuning numbers, VISION_DOUBLE_CHECK_ENABLED, PDF_QUALITY_ROUTING_ENABLED, BQ_TABLE_SURVEY_RESPONSES, BQ_TABLE_CORRECTIONS, and the whole 'Survey Questions' tab"),
        ("Step 5 - Roll Up Quality", "Summarizes step 3/4's per-file quality findings into one reviewable report.",
         "GCP_PROJECT_ID, BQ_TABLE_FILE_QUALITY"),
        ("Step 6 - Apply Human Corrections", "Reads the LATEST reviewer's-corrections spreadsheet step 4 exported and applies it to the final, corrected results table.",
         "GCS_FEEDBACK_BUCKET, GCS_FEEDBACK_PREFIX, GCP_PROJECT_ID, BQ_TABLE_SURVEY_RESPONSES, BQ_TABLE_SURVEY_RESPONSES_WITH_FEEDBACK"),
    ]
    for title, desc, used in steps:
        _row(title, bold=True, size=11)
        _row(desc)
        _row(f"Settings this step uses: {used}")
        _row("")
    _row("")
    _row("HOW TO EDIT THE SETTINGS TAB", bold=True, size=12, fill=SECTION_FILL)
    _row(
        "Only edit the yellow 'Answer' column. Each row's 'Question' column explains what the "
        "setting does in plain language, and the 'Where To Find This Value' column tells you "
        "exactly where to look it up (a Google Cloud Console screen, the scanned forms "
        "themselves, etc.) - or says plainly that it's an internal tuning number with no "
        "real-world source, in which case leave it alone unless engineering asks you to change it."
    )
    _row("")
    _row("HOW TO EDIT THE SURVEY QUESTIONS TAB", bold=True, size=12, fill=SECTION_FILL)
    _row(
        "One row per question on the form. To port this pipeline to a DIFFERENT survey PDF "
        "entirely, replace this whole tab with that new form's questions - the pipeline rebuilds "
        "its entire understanding of the form from this tab alone, every run."
    )
    _row(
        "'Question Type' is the most important column - it tells the pipeline HOW to read and "
        "verify that question. Click any cell in that column to see the dropdown of the five "
        "valid types:"
    )
    for t in QUESTION_TYPES:
        _row(f"   • {t}")
    _row(
        "   • Single Choice (pick one): a fixed list of choices where exactly one should be "
        "marked - Yes/No questions, rating scales, etc. Fill in 'Answer Choices'."
    )
    _row(
        "   • Multiple Choice (pick several): a fixed list of choices where more than one "
        "can legitimately be marked at once (e.g. \"select all that apply\"). Fill in 'Answer "
        "Choices'."
    )
    _row("")
    _row("HOW TO WRITE 'ANSWER CHOICES'", bold=True, size=12, fill=SECTION_FILL)
    _row(
        "Separate each choice from the next with a | (pipe) character: \"Yes | No\", or "
        "\"Strongly Agree | Agree | I am Neutral | Disagree | Strongly Disagree | Not Applicable\"."
    )
    _row(
        "A / (slash) is NOT a separator - many real choices on this form contain their own "
        "slashes, and those must be typed exactly as printed on the paper form. For example, on "
        "the gender identity question, \"Female-to-Male (FTM)/Transgender Male/Trans Man\" is ONE "
        "single choice (one checkbox on the form), not three."
    )
    _row(
        "The quickest way to check you got it right: count the | pipes in the cell, add 1, and "
        "make sure that equals the number of checkboxes printed next to that question on the "
        "form. The ORDER matters too - list them in the same order they're printed, because the "
        "pipeline identifies a marked answer by its position in this list."
    )
    _row(
        "   • Written Answer - Short Code or Number: a handwritten field that's just one "
        "short word or number (an ID number, an age, a short code). Leave 'Answer Choices' blank."
    )
    _row(
        "   • Written Answer - Longer Text: a handwritten field that can span several words "
        "(a name, an address, a date, a comment box). Leave 'Answer Choices' blank."
    )
    _row(
        "   • Written Answer - Not Checked by Cloud Vision: a handwritten field where the "
        "AI's own reading is trusted with no independent OCR double-check. Leave 'Answer Choices' "
        "blank."
    )
    _row(
        "The exact PHYSICAL location of each checkbox/circle on the page (pixel coordinates) is "
        "deliberately NOT in this workbook - that has to be re-measured against the new form's "
        "actual print layout by an engineer (see step2_pdf_calibration.py), it can't be typed in."
    )
    _row("")
    _row("IF THIS FILE BECOMES UNREACHABLE", bold=True, size=12, fill=SECTION_FILL)
    _row(
        "Every pipeline step falls back to its own last-known-good built-in defaults and prints "
        "a warning, rather than failing outright. A fallback still WORKS, but it means whatever "
        "you last changed here isn't taking effect - treat that warning as something to fix "
        "(check the file wasn't moved/deleted, and that whoever is running the pipeline has "
        "access to it), not something to ignore."
    )
    _row("")
    _row("DO NOT RENAME", bold=True, size=12, fill=SECTION_FILL)
    _row(
        "'Parameter' in Settings, and 'Number' in Survey Questions - the pipeline matches rows by "
        "these exact column names/values. Every other column header can have its wording tweaked "
        "without breaking anything, but keep these two exactly as they are."
    )
    ws3.column_dimensions["A"].width = 110

    wb.save(path)
    print(f"Wrote {path}")


# --------------------------------------------------------------------------
# Automation: regenerating a NEW YEAR's workbook (e.g. a revised form like
# "TPS 2026 Adult English") without hand-editing pipeline_config.py's own
# SURVEY_QUESTIONS/REPORT_YEAR - those stay the current production
# defaults; a new year's few actual wording/choice changes are instead
# captured as a small, reviewable JSON overrides file (see --question-
# overrides below), applied to an in-memory copy of SURVEY_QUESTIONS just
# for this one build, then (optionally) uploaded straight to the same GCS
# folder the live workbook lives in.
# --------------------------------------------------------------------------
def apply_question_overrides(overrides: dict, full_replace: bool = False) -> None:
    """Mutates cfg.SURVEY_QUESTIONS in place.

    Two distinct modes, because a --question-overrides file can mean two
    very different things:

    - full_replace=False (default): PATCH mode. Replaces sub_text and/or
      choices for each question number present in `overrides` - every
      OTHER question (and every other column: group_key) is left exactly
      as-is. This is the right mode for a small wording revision that only
      touched a handful of questions - e.g. the original 2026 use case
      this function was built for, where a committed overrides file lists
      just the few questions that actually changed and everything else on
      the form stayed the same.

    - full_replace=True: REPLACE mode. The overrides file is treated as
      the COMPLETE, authoritative question list for this form revision -
      any existing question number NOT present in `overrides` is DROPPED
      entirely, not silently carried over from the previous form. This is
      the right mode for a genuinely redesigned/shorter form - explicit
      user request/real bug, confirmed on the 2027 form (only 23 real
      questions) after --question-overrides in the old default PATCH mode
      left questions 24-35 behind from the legacy 2025 form, producing a
      hybrid that doesn't match either form. --extract-from-pdf's own
      output is always a COMPLETE re-transcription of one form (every
      question Gemini found on that PDF, not a hand-picked diff), so it
      should almost always be paired with full_replace=True/--replace-
      questions unless you specifically know the new form is a strict
      superset of the old one's numbering.

    overrides: {question_number: {"question_text": str, "choices": str}} -
    either key is optional per question; omit one to leave that column
    unchanged (patch mode) or blank (replace mode, if truly omitted).
    A question_number in `overrides` with no corresponding existing
    SURVEY_QUESTIONS entry is still added (in replace mode) or ignored/
    logged (in patch mode, unchanged from before) - patch mode still never
    adds a genuinely new question row, since step4_process_pdf.py's pixel
    detectors have nothing calibrated for it yet (see pipeline_config.py's
    own module docstring); replace mode assumes the whole schema - and any
    new pixel calibration it needs - is being deliberately replaced."""
    known_numbers = {number for number, *_ in cfg.SURVEY_QUESTIONS}

    if full_replace:
        existing_by_number = {number: (group_key, sub_text) for number, group_key, sub_text, _choices in cfg.SURVEY_QUESTIONS}
        updated = []
        for number, override in overrides.items():
            group_key, old_sub_text = existing_by_number.get(number, (None, ""))
            sub_text = override.get("question_text", old_sub_text)
            choices = override.get("choices", "")
            updated.append((number, group_key, sub_text, choices))
        dropped = sorted(known_numbers - set(overrides), key=lambda n: (len(n), n))
        if dropped:
            print(f"[generate_pipeline_config_doc] --replace-questions: dropping question number(s) not present in the overrides file (not on this form revision): {dropped}")
        cfg.SURVEY_QUESTIONS = updated
        return

    unknown = set(overrides) - known_numbers
    if unknown:
        print(f"[generate_pipeline_config_doc] WARNING: --question-overrides mentions unknown question number(s) {sorted(unknown)} - ignoring them (pass --replace-questions if this overrides file is a COMPLETE new form, not a patch).")

    updated = []
    for number, group_key, sub_text, choices in cfg.SURVEY_QUESTIONS:
        override = overrides.get(number)
        if override:
            sub_text = override.get("question_text", sub_text)
            choices = override.get("choices", choices)
        updated.append((number, group_key, sub_text, choices))
    cfg.SURVEY_QUESTIONS = updated


def set_report_year(report_year: str) -> None:
    """Overrides cfg.REPORT_YEAR for this build AND pre-seeds _LIVE with it,
    so _current("REPORT_YEAR", ...) picks up the override too - without
    this, _load_live_settings() would still prefer whatever REPORT_YEAR the
    CURRENT live workbook happens to have saved (e.g. "2025"), silently
    discarding this override the same way it's designed to preserve every
    other already-customized Settings value."""
    cfg.REPORT_YEAR = report_year
    _LIVE["REPORT_YEAR"] = report_year


def upload_to_gcs(local_path: str, blob_name: str, bucket_name: str = None) -> str:
    """Uploads the just-built workbook to gs://{bucket_name}/{blob_name}
    (bucket_name defaults to cfg._CONFIG_GCS_BUCKET - the same bucket the
    live, currently-read workbook lives in). Returns the gs:// URI written.
    Pass a blob_name under the SAME folder as the live workbook (Pipeline_
    Config/...) to keep every year's workbook alongside each other, e.g.
    "Pipeline_Config/pipeline_configuration_2026.xlsx"."""
    from google.cloud import storage

    bucket_name = bucket_name or cfg._CONFIG_GCS_BUCKET
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(
        local_path,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    print(f"Uploaded {local_path} -> {gcs_uri}")
    return gcs_uri


# --------------------------------------------------------------------------
# Automation: generating a --question-overrides JSON file straight from a
# blank (or filled - ink is explicitly ignored, see the prompt below) scan of
# the form PDF, via Gemini, instead of a human hand-transcribing each
# question's exact wording/choices into JSON - explicit user request, after
# TWO separate hand-transcription bugs (Q24, Q29) both independently turned
# out to be a human paraphrasing the form's actual printed wording instead of
# copying it verbatim. This doesn't replace human review - see main()'s
# --extract-from-pdf, which only WRITES a JSON file for someone to read over
# and diff against pipeline_config.py before it's ever used as a
# --question-overrides input - but it means every question's starting text
# comes from an actual re-read of the real page image, not from memory/a
# paraphrase, which is exactly the class of bug both prior fixes were.
# --------------------------------------------------------------------------
def build_question_extraction_prompt() -> str:
    """The Gemini prompt for extract_survey_questions_from_pdf() - reads
    every page of a survey form PDF and transcribes each question's PRINTED
    text/choices/type into the exact JSON shape apply_question_overrides()
    expects, so its output can be saved straight to a --question-overrides
    file (after a human reviews it - see that flag's own help text)."""
    type_list = "\n".join(f"  - \"{t}\"" for t in QUESTION_TYPES)
    return (
        "You are transcribing a scanned, printed survey FORM TEMPLATE - a "
        "'Treatment Perceptions Survey' - into structured JSON. This PDF may "
        "be a blank template or a filled-in EXAMPLE with handwritten answers "
        "already written on it; if so, COMPLETELY IGNORE every handwritten "
        "mark, checkbox X, or written-in value anywhere on the page. You are "
        "transcribing only the form's own PRINTED text - the questions and "
        "their printed answer choices - never what a respondent wrote on it.\n\n"
        "THE SINGLE MOST IMPORTANT RULE: transcribe every question's text and "
        "every answer choice's text EXACTLY as printed on the page - the same "
        "words, in the same order, with the same punctuation and capitalization. "
        "Do NOT paraphrase, summarize, reorder, or reword ANYTHING, even "
        "slightly, even if a reworded version would read more naturally or "
        "grammatically. Do NOT change a question written in second person "
        "('you', 'your') into third person ('the respondent', 'their') or vice "
        "versa. Do NOT move a parenthetical note from after a question mark to "
        "before it, or drop it, or shorten it. If a question spans more than "
        "one printed line or sentence (e.g. a comment box's instructions, or a "
        "privacy notice below a question), include ALL of that text in "
        "'question_text', not just the first sentence. This exact failure mode "
        "- a human silently paraphrasing or truncating a question while "
        "transcribing it - is the reason this extraction step exists at all, "
        "so treat verbatim transcription as the only acceptable output, never "
        "a tidied-up or shortened rewrite.\n\n"
        "Go through the ENTIRE form, both the numbered questions (1, 2, 3, "
        "...) AND the unnumbered header fields at the top of page 1 (CalOMS "
        "Provider ID, Program Reporting Unit/Address, Setting, Field Based "
        "Services Agency, Field Based Services Address, Today's Date) - label "
        "these six header fields exactly \"H1\" through \"H6\" in that same "
        "order (H1=CalOMS Provider ID, H2=Program Reporting Unit/Address, "
        "H3=Setting, H4=Field Based Services Agency, H5=Field Based Services "
        "Address, H6=Today's Date), and every other question by its own "
        "printed number (\"1\", \"2\", ... \"35\", with no leading zero and no "
        "period).\n\n"
        "Return ONLY a JSON object - no other text, markdown, or commentary - "
        "shaped exactly like this, one key per question/field:\n"
        "{\n"
        '  "<question number or H1-H6>": {\n'
        '    "question_text": "<the question text, exactly as printed>",\n'
        '    "choices": "<see below>",\n'
        '    "question_type": "<see below>"\n'
        "  },\n"
        "  ...\n"
        "}\n\n"
        "\"question_type\" must be exactly one of these five values:\n"
        f"{type_list}\n\n"
        "\"choices\":\n"
        "  - For \"Single Choice (pick one)\" or \"Multiple Choice (pick "
        "several)\": every printed answer choice, in the exact order printed, "
        "joined with \" | \" (a space, a PIPE character, a space) between each "
        "one - e.g. \"Yes | No\" or \"Strongly Agree | Agree | I am Neutral | "
        "Disagree | Strongly Disagree | Not Applicable\". Transcribe each "
        "choice's own text exactly as printed too - do not shorten, "
        "reorder, or merge choices.\n"
        "    CRITICAL - use the pipe ONLY between separate choices, NEVER "
        "inside one: many individual choices contain their own forward "
        "slashes, and those slashes must be left exactly as printed. For "
        "example \"Female-to-Male (FTM)/Transgender Male/Trans Man\" is ONE "
        "single choice, and \"Unsure/Questioning/Don't know\" is ONE single "
        "choice - do NOT turn their internal slashes into pipes, and do NOT "
        "split them into several choices. A question's pipe count must equal "
        "its number of printed checkboxes minus one.\n"
        "  - For any of the three \"Written Answer\" types (no fixed choice "
        "list - a blank line, box, or space for handwriting instead): a "
        "short plain-English description of what belongs there instead of "
        "real choices, e.g. \"written agency name, if filled in\" or \"open "
        "text\" - never leave this field empty.\n\n"
        "Use \"Multiple Choice (pick several)\" only for a question whose own "
        "printed instructions say something like \"mark/select all that "
        "apply\" - every other fixed-choice question (even a long list of "
        "choices) is \"Single Choice (pick one)\". Use \"Written Answer - "
        "Short Code or Number\" for a short field meant to be one word/token "
        "(an ID number, an age, a short alphanumeric code); \"Written Answer "
        "- Longer Text\" for a field that can reasonably span multiple words "
        "(a name, an address, a date, an open comment box); \"Written Answer "
        "- Not Checked by Cloud Vision\" only if you have no other reasonable "
        "guess which of the other two fits."
    )


def extract_survey_questions_from_pdf(
    pdf_path: str,
    vertex_project: str = None,
    vertex_location: str = None,
    model: str = None,
) -> dict:
    """Renders every page of the PDF at pdf_path and sends them, together
    with build_question_extraction_prompt(), to Gemini in one call -
    returns the parsed JSON dict (see that prompt's docstring for the exact
    shape), ready to be reviewed and saved as a --question-overrides file
    (see main()'s --extract-from-pdf/--write-overrides).

    Defaults for vertex_project/vertex_location/model all come from
    pipeline_config.py (TPS_EXTRACTION_PROJECT/TPS_EXTRACTION_LOCATION/
    TPS_EXTRACTION_MODEL-equivalent settings) when not given explicitly, same
    convention as step1_merge_pdf.py/step4_process_pdf.py's own Gemini calls.

    Raises on any failure (a malformed/non-JSON response, an API error) -
    unlike this pipeline's OTHER Gemini call sites (which are mid-pipeline
    and must degrade gracefully), this is a standalone, human-supervised
    tool run once per form revision - surfacing a real failure directly is
    more useful here than silently returning an empty/partial result."""
    import pymupdf
    from google import genai
    from google.genai import types

    vertex_project = vertex_project or cfg.GCP_PROJECT_ID
    vertex_location = vertex_location or cfg.VERTEX_LOCATION
    model = model or cfg.TPS_EXTRACTION_MODEL

    with pymupdf.open(pdf_path) as doc:
        image_parts = [
            types.Part.from_bytes(
                data=page.get_pixmap(matrix=pymupdf.Matrix(2, 2), alpha=False).tobytes("png"),
                mime_type="image/png",
            )
            for page in doc
        ]

    client = genai.Client(vertexai=True, project=vertex_project, location=vertex_location)
    response = client.models.generate_content(
        model=model,
        contents=[*image_parts, build_question_extraction_prompt()],
        config=types.GenerateContentConfig(temperature=0, response_mime_type="application/json"),
    )
    return _normalize_extracted_questions(json.loads(response.text))


def _normalize_extracted_questions(extracted: dict) -> dict:
    """Reconciles Gemini's verbatim transcription with the two storage
    conventions SURVEY_QUESTIONS uses, so the result is directly usable as a
    --question-overrides file instead of a trap for whoever runs this:

    1. A question's own printed number is NOT stored in question_text -
       build_full_question_text() prefixes "<number>. " itself at read time,
       so leaving Gemini's (correct, verbatim) "24. Comment: ..." in place
       would render as "24. 24. Comment: ...".
    2. A trailing ":" on a field label or choice ("Setting:", "FBS Address:",
       "Other (specify):") is printed layout punctuation separating the label
       from its write-in box, not part of the label/choice text itself -
       step4_process_pdf.py matches a model's answer against the choice text,
       and an extra colon there is a gratuitous mismatch."""
    import re

    def _clean(text: str) -> str:
        return re.sub(r"\s*:\s*$", "", (text or "").strip()).strip()

    normalized = {}
    for number, entry in extracted.items():
        question_text = _clean(entry.get("question_text", ""))
        # Strip the question's own leading number ("24. ", "H3 ") if Gemini
        # included it - matched against THIS entry's own key, so a number that
        # genuinely belongs to the text (e.g. "AB109") is never touched.
        question_text = re.sub(rf"^{re.escape(number)}\s*[.)]?\s*", "", question_text).strip()
        # Re-joined through pipeline_config's own canonical separator, so the
        # written file always uses the unambiguous pipe form regardless of
        # which separator Gemini happened to emit.
        raw_choices = entry.get("choices", "") or ""
        parsed = cfg.split_choices(raw_choices)
        choices = cfg.join_choices([_clean(c) for c in parsed]) if parsed else _clean(raw_choices)
        normalized[number] = {
            "question_text": question_text,
            "choices": choices,
            "question_type": entry.get("question_type", ""),
        }
    return normalized


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example - regenerate the 2026 workbook from a committed overrides file "
            "and upload it alongside the live 2025 one:\n"
            "  ./.venv/bin/python generate_pipeline_config_doc.py pipeline_configuration_2026.xlsx "
            "--report-year 2026 --question-overrides tps_2026_question_overrides.json "
            "--upload-blob Pipeline_Config/pipeline_configuration_2026.xlsx"
        ),
    )
    ap.add_argument(
        "output", nargs="?", default="pipeline_configuration_2025.xlsx",
        help="Local path to write the workbook to.",
    )
    ap.add_argument(
        "--report-year", default=None,
        help="Override REPORT_YEAR for this build (e.g. '2026') - see set_report_year().",
    )
    ap.add_argument(
        "--question-overrides", default=None, metavar="FILE.json",
        help="Path to a JSON file of {question_number: {\"question_text\": ..., \"choices\": ...}} "
        "to apply over pipeline_config.py's current SURVEY_QUESTIONS before building - see "
        "apply_question_overrides(). Use this for a revised form's actual wording/choice changes "
        "instead of hand-editing pipeline_config.py. By default this PATCHES only the question "
        "numbers listed, leaving every other existing question untouched - pass --replace-"
        "questions if this file is a COMPLETE new form's question list, not a partial diff.",
    )
    ap.add_argument(
        "--replace-questions", action="store_true",
        help="Use with --question-overrides when that file is a COMPLETE, authoritative question "
        "list for a redesigned form (e.g. straight from --extract-from-pdf) rather than a partial "
        "wording patch - any existing question number NOT in the overrides file is DROPPED instead "
        "of silently carried over from the previous form. See apply_question_overrides()'s "
        "docstring for the full PATCH-vs-REPLACE distinction.",
    )
    ap.add_argument(
        "--upload-blob", default=None, metavar="Pipeline_Config/NAME.xlsx",
        help="If given, also upload the generated workbook to this blob path in --bucket "
        "(e.g. 'Pipeline_Config/pipeline_configuration_2026.xlsx') - same folder the live "
        "workbook lives in by convention, so pipeline_config.py's own _CONFIG_GCS_BLOB can "
        "later be pointed at it with just a one-line code change.",
    )
    ap.add_argument(
        "--bucket", default=None,
        help="GCS bucket --upload-blob uploads to. Defaults to pipeline_config.py's own "
        "_CONFIG_GCS_BUCKET (the same bucket the live workbook is read from).",
    )
    ap.add_argument(
        "--extract-from-pdf", default=None, metavar="FORM.pdf",
        help="Instead of building a workbook, read every question's printed text/choices/type "
        "straight off this survey form PDF with Gemini (see build_question_extraction_prompt()) "
        "and write the result to --write-overrides as a --question-overrides-shaped JSON file. "
        "Handwritten ink on a filled-in example form is ignored - only the printed template is "
        "transcribed. REVIEW the output before using it: this is a starting point that removes "
        "hand-transcription errors, not an unreviewed source of truth.",
    )
    ap.add_argument(
        "--write-overrides", default=None, metavar="OUT.json",
        help="Where --extract-from-pdf writes its JSON. Required with --extract-from-pdf.",
    )
    ap.add_argument(
        "--vertex-project", default=None,
        help="[--extract-from-pdf] GCP project for the Gemini call. Defaults to GCP_PROJECT_ID.",
    )
    ap.add_argument(
        "--vertex-location", default=None,
        help="[--extract-from-pdf] Vertex AI region. Defaults to VERTEX_LOCATION.",
    )
    ap.add_argument(
        "--gemini-model", default=None,
        help="[--extract-from-pdf] Gemini model ID. Defaults to TPS_EXTRACTION_MODEL.",
    )
    args = ap.parse_args()

    if args.extract_from_pdf:
        if not args.write_overrides:
            ap.error("--extract-from-pdf requires --write-overrides (where to save the JSON).")
        extracted = extract_survey_questions_from_pdf(
            args.extract_from_pdf,
            vertex_project=args.vertex_project,
            vertex_location=args.vertex_location,
            model=args.gemini_model,
        )
        with open(args.write_overrides, "w") as f:
            json.dump(extracted, f, indent=2, ensure_ascii=False)
            f.write("\n")
        print(
            f"Wrote {len(extracted)} question(s) to {args.write_overrides} - REVIEW this file "
            "(diff it against pipeline_config.py's SURVEY_QUESTIONS) before passing it to "
            "--question-overrides."
        )
        return

    if args.question_overrides:
        with open(args.question_overrides) as f:
            apply_question_overrides(json.load(f), full_replace=args.replace_questions)
    if args.report_year:
        set_report_year(args.report_year)

    build_workbook(args.output)

    if args.upload_blob:
        upload_to_gcs(args.output, args.upload_blob, bucket_name=args.bucket)


if __name__ == "__main__":
    main()
