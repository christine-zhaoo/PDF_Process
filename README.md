# LADPH Survey PDF Pipeline

This repo processes scanned LADPH survey PDFs end-to-end: organizing raw scans, measuring page geometry, running quality checks, extracting answers with a vision model, rolling up files that need manual review, and applying human corrections back onto the results. Each step reads/writes Google Cloud Storage (GCS) and BigQuery.

## Configuration — `pipeline_config.py`

Every step imports its settings — GCS bucket/prefixes, GCP project, BigQuery dataset/table names, Gemini/Vision model names, extraction thresholds, and the entire survey question schema (questions, answer choices, and per-question verification rules) — from `pipeline_config.py`, instead of hardcoding its own copy. This is what makes it possible to point the whole pipeline at a different bucket/project, or port it to a **different survey PDF entirely**, without touching any step's code.

`pipeline_config.py` itself downloads a human-editable Excel workbook from GCS (`gs://<bucket>/Pipeline_Config/pipeline_configuration.xlsx` — see `_CONFIG_GCS_BUCKET`/`_CONFIG_GCS_BLOB` at the top of the file) every time any step runs. The workbook contains:
- **Settings** — one row per scalar setting (bucket, project, table names, model names, thresholds), with a plain-language question, the current answer, and where to find that value.
- **Form Setup** — form identity, pages per survey, detection mode, language, decline keywords, and reviewed measurement settings.
- **Survey Questions** — one row per question, answer choices/type, page, optional format constraints, and optional Vision label anchors.
- **Control Calibration** — reviewer-approved question/choice-to-control coordinate mappings.
- **Calibration Review** — detected geometry suggestions, kept pending until a human maps and approves them.

The question schema is required: if the workbook cannot be read, the pipeline stops with an error rather than processing with an empty or stale schema. Older workbooks without the new optional form-profile tabs remain readable using legacy defaults.

Regenerate the workbook's starting point (e.g. after adding a brand-new setting to `pipeline_config.py`) with `generate_pipeline_config_doc.py`; it pre-fills from whatever the live workbook already has, so regenerating never silently discards someone's edits.

python3 generate_pipeline_config_doc.py pipeline_configuration_2026.xlsx \
  --report-year 2026 \
  --question-overrides tps_2026_question_overrides.json \
  --upload-blob Pipeline_Config/pipeline_configuration_2026.xlsx
  

Some detector algorithms remain specialized to known layouts, but approved registration marks, grid parameters, and control rectangles are workbook-driven. For a new form, start in `model_only` mode; geometry detection provides suggestions only. Do not activate `hybrid` detectors until question-to-control mappings have been reviewed and approved.

To start a new form from a local blank template, run:
```sh
python3 generate_pipeline_config_doc.py pipeline_configuration_new.xlsx --setup-from-pdf blank_form.pdf
```
Review every extracted question and page number, then map detected control rectangles to exact question/choice labels in **Control Calibration**. Mark each reviewed suggestion in **Calibration Review** `APPROVED`, and mark any runtime control mapping `APPROVED` before enabling `hybrid`. Keep `FORM_SETUP_STATUS` as `DRAFT` until the schema and geometry review are complete; the runtime loader refuses draft or partially reviewed profiles. The command can upload the draft workbook with `--upload-blob`, but uploading a draft does not make it runnable.

## Pipeline overview

```
step1_merge_pdf.py          step2_pdf_calibration.py     step3_pdf_quality_check.py     step4_process_pdf.py            step5_file_quality.py         step6_load_feedback.py
(organize/split scans)  ->  (measure page geometry)  ->  (per-file quality check)   ->  (extract answers)           ->  (needs_review rollup)      ->  (apply human corrections)
        |                           |                            |                              |                                |                                |
  GCS: source-name folders BQ: pdf_calibration_profile   BQ: pdf_quality               BQ: survey_responses            BQ: file_quality_review        BQ: survey_responses_with_feedback

                                                                         pipeline_config.py (settings + survey schema, read from a GCS-hosted Excel workbook)
                                                                                 ^ every step above imports from this
```

### Step 1 — `step1_merge_pdf.py`: Organize & split scans
Processes PDFs in GCS without depending on their naming/date format. Step 1 splits each source into units using configured `PAGES_PER_SURVEY` (two by default) and stores them in a folder named after the original PDF's filename stem (for example, `TPS 2026 Adult English_Filled/TPS 2026 Adult English_Filled_p1.pdf`). The current date is recorded in the manifest, not used as an output folder.

- **Input:** loose PDFs under `gs://<bucket>/<root-prefix>/` (defaults from `pipeline_config.py`'s `GCS_BUCKET`/`GCS_RAW_PREFIX`)
- **Output:** reorganized PDFs in per-source-name folders; a manifest loaded to BigQuery (default table `pdf_manifest_list`, from `pipeline_config.py`'s `BQ_TABLE_MANIFEST`, partitioned by `moved_date`); optional failure log CSV (`step1_failed_sources*.csv`)
- **Usage:**
  ```
  python step1_merge_pdf.py --bucket <bucket> --root-prefix <prefix> [--year 2025] [--dry-run] \
      [--bq-project <p>] [--bq-dataset <d>] [--manifest-table pdf_manifest_list] [--no-manifest-table] \
      [--loose-only] [--only-file NAME] [--only-pages START-END] [--max-surveys N] \
      [--failure-log FILE] [--self-test]
  ```
- **Key deps:** `google-cloud-storage`, `pypdf`, `google-cloud-bigquery`, `pandas`, `pyspark`

### Step 2 — `step2_pdf_calibration.py`: Page geometry calibration
Measures each page's registration-mark transform, per-question box geometry, and per-file ink threshold. This geometric calibration is reused by the quality check (step 3) and extraction (step 4) steps.

- **Input:** PDFs under a local folder or GCS prefix (organized output of step 1)
- **Output:** one row per file to BigQuery table `pdf_calibration_profile` (partitioned by `partition_date`); local sample: `generated_calibration.json`
- **Usage:** no CLI — imported and called directly:
  ```python
  import step2_pdf_calibration as step2
  step2.run(root_uri, table=step2.BQ_TABLE, project=None, dpi=step2.RENDER_DPI,
            write_disposition="WRITE_APPEND", verbose=True, dry_run=False)
  ```
- **Key deps:** `numpy`, `google-cloud-storage`, `google-cloud-bigquery`

### Step 3 — `step3_pdf_quality_check.py`: Per-file quality check
Reads every PDF and writes one QC row per file: `overall_quality` (clear / unclear / totally_unreadable) and `recommended_route` (pixel / vision / fallback). Primarily reuses step 2's `pdf_calibration_profile` geometry; if a file is missing there, it self-measures inline and raises an `[ALERT]`.

- **Input:** PDFs under a folder/GCS prefix; optionally BigQuery `pdf_calibration_profile` (step 2)
- **Output:** rows in BigQuery `pdf_quality` (partitioned by `partition_date`)
- **Usage:** no CLI — call directly:
  ```python
  import step3_pdf_quality_check as step3
  step3.run(uri, vision=True)      # or
  step3.run("./pdfs", dry_run=True)
  ```
- **Key deps:** `numpy`, `google-cloud-bigquery`; optionally `vertexai` / `google-cloud-aiplatform` for vision-based judging

### Step 4 — `step4_process_pdf.py`: Extract answers
The largest and core script: reads each survey PDF with a Vertex AI Gemini vision model (checkboxes/handwriting aren't in the text layer), with optional approved pixel calibration and Google Cloud Vision checks for configured written fields, flagging rows that `needs_review`. New form profiles default to `model_only`; pixel geometry is used only after reviewers approve the mappings and select hybrid mode. Files within a folder are extracted concurrently (`FILE_EXTRACTION_WORKERS` worker threads, from `pipeline_config.py`). Also owns the human-feedback export/sync (`--export-needs-review-feedback`, ingested by step 6 — see Commands below).

- **Input:** PDFs in source-name folders (output of step 1) in GCS; optionally step 2's `pdf_calibration_profile` as a per-file pixel-detection hint, and step 3's `pdf_quality` for routing
- **Output:** rows in BigQuery `survey_responses` (columns include `survey_question`, `survey_answer`, `mark_position`, `needs_review`, `detection_method`, `model_confidence`, `vision_cross_check`)
- **Usage:**
  ```
  python step4_process_pdf.py --bucket <bucket> --root-prefix <prefix> \
      --vertex-project <p> --vertex-location <loc> --gemini-model <model> \
      --bq-project <p> --bq-dataset <d> --bq-table survey_responses \
      [--no-vision-check] [--question N] [--file NAME] [--root-cause CATEGORY] \
      [--corrections-limit N] [--list-corrections]
  ```
- **Key deps:** `google-cloud-storage`, `google-genai` (Vertex AI/Gemini), `google-cloud-bigquery`, `google-cloud-vision`, `pypdf`, `pymupdf` (fitz), `opencv-python-headless`, `numpy`

### Step 5 — `step5_file_quality.py`: Needs-review rollup
Reads BigQuery's `survey_responses` table (from step 4) and builds a `file_quality_review` table — one row per source file summarizing all `needs_review=TRUE` questions for quick triage.

- **Input:** BigQuery `survey_responses`, optionally filtered by `--folders`
- **Output:** BigQuery `file_quality_review` (partitioned by `refreshed_date`), replacing rows for processed folders each run
- **Usage:**
  ```
  python step5_file_quality.py --bq-project <p> --bq-dataset <d> \
      --survey-responses-table survey_responses --file-quality-table file_quality_review [--dry-run]
  ```
  Can also be called directly as `run(...)` from a Python shell (argparse doesn't work well in Jupyter).
- **Key deps:** `google-cloud-bigquery`, `pandas`, `pyspark` (with Spark BigQuery connector jar)

### Step 6 — `step6_load_feedback.py`: Apply human corrections
Ingests the latest timestamped feedback workbook using the configured `FEEDBACK_FILE_PREFIX` that step 4's `--export-needs-review-feedback` wrote, and applies any filled-in `correct_answer` cells to `survey_responses_with_feedback` (never to `survey_responses` itself). Idempotent: a row already marked `updated_with_feedback` is matched but left untouched, so re-running against an old/already-ingested file is always safe; a row a reviewer left blank is also left untouched (not marked reviewed), so it's exported again next time instead of being silently dropped.

- **Input:** the latest `gs://<feedback-bucket>/<feedback-prefix><FEEDBACK_FILE_PREFIX>*.xlsx` file; BigQuery `survey_responses` (synced from, never modified)
- **Output:** BigQuery `survey_responses_with_feedback` — `survey_answer`/`correct_answer` overwritten, `updated_with_feedback=TRUE`, `feedback_updated_time`, `ingested_from` set on every row actually applied
- **Usage:**
  ```
  python step6_load_feedback.py [--bq-project <p>] [--bq-dataset <d>] \
      [--survey-table survey_responses] [--feedback-table survey_responses_with_feedback] \
      [--feedback-bucket <bucket>] [--feedback-prefix <prefix>]
  ```
- **Key deps:** `google-cloud-storage`, `google-cloud-bigquery`, `pandas`, `openpyxl` (reuses `step4_process_pdf.py`'s own GCS/BigQuery connection helpers and schema)

## Detailed logic & rules

This section documents the actual decision rules, thresholds, and edge-case handling in each script, for maintainers who need to tune or debug the pipeline. The original TPS form's fixed-layout calibration and recognition rules remain as a legacy fallback for workbooks without a reviewed Form Setup profile. New form profiles use workbook-specified questions, pages, language, validation rules, anchors, and approved control geometry; they start in `model_only` mode.

### Step 1 — `step1_merge_pdf.py`

Step 1 does not extract or use a handwritten survey ID. It groups pages by configured `PAGES_PER_SURVEY`, preserves the input PDF stem in its output path, and records the run date only in the manifest. The language check uses `SURVEY_LANGUAGE`; a mismatch or failed check is retained for human review rather than used as a file-name/date filter. Step 1's Gemini model, language retry count, and retry delay are configurable in the Settings sheet.

**Splitting logic and naming.** Every input PDF is split into fixed, non-overlapping units of configured `PAGES_PER_SURVEY` (2 by default); source filename patterns and dates are not used to decide whether or how to split. Each output keeps the source stem as its enclosing folder and gets a one-based survey index. For example, `TPS 2026 Adult English_Filled.pdf` produces `TPS 2026 Adult English_Filled/TPS 2026 Adult English_Filled_p1.pdf` for source pages 1–2 when the setting is 2. The current date is recorded in the manifest only. If the source page count is not divisible by the configured page count, that source is recorded as unsplittable.

**Content checks (per survey unit).** Each configured survey unit continues through the language and blank/declined-content checks. Outputs are routed to the normal, `Declined/`, or `Rejected/` category locations and the manifest records the source page range and any review/rejection reason.

**Failure logging (`step1_failed_sources*.csv`).** Rows are logged for unsplittable source PDFs, rejected survey units, and unhandled per-source errors. Filenames without a date are not failures.

**Dry-run.** Runs the content checks and computes the manifest but skips GCS uploads and BigQuery writes. It also bypasses the already-processed optimization, so each PDF is evaluated again.

**Key thresholds:** language checks use `STEP1_CHECK_MAX_ATTEMPTS` and `STEP1_CHECK_RETRY_DELAY_SECONDS`; `SOURCE_PDF_WORKERS=4` concurrent source PDFs; `MAX_CONCURRENT_VERTEX_CALLS=4`.

**Manifest schema (`organize_manifest`/`pdf_manifest_list`):** `source_file, source_gcs_uri, parsed_month_day, destination_folder, destination_gcs_uri, moved_at, moved_date (partition), source_page_range, rejected, rejected_reason, needs_review, needs_review_reason`. Re-running replaces rows only for the same `(source_gcs_uri, source_page_range)`, not the whole file/folder.

### Step 2 — `step2_pdf_calibration.py`

**Registration marks.** Detected via connected-component analysis on a binarized page (`INK_THRESHOLD=165`): a mark candidate must be roughly square (74±28px), ≥75% filled, with exactly one candidate per page quadrant — any quadrant with 0 or >1 candidates means "not found" (no guessing). If marks aren't found, that page gets no transform and no question geometry (other pages/files are unaffected).

**Transform.** A similarity transform (uniform scale + rotation + translation, no shear) is fit from the 4 baseline mark positions to this page's detected marks, via a least-squares complex-number solve. The fit residual (max pixel deviation when mapping the baseline marks back through the transform) is stored; if `> 8px`, the page's boxes are still measured but flagged as suspect in `warnings`.

**Question box geometry.** A fixed template (measured once from a reference file `Nov1_7_TPS_4223.pdf`) gives expected box positions in template space; per-page, these are projected through the page's transform, then `locate_box()` searches a small window (±8px) for an actual closed rectangle (requires all 4 sides ≥60% inked, size 18-55px) to "confirm" the box rather than trusting pure geometry. Three questions (31, 33, 34) get a manual `(dx,dy)` correction since their baseline was measured from a different reference file. Questions with multiple possible printed layouts (e.g. Q27) resolve to whichever layout has the most confirmed boxes.

**Ink threshold.** Per file, confirmed boxes' ink ratios are clustered into "blank" vs. "marked" by finding a gap between sorted ratios. Legacy workbooks retain the TPS bounds (8–25 plausible marked boxes and at least 12 measured boxes). Configured profiles derive the sample minimum and plausible marked-count bounds from the approved checkbox geometry, so small forms are not forced to meet TPS box counts. A gap ≥0.08 is `clear`; otherwise it is `tight` and unsafe to threshold on. This is diagnostic output for Steps 3/4, not fed back into Step 2's fixed `INK_THRESHOLD=165` binarization.

**Failure handling:** missing marks/boxes/threshold degrade gracefully per-page/per-box (never aborts the file); a whole-file exception produces a stub all-null row rather than dropping the file. No PDFs found under the root raises `FileNotFoundError`.

**Output:** one row per file to `pdf_calibration_profile` (partitioned by `partition_date`), including per-page scale/rotation/offset/residual, ink threshold stats, `boxes_confirmed`/`boxes_total`, and a JSON blob of every question's confirmed pixel rects. `write_disposition` defaults to `WRITE_APPEND` (never truncates the partitioned table). `dry_run=True` returns the DataFrame without any BigQuery write.

### Step 3 — `step3_pdf_quality_check.py`

**Classification** happens in a fixed priority order so `overall_quality` and `recommended_route` never disagree:
```
pixel_unreadable        → overall_quality="totally_unreadable", route="fallback"
questionable / ≥5 flags → overall_quality="unclear",             route="vision"
any other reason        → overall_quality="unclear",             route="pixel"
no reasons               → overall_quality="clear",               route="pixel"
```
`needs_review = route != "pixel"`.

- **pixel_unreadable** triggers: ink marks unreadable (no clean blank/marked split), box borders unreadable, page tilt >0.5°/unmeasurable, registration marks missing on any page, or fit residual >8px.
- **questionable** triggers: ink marks in the "tight" (ambiguous) zone, marks found outside their box, or (if vision ran) handwriting rated questionable/unreadable or physical tear/damage detected.
- Individual field rules: faint marks if ink ratio <0.30; shadow present if shadow coverage ≥1% across ≥6 of 20 bands; borders readable only if ≥95% of expected borders are found; faint border if border coverage <70%.

**Self-measurement fallback.** Primarily looks up the file's row in `pdf_calibration_profile` (step 2's output). If missing (or the BigQuery query fails for any reason — no client, no table, network error), it falls back to an inline, functionally-identical re-implementation of step 2's own geometry/ink logic, sets `calibration_source="self-measured"`, and logs one of three distinct `[ALERT]` messages depending on the failure point. This always adds a reason to the file's flags, which alone can push `overall_quality` from clear to unclear.

**Vision judging** (Gemini, only when `vision=True`) only runs for files whose *pixel-only* classification isn't already "clear" (cost-saving; can be forced on for every file via `vision_only_flagged=False`). The prompt explicitly restricts Gemini to two judgments only — handwriting readability and physical damage — since all other measurements are already trusted pixel data the model is told not to re-judge. Vision results can only push a file toward "unclear"/vision-routing; they never override a pixel-level `fallback` classification. A vision API failure never crashes the file — it's recorded as `vision_error` and counted as a flag.

**Output:** one row per file to `pdf_quality` (partitioned by `partition_date`) with the full set of per-field ratings, raw measurements, and boolean trigger columns for auditability. `dry_run=True` skips the BigQuery write.

### Step 4 — `step4_process_pdf.py`

**Legacy note:** the module docstring below still contains historical TPS-specific implementation notes. The current configured workflow reads individual PDFs, uses the approved form profile and question metadata, and does not merge source PDFs.

**Folder/file discovery.** Step 4 finds PDF-containing directories below `GCS_INPUT_PREFIX`; current Step 1 output is one source-named folder per PDF (for example, `TPS 2026 Adult English_Filled/`). It groups each PDF's BigQuery rows by its immediate parent folder, so a targeted rerun replaces only that source survey's rows. Older nested date/batch layouts remain discoverable. Loose PDFs directly under the input prefix are skipped and logged. PDFs within each folder are sorted in natural order. A date-like folder name supplies `report_date`; for other folder names, Step 4 uses the current run date.

**Extraction — vision model.** Each page is rendered to a 300-DPI PNG (not sent as raw PDF — native PDF ingestion misread dense checkbox tables) and sent to Gemini (`pipeline_config.GEMINI_MODEL`) with a 16-rule prompt covering: only count actual ink (not printed outlines/creases), one answer for single-select vs. multiple for Q33/Q34, blank is a valid answer, "(specify)" free text gets appended to the answer, position-then-label reading order for the 6-point scale (to prevent self-contradiction), a required one-sentence reasoning per question (surfaces later in `review_note`), strict `mark_position` format, and a 0.0-1.0 self-reported `confidence` the model is told not to inflate. H1 must be exactly 6 digits; H6 must be MM/DD/YYYY.

**Pixel-based deterministic reader.** Runs independently of the model, using step 2/3's calibrated geometry, to catch cases where Gemini is confidently self-consistent but wrong:
- **Checkbox grid (Q1-18):** locates 18 ruled rows by line spacing (45-125px band), measures ink density per of 6 columns per row; a row reads "blank" only if density is near-zero AND the margin between top two candidates is near-zero; a column with implausibly high density suggests a correction/cross-out.
- **Yes/No & single-choice boxes (10 questions):** fixed calibrated coordinates, re-validated per scan; stricter confidence margin (0.15 vs. the grid's 0.05) since fewer boxes are being compared.
- **H3 circles:** margin threshold 0.8 (real margins run 0.95-1.0, so this is intentionally generous).
- **Multi-select (Q33/Q34):** measures every checkbox's ink ratio independently (not winner-take-all); below 0.10 = confidently blank, at/above = confidently marked — this can silently veto a model-claimed choice or add a model-missed one.

All pixel thresholds are centrally defined and can be overridden at runtime from a BigQuery `pipeline_config` table without a code deploy.

**Calibration-profile hints (`PDF_CALIBRATION_ROUTING_ENABLED`, default on).** `query_pdf_calibration_profile()` looks up this file's newest row in step 2's `pdf_calibration_profile` table and passes it into the pixel detectors as an optional, additive hint (a missing/failed lookup falls back to this file's pre-existing, unchanged behavior) — added after tracing real Q20/23/25/29 pixel errors on one file back to information step 2 already measures but step4 never consulted:
- **Registration offset seed** (`offset_dx_p1/dy_p1/dx_p2/dy_p2`, same 300-DPI pixel space step4 already works in): tried as the first shift candidate in `_locate_yesno_list_shift()`/`detect_multiselect_ink_ratios()`'s anchor search, ahead of the checkbox-shape-based guess — a real, measured offset from this scan's own printed registration marks is stronger evidence than shape-matching against repeating, identically-sized checkbox glyphs. Still only adopted if it independently validates against the real box at the normal narrow pad.
- **Per-question confirmed rects** (`question_rects`, from `question_rects_json`): when step 2 has a *fully*-confirmed set of box positions for one question on this file (`boxes_confirmed == boxes_total`), those rects are used to **replace** step4's own calibrated coordinates outright for that question, skipping the shift search entirely — needed because the offset seed alone is a single page-wide shift and can't correct per-row/per-column jitter beyond that (e.g. a two-column layout like Q31).
- **Relaxed ink floor for "tight" files** (`ink_quality`, `ink_mark_lo`): `_locate_yesno_list_shift()`'s shift-confirmation pass normally requires a candidate box's ink ratio to clear a fixed `_YESNO_LIST_ANCHOR_GENUINE_MARK_FLOOR=0.40` to count as "trustworthy"; when step 2 flagged this file's own blank/marked ink separation as `"tight"`, the floor is lowered to just under this file's own measured `ink_mark_lo` instead of blindly trusting the global default.
- `_locate_yesno_list_shift()` also now always scores the calibration-seeded candidate and the native `(0, 0)` no-shift hypothesis explicitly (checked before any anchor-derived candidate), rather than only ever comparing anchor-derived shifts against each other — a real box whose own border-tracing anchor search fails (e.g. a checkmark stroke crossing and corrupting a box's printed border) used to silently drop out of both the "candidate" and "corroborating" roles, letting a wrong shift win by default even when the file needed no shift at all.

**Quality-route / calibration-profile lookup fallback.** Both `query_pdf_quality_route()` and `query_pdf_calibration_profile()` now fall back to a `file_name`-only match when no row matches the file's exact `gcs_uri` — needed once step 1's batch-subfolder reorganization changed every file's path out from under rows written by an earlier step 2/3 run, which otherwise silently returned no hint at all with no visible error. Logged clearly as a fallback either way; same file-name-not-globally-unique caveat as the exact-match case applies.

**Cross-checks, in order:**
1. **Self-consistency** — does the model's own answer text match its own `mark_position` for that same call.
2. **Pixel vs. model** — if the pixel reader resolved an answer above its confidence margin, it overwrites the model's answer outright (`detection_method` becomes `pixel_*`); a disagreement is logged but doesn't by itself force review (flagging every override would train reviewers to ignore the flag) — except on not-yet-validated calibrations, where it does. If pixel positively found nothing marked but the model reported an answer, the answer is force-blanked and flagged.
   - **`MODEL_OVERRULES_PIXEL_QUESTION_NUMBERS`** (`19,20,21,22,23,25,27,28,29,30,31,32,33,34,35` — every pixel-checked question except the written-text/Vision-checked 24/26): for these questions, a pixel/model disagreement never auto-corrects or auto-reconciles the model's answer — the model's reading is always kept, and the disagreement is only surfaced via `needs_review`. Started as just `{31,32,33,34}` after a confirmed false-blank on Q34 (row-banding text-anchor logic landed one row early), then expanded to the full range after a real accuracy test on one file found four confirmed pixel-detector errors in this range (Q20, Q23, Q25, Q29 — one of which shipped with no review flag at all) against zero cases of pixel correctly overriding the model.
3. **Vision OCR cross-check** — for the written-text fields only (which fields, and how strictly — token/freeform/none — is configured per-question in the shared config workbook's "Cloud Vision Check" rule, derived from `_VISION_TOKEN_FIELDS`/`_VISION_FREEFORM_FIELDS`): Cloud Vision OCRs the page (English-handwriting hint), text is located by anchoring to the field's printed label where possible, and compared to the model's answer — exact match (with digit-strip/char-overlap tolerance) for short "token" fields, word-level coverage (≥`VISION_FREEFORM_COVERAGE_THRESHOLD`) for "freeform" fields. Fields in `_VISION_AUTHORITATIVE_FIELDS` (also config-driven; H2/H5 by default) are Vision-authoritative — a non-empty anchored Vision reading overwrites the model's answer for those fields specifically.

**Every condition that sets `needs_review=True`:** pixel/model disagreement on unvalidated calibration; a `MODEL_OVERRULES_PIXEL_QUESTION_NUMBERS` disagreement (any direction: pixel vs. model position, pixel-blank vs. model-answer, or multi-select VETO/FILL); a pixel-detected correction/cross-out; pixel found blank but model didn't; model self-inconsistency (model-only rows); ambiguous multi-select or yes/no mark not corroborated by the model; the whole 18-question grid failing to locate at all on a file (all 18 flagged); two boxes reading marked in one row/question *unless* pixel's own top pick already agrees with the model's answer (`_pixel_agrees_with_model()` — a second, unrelated dark mark, e.g. a crossed-out earlier answer, isn't a real disagreement about the final answer); model confidence below `pipeline_config.MODEL_CONFIDENCE_THRESHOLD` (model-only rows) *unless* both the model and an independently-run pixel/Vision reading agree the field is genuinely blank; a Vision/model disagreement on a write-in field, including Q24's comment box (generalized beyond H2/H6: Vision now also directly fills in Q24's answer, always flagged, when the model reported it blank but Vision's OCR of the comment box found real text); H1/H6 failing their format check; and a generic "confirm this is a genuine skip" flag — now annotated with each source's own separate reading via `_describe_separate_readings()` (e.g. "model said 'X'; pixel said (blank); vision said (blank)") — on any final blank answer outside H4/H5 and outside the two-independent-sources-agree-blank exemption above.

**Two-independent-sources-agree-blank exemption.** Both the model-confidence gate (4a) and the generic blank-answer backstop now skip flagging when a question's blank answer is independently corroborated: `both_confirm_written_blank` (every written-text field H1/H2/H4/H5/H6/24/26 — model says blank AND Vision's own OCR of that same anchored field also resolves to empty) or `both_agree_blank` (any other question where the pixel detector ran and also found nothing marked). A blank the pixel/Vision side actually *overrode from a non-blank model answer* is a real disagreement, already flagged elsewhere, and is never covered by this exemption — only requires the model's own raw answer to be blank too.

**Ambiguous-yes/no backstop widened.** Previously an ambiguous yes/no mark (ink bleeding across a box border, most often) only avoided `needs_review` when the pixel position it *did* resolve happened to match the model's answer; now it's exempted whenever the model gave any non-empty answer at all, since a resolved pixel position was never required for the model's own self-consistency check to already vouch for its reading.

**Manual corrections log.** `--log-corrections-file`, `--list-corrections`, `--question`/`--file`/`--root-cause`/`--corrections-limit` operate on a separate `corrections_log` BigQuery table — a human-maintained audit trail of confirmed extraction errors (root cause, fix status, whether it's a recurrence), used by developers to spot recurring failure patterns. It does not automatically modify `survey_responses`.

**Retries.** Up to 3 attempts per file with exponential backoff (10s/20s/40s) to ride out Vertex AI rate limits; any bonus signal (pixel detector, Vision OCR) failing is treated as non-fatal and simply skipped for that file, never aborting extraction.

### Step 5 — `step5_file_quality.py`

**Aggregation.** Groups `survey_responses` rows by `(folder_name, file_name)`; for each file, filters to `needs_review=TRUE` rows and builds a category per flagged question via fixed string-matching against `review_note` phrasing (e.g. "self-reported confidence" → *low model confidence*; "cloud vision" → *independent OCR cross-check disagreement*; "reconciled to"/"claimed"+"missed" → *multi-select pixel reconciliation*). Every file gets a row, including fully clean files — there's no severity/priority tiering, just counts and a concatenated issue list.

**Replace semantics.** Not a MERGE or partition overwrite — for each folder present in the current run's data, existing `file_quality_review` rows for that folder are deleted via a DML `DELETE ... WHERE folder_name = @folder`, then new rows are appended. `--no-replace-folder` skips the delete (append-only, duplicates rows on rerun).

**Output:** `file_quality_review`, partitioned by `refreshed_date` (ETL run time, stamped once per run — distinct from `report_date`, which reflects when the survey was filled out). Columns: `folder_name, file_name, report_date, refreshed_at, refreshed_date, needs_review, flagged_question_count, total_question_count, issue_summary, issues[], unreadable_areas[]`.

**dry_run=True** stops after building/logging the in-memory summary — no table creation, deletes, or writes happen.

## Repo file reference

| File | Description |
|---|---|
| `step1_merge_pdf.py` … `step6_load_feedback.py` | Pipeline stages, described above |
| `pipeline_config.py` | Shared settings + survey schema, read by every step; downloads live overrides from a GCS-hosted Excel workbook at import time |
| `generate_pipeline_config_doc.py` | Generates/regenerates that Excel workbook, pre-filled from the live workbook's current values (or `pipeline_config.py`'s built-in defaults) |
| `generated_calibration.json` | Sample per-file geometry calibration output (step 2) |

## Commands

### End-to-end: extract a file, review its feedback, reload it
The human-review feedback loop lives across two scripts: `step4_process_pdf.py
--export-needs-review-feedback` **generates** the Excel review file, and
`step6_load_feedback.py` **ingests** it back in. Both read/write `survey_responses_
with_feedback` (a copy of `survey_responses` plus `correct_answer`/`updated_with_
feedback`/`feedback_updated_time`/`ingested_from` - see `sync_survey_responses_with_feedback()` in
`step4_process_pdf.py`), never `survey_responses` itself.

1. **Extract one file in step 4** (or run a normal folder/full extraction instead -
   this is just the single-file form):
   ```
  python3 step4_process_pdf.py \
    --bucket tps_survey \
    --root-prefix "TPS_Scanned_2025_Reorgnized/" \
    --only-file "Nov 23 2025/Nov23_5/2025_Nov_23_5_TPS_3996.pdf" \
    --bq-project gcp-sapchoda-dev \
    --bq-dataset ladph_tps \
    --bq-table survey_responses \
    --vertex-location global \
    2>&1 | grep -Ei "ERROR|INFO.*QUALITY|INFO.*FILE|Loaded|survey_responses|Traceback" 

   ```

2. **Generate the feedback Excel in step 4** - syncs `survey_responses_with_feedback`
   from `survey_responses`, then exports every `needs_review=TRUE` row not yet covered
   by ingested feedback to a new `gs://tps_survey/TPS_Feedback/tps_feedback_{datetime}.xlsx`:
   ```
   python3 step4_process_pdf.py --export-needs-review-feedback
   ```

3. **Review it** - download that file, fill in `correct_answer` for any row that's
   wrong (leave it blank for a row that's already correct), and upload it back to the
   *same* GCS path (overwrite in place).

4. **Ingest the feedback with step 6** - finds the LATEST `tps_feedback_*.xlsx` file,
   syncs again, then ingests it: a row whose `correct_answer` was filled in gets
   `survey_answer` overwritten; a row left blank keeps its `survey_answer` unchanged.
   Every matched row not already ingested gets `updated_with_feedback=TRUE` +
   `feedback_updated_time=now` + `ingested_from=<this file's gs:// URI>` - already-
   ingested rows are cross-checked against `survey_responses_with_feedback` and
   skipped, so this is safe to run repeatedly:
   ```
   python3 step6_load_feedback.py
   ```