"""Central configuration for the LADPH survey-processing
pipeline - step1_merge_pdf.py through step6_load_feedback.py.

WHY THIS FILE EXISTS: every step of this pipeline used to hardcode its own
copy of the GCS bucket/prefixes, GCP project, BigQuery dataset/table names,
model names, and (in step4) the entire survey question schema. Porting the
pipeline to a DIFFERENT survey PDF (different bucket, different questions,
different answer choices) meant hunting through six separate files for
every place one of those values was duplicated. This file is the single
source of truth for all of that - every step imports from here instead of
defining its own copy.

Form-specific choices that can be reviewed safely (form page count,
question validation metadata, detection mode, and approved control
coordinates) are loaded from the workbook's Form Setup, Survey Questions,
and Control Calibration tabs. Calibration Reference stores detected control
geometry for Step 2 comparisons only. A new form starts in DRAFT and cannot
run until its extracted schema and setup have been approved.

WHERE THE VALUES BELOW ACTUALLY COME FROM: every constant below is a
FALLBACK default for operational settings; the GCS-hosted workbook (see
_CONFIG_GCS_BUCKET/_CONFIG_GCS_BLOB just below) is required for the survey
schema. On a normal
run, this module downloads and parses that workbook at import time and
OVERRIDES these defaults with whatever it finds - see _load_overrides()
near the bottom of this file. This means the real, current values a given
run is using can differ from what's printed here; read the workbook (or
this module's own `import pipeline_config; print(pipeline_config.GCS_BUCKET)`
etc. after import) to see what actually applied. Regenerate the workbook's
starting point with generate_pipeline_config_doc.py.

To port this pipeline to a different survey PDF, generate a DRAFT workbook
from its blank template with generate_pipeline_config_doc.py
--setup-from-pdf, review the questions and geometry, then approve its
profile. Older workbooks without the form-profile tabs remain supported.
The workbook is required at runtime; missing configuration is a fatal error.
"""

import io

# ==========================================================================
# BOOTSTRAP: where the configuration workbook itself lives. This is the one
# thing that genuinely can't come from the workbook (we have to know where
# to look before we can read it) - it's the only GCS location in this file
# that ISN'T a fallback default, and changing it requires an actual code
# edit + redeploy, unlike everything else below.
# ==========================================================================
_CONFIG_GCS_BUCKET = "tps_survey"
_CONFIG_GCS_BLOB = "Pipeline_Config/pipeline_configuration_2027.xlsx"

# ==========================================================================
# GCS locations
# ==========================================================================
GCS_BUCKET = "tps_survey"
# step1's input: raw, multi-survey combined scans as they land from scanning.
GCS_RAW_PREFIX = "TPS_Scanned_2025/"
# step1's output, and step2/step3/step4's input: one configured-page-count
# PDF per survey, stored beneath a folder named after its source PDF.
GCS_SPLIT_PREFIX = "TPS_Scanned_2025_Reorgnized/"

# step6's input: the human-reviewed feedback/corrections spreadsheet.
GCS_FEEDBACK_BUCKET = "tps_survey"
# step4's output / step6's input: the folder step4's --export-needs-review-
# feedback writes one "tps_feedback_{datetime}.xlsx" file into per run, and
# step6 reads the LATEST (by name, since the datetime suffix sorts
# lexicographically) file from - see export_needs_review_feedback() in
# step4_process_pdf.py and step6_load_feedback.py's find_latest_feedback_blob().
GCS_FEEDBACK_PREFIX = "TPS_Feedback/"
FEEDBACK_FILE_PREFIX = "tps_feedback_"

# ==========================================================================
# Form profile — reviewed form-specific setup generated from a blank template
# ==========================================================================
FORM_SETUP_STATUS = "APPROVED"
FORM_PROFILE_CONFIGURED = False
FORM_NAME = "Treatment Perceptions Survey"
PAGES_PER_SURVEY = 2
FORM_DETECTION_MODE = "hybrid"
SURVEY_LANGUAGE = "English"
DECLINED_KEYWORDS = ["declined"]
FORM_BASELINE_MARKS = {}
FORM_INK_THRESHOLD = 165
FORM_GRID_COLUMN_CENTERS = []
FORM_GRID_BOX_EXPECTED_SIZE = 40
FORM_CONTROL_CALIBRATION = []
FORM_CALIBRATION_REFERENCE = []

# ==========================================================================
# GCP project / BigQuery dataset+tables
# ==========================================================================
GCP_PROJECT_ID = "gcp-sapchoda-dev"
BQ_DATASET = "ladph_tps"

BQ_TABLE_MANIFEST = "pdf_manifest_list"                             # step1: source PDF -> split-survey manifest
BQ_TABLE_CALIBRATION = "pdf_calibration_profile"                    # step2: per-file registration/ink profiling
BQ_TABLE_QUALITY = "pdf_quality"                                    # step3: per-file quality/routing classification
BQ_TABLE_SURVEY_RESPONSES = "survey_responses"                      # step4: extracted Q&A, one row per question per survey
BQ_TABLE_CORRECTIONS = "corrections_log"                            # step4: logged human corrections
BQ_TABLE_PIPELINE_CONFIG = "pipeline_config"                        # step4: runtime calibration overrides (BQ-stored, unrelated to this file)
BQ_TABLE_FILE_QUALITY = "file_quality_review"                       # step5: per-file quality review rollup
BQ_TABLE_SURVEY_RESPONSES_WITH_FEEDBACK = "survey_responses_with_feedback"  # step6: survey_responses + feedback-corrected answers

# ==========================================================================
# Vertex AI / Cloud Vision
# ==========================================================================
VERTEX_LOCATION = "global"
# step1's form-content and language checks use one configurable model.
STEP1_CLASSIFICATION_MODEL = "gemini-3.8-flash"
# Backward compatibility for existing workbooks and callers.
TPS_EXTRACTION_MODEL = STEP1_CLASSIFICATION_MODEL
STEP1_CHECK_MAX_ATTEMPTS = 3
STEP1_CHECK_RETRY_DELAY_SECONDS = 2
# step4: the main survey question/answer extraction model.
GEMINI_MODEL = "gemini-3.8-flash"
# step3: the Cloud Vision judgment call on handwriting_readable/tears_or_
# damage for non-clear files (see classify_gcs()/classify_pdf_bytes() in
# step3_pdf_quality_check.py).
STEP3_VISION_MODEL = "gemini-3.1-flash-lite"
# None -> Cloud Vision uses the application-default GCP project, same
# convention as step4's own VERTEX_PROJECT_ID/BQ_PROJECT_ID.
VISION_PROJECT_ID = None

# ==========================================================================
# Report year
# ==========================================================================
# Legacy TPS setting used for H6 date guidance/validation when loading a
# workbook without a reviewed form profile. New profiles use per-question
# Expected Year metadata instead.
REPORT_YEAR = "2025"

# ==========================================================================
# step4 thresholds / feature flags
# ==========================================================================
# Model self-reported confidence (0-1) below which a model-only answer (no
# pixel/Vision corroboration) is flagged needs_review.
MODEL_CONFIDENCE_THRESHOLD = 0.7
# General coverage/overlap threshold (0-1) for every written-text field's
# near-miss tolerance check against Cloud Vision's OCR (word-level for H4/
# H5/24, character-level for H2, digit-level for H1/26/H6) - below this,
# the field disagrees with Vision's reading and is flagged. Previously H2/
# H4/H6 each had their own separately-tuned threshold (0.8/0.5/0.5); explicit
# user request consolidated all of them into this one general value.
VISION_FREEFORM_COVERAGE_THRESHOLD = 0.7
# Set False to skip the Cloud Vision double-check entirely (e.g. no Vision
# API enabled/quota) - written-text questions fall back to model-only +
# confidence-threshold checking alone.
VISION_DOUBLE_CHECK_ENABLED = True
# Set False to skip the upstream pdf_quality routing lookup and process
# every file identically.
PDF_QUALITY_ROUTING_ENABLED = True
# Set False to always run with this file's built-in calibration defaults,
# ignoring BQ_TABLE_PIPELINE_CONFIG's runtime overrides entirely.
PIPELINE_CONFIG_ENABLED = True
# Number of source PDFs processed concurrently in a step4 run.
FILE_EXTRACTION_WORKERS = 4

# ==========================================================================
# Survey schema: questions, answer choices, and question-TYPE rules
# ==========================================================================
# To port this pipeline to a different survey PDF, edit the configuration
# WORKBOOK on GCS, not this file. SURVEY_QUESTIONS (loaded from that
# workbook at import) is the single source of truth - step4 derives
# QUESTION_TEXT_BY_NUMBER, CHOICE_LISTS_BY_NUMBER, and WRITTEN_TEXT_QUESTION_
# NUMBERS straight from it, so those never need separate maintenance. The
# sets below it (MULTI_SELECT_QUESTION_NUMBERS onward) come from that same
# sheet's per-question columns - they encode real, case-by-case decisions
# about how each question's answer should be verified, made and documented
# over many real scans (see step4_process_pdf.py's own inline history).

# No grouped "big question -> lettered sub-items" structure in this
# template (e.g. no "Q5a / Q5b / Q5c" under one shared stem) - a form that
# has one would populate this as {group_key: "shared stem text"}.
GROUP_HEADERS = {}

# ==========================================================================
# How a question's answer choices are written in one cell/string, and the
# ONE parser (split_choices()/is_choice_list() below) every step must use to
# read them back - never a hand-rolled .split() at the call site.
#
# CHOICE_SEPARATOR is "|" because a choice's OWN text very often contains a
# slash - "Female-to-Male (FTM)/Transgender Male/Trans Man" is ONE choice on
# Q29, "Unsure/Questioning/Don't know" is ONE choice on Q31 - so a slash
# cannot safely separate choices from each other. The original convention
# worked around that by requiring a SPACE-slash-SPACE (" / ") separator and
# splitting on exactly that, which does parse correctly, but silently
# depends on invisible whitespace: someone editing the workbook who types
# "Yes/No" or "Yes/ No" instead of "Yes / No" gets a SINGLE run-together
# choice with no error, and the resulting off-by-N choice list shifts every
# mark_position after it - exactly the class of silent mislabeling the Q31
# choice-ORDER bug caused. A pipe can't collide with a choice's own text, so
# it needs no whitespace ceremony to be unambiguous.
#
# Legacy " / " input is still parsed (see split_choices()) so the live
# workbook written under the old convention keeps working untouched.
# ==========================================================================
CHOICE_SEPARATOR = "|"
_LEGACY_CHOICE_SEPARATOR = " / "


def is_choice_list(choices: str) -> bool:
    """True when `choices` is a fixed-choice question's list of options,
    False when it's an open write-in field's plain descriptive string (e.g.
    "written agency name, if filled in"). This is what tells the two
    question TYPES apart everywhere downstream - see step4_process_pdf.py's
    CHOICE_LISTS_BY_NUMBER/WRITTEN_TEXT_QUESTION_NUMBERS."""
    choices = choices or ""
    return CHOICE_SEPARATOR in choices or _LEGACY_CHOICE_SEPARATOR in choices


def split_choices(choices: str) -> list:
    """Parses a choices string into its ordered list of individual choices.

    Prefers CHOICE_SEPARATOR ("|"), and deliberately splits on it with or
    without surrounding spaces ("A|B", "A | B" and "A |B" all parse the
    same) - safe precisely because a pipe never appears inside a choice's
    own text, so there's no whitespace convention left for a human editing
    the workbook to get subtly wrong.

    Falls back to the legacy " / " separator (space-slash-space, spaces
    REQUIRED - unlike the pipe, a bare "/" genuinely does appear inside many
    choices and must never be split on) only when no pipe is present, so a
    workbook still written the old way keeps parsing exactly as before.

    Returns [] for an open write-in field's descriptive string."""
    choices = choices or ""
    if CHOICE_SEPARATOR in choices:
        parts = choices.split(CHOICE_SEPARATOR)
    elif _LEGACY_CHOICE_SEPARATOR in choices:
        parts = choices.split(_LEGACY_CHOICE_SEPARATOR)
    else:
        return []
    return [part.strip() for part in parts if part.strip()]


def join_choices(choices: list) -> str:
    """The inverse of split_choices() - writes a choice list back out in the
    canonical CHOICE_SEPARATOR form, spaced (" | ") for readability in a
    spreadsheet cell."""
    return f" {CHOICE_SEPARATOR} ".join(c.strip() for c in choices if c.strip())


# The survey's questions, answer choices and per-question handling rules
# live ENTIRELY in the configuration workbook on GCS (the "Survey Questions"
# sheet of _CONFIG_GCS_BLOB, generated/edited via
# generate_pipeline_config_doc.py). There is deliberately NO built-in copy
# of any question text or answer choice in this file: a second copy here
# would silently diverge from the workbook, and because _load_overrides()
# REPLACES this list wholesale rather than merging into it, an edit made
# here would appear to work locally and then be discarded at import time.
# The workbook is the single source of truth.
#
# Everything below is populated by _apply_survey_questions() at import.
# They start empty, so if the workbook cannot be read the pipeline fails
# loudly (see _load_overrides()) instead of extracting against a stale or
# wrong question list.
SURVEY_QUESTIONS = []

# Questions where more than one choice can legitimately be marked at once.
MULTI_SELECT_QUESTION_NUMBERS = set()

# Choice questions where, on disagreement between the model's reading and
# the deterministic pixel detector, the MODEL's answer is kept and the
# disagreement is only surfaced via needs_review, rather than the pixel
# reading being auto-applied. (Workbook column: "If the AI's answer and the
# automatic checkbox-detector disagree, trust the AI's answer instead...")
MODEL_OVERRULES_PIXEL_QUESTION_NUMBERS = set()

# Write-in fields compared against Cloud Vision's word-level OCR tokens for
# an exact match (short, realistically one token) vs. its fuzzy freeform
# coverage check (can span multiple words/lines). See
# cross_check_written_field_with_vision() in step4.
_VISION_TOKEN_FIELDS = set()
_VISION_FREEFORM_FIELDS = set()

# Write-in fields legitimately left blank often enough to be exempt from the
# generic "blank final answer always needs review" backstop (any OTHER check
# can still flag them for its own reason).
_BLANK_ANSWER_EXEMPT_FIELDS = set()

# Write-in fields where, on disagreement, Cloud Vision's anchored OCR
# reading is treated as MORE trustworthy than the model's and used as
# survey_answer instead (still always flagged needs_review).
_VISION_AUTHORITATIVE_FIELDS = set()

# Which page (0-indexed) of a survey's 2-page PDF each write-in question
# sits on - used to fetch the right page's Vision OCR result.
_WRITTEN_TEXT_QUESTION_PAGE = {}
QUESTION_METADATA_BY_NUMBER = {}


# ==========================================================================
# Load overrides from the GCS-hosted configuration workbook (generated by
# generate_pipeline_config_doc.py). This section fills in the survey schema
# names declared empty above, and overrides the tunable settings.
#
# The SETTINGS above (paths, model names, thresholds) are real fallback
# defaults. The SURVEY SCHEMA is not: there is no built-in question list any
# more, so a missing/unreachable workbook, a missing sheet or a missing
# dependency (google-cloud-storage/pandas/openpyxl) is FATAL rather than a
# warning - running on an empty question list would quietly write a whole
# batch of empty answers to BigQuery.
# ==========================================================================
_YES_VALUES = {"y", "yes", "true", "1"}


def _is_yes(value) -> bool:
    return str(value).strip().lower() in _YES_VALUES


def _clean(value) -> str:
    """Excel/pandas gives back NaN (a float) for a blank cell - normalize
    every cell to a plain, stripped string (or "" for blank/NaN) before use,
    so downstream code never has to special-case pandas' float NaN."""
    import pandas as pd  # local import - see the try/except this is only ever called within
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def _apply_settings(settings: dict) -> None:
    """settings: {Parameter -> raw cell value}, already _clean()'d. Applies
    each recognized parameter to this module's globals with the right type
    coercion; an unrecognized Parameter row is ignored (forward-compatible
    with a workbook that has extra notes/rows), not an error."""
    g = globals()
    string_params = (
        "GCS_BUCKET", "GCS_RAW_PREFIX", "GCS_SPLIT_PREFIX", "GCS_FEEDBACK_BUCKET",
        "GCS_FEEDBACK_PREFIX", "GCP_PROJECT_ID", "BQ_DATASET", "BQ_TABLE_MANIFEST",
        "BQ_TABLE_CALIBRATION", "BQ_TABLE_QUALITY", "BQ_TABLE_SURVEY_RESPONSES",
        "BQ_TABLE_CORRECTIONS", "BQ_TABLE_PIPELINE_CONFIG", "BQ_TABLE_FILE_QUALITY",
        "BQ_TABLE_SURVEY_RESPONSES_WITH_FEEDBACK", "VERTEX_LOCATION",
        "STEP1_CLASSIFICATION_MODEL", "TPS_EXTRACTION_MODEL",
        "GEMINI_MODEL", "STEP3_VISION_MODEL", "REPORT_YEAR",
        "FEEDBACK_FILE_PREFIX",
    )
    float_params = ("MODEL_CONFIDENCE_THRESHOLD", "VISION_FREEFORM_COVERAGE_THRESHOLD")
    bool_params = ("VISION_DOUBLE_CHECK_ENABLED", "PDF_QUALITY_ROUTING_ENABLED", "PIPELINE_CONFIG_ENABLED")

    for name, raw in settings.items():
        if name not in g:
            continue  # unrecognized Parameter row - a note/typo, not this module's problem
        if raw == "":
            continue  # blank Answer cell - keep the built-in default rather than overwrite with empty
        if name in string_params:
            g[name] = raw
        elif name in float_params:
            g[name] = float(raw)
        elif name == "FILE_EXTRACTION_WORKERS":
            g[name] = int(float(raw))
        elif name == "STEP1_CHECK_MAX_ATTEMPTS":
            attempts = int(float(raw))
            if attempts < 1:
                raise ValueError("STEP1_CHECK_MAX_ATTEMPTS must be at least 1")
            g[name] = attempts
        elif name == "STEP1_CHECK_RETRY_DELAY_SECONDS":
            delay = float(raw)
            if delay < 0:
                raise ValueError("STEP1_CHECK_RETRY_DELAY_SECONDS cannot be negative")
            g[name] = delay
        elif name in bool_params:
            g[name] = _is_yes(raw)
        elif name == "VISION_PROJECT_ID":
            g[name] = raw or None
        elif name == "PAGES_PER_SURVEY":
            pages = int(float(raw))
            if pages < 1:
                raise ValueError("PAGES_PER_SURVEY must be at least 1")
            g[name] = pages
        elif name in ("FORM_SETUP_STATUS", "FORM_NAME", "FORM_DETECTION_MODE", "SURVEY_LANGUAGE"):
            g[name] = raw
        elif name == "DECLINED_KEYWORDS":
            g[name] = split_choices(raw)
        # else: a recognized-but-unhandled name (shouldn't happen given the
        # lists above cover every settings-sheet row) - leave the default.

    legacy_model = settings.get("TPS_EXTRACTION_MODEL", "")
    configured_model = settings.get("STEP1_CLASSIFICATION_MODEL", "")
    if configured_model:
        g["STEP1_CLASSIFICATION_MODEL"] = configured_model
    elif legacy_model:
        g["STEP1_CLASSIFICATION_MODEL"] = legacy_model
    g["TPS_EXTRACTION_MODEL"] = g["STEP1_CLASSIFICATION_MODEL"]


def _first_present(row: dict, *keys: str) -> str:
    """Returns the value of the first of `keys` that's an actual column in
    this row, or "" if none are. Lets _apply_survey_questions() read either
    the CURRENT sheet's column headers or an OLDER workbook's headers for
    the same underlying field - a workbook generated before a header was
    reworded (or a column was consolidated into "Question Type") still
    loads correctly instead of silently losing that field to a missed
    dict lookup."""
    for key in keys:
        if key in row:
            return row[key]
    return ""


# Maps the "Question Type" column's five dropdown values (see
# generate_pipeline_config_doc.py's QUESTION_TYPES) to the underlying
# (is_multi_select, vision_check) rule pair every other part of step4 keys
# off. "Question Type" REPLACES the older, more technical "Multi-Select?
# (Y/N)" + "Cloud Vision Check (token / freeform / none)" pair of columns
# with one plain-language dropdown a non-technical editor can't mistype -
# _apply_survey_questions() below still reads the old pair as a fallback
# for a workbook that hasn't been regenerated with the new column yet.
_QUESTION_TYPE_RULES = {
    "single choice (pick one)": (False, "none"),
    "multiple choice (pick several)": (True, "none"),
    "written answer - short code or number": (False, "token"),
    "written answer - longer text": (False, "freeform"),
    "written answer - not checked by cloud vision": (False, "none"),
}


def _apply_survey_questions(rows: list) -> None:
    """rows: list of dicts, one per Survey Questions sheet row, already
    _clean()'d. Rebuilds SURVEY_QUESTIONS and every question-type rule set
    from scratch - this sheet is the full replacement, not a per-field
    override, since a ported survey's question list is usually entirely
    different from the built-in default's."""
    g = globals()
    survey_questions = []
    multi_select = set()
    model_overrules_pixel = set()
    blank_exempt = set()
    vision_token = set()
    vision_freeform = set()
    vision_authoritative = set()
    written_text_page = {}
    question_metadata = {}
    legacy_page_column = (
        "Which page of the 2-page form is this on? (0 = first page, 1 = second page; leave blank for a Single/Multiple Choice question)",
        "Page (0 or 1)",
    )
    legacy_page_values = [
        int(float(value))
        for row in rows
        for value in [_first_present(row, *legacy_page_column)]
        if value != ""
    ]
    legacy_pages_one_based = (
        PAGES_PER_SURVEY > 1
        and PAGES_PER_SURVEY in legacy_page_values
        and 0 not in legacy_page_values
    )

    for row in rows:
        number = row.get("Number", "")
        if not number:
            continue  # blank template/example row - skip rather than error
        group_key = row.get("Group Key") or None
        question_text = row.get("Question Text", "")
        choices_column = next(
            (column for column in row if column.startswith("Answer Choices")),
            None,
        )
        choices = (
            row.get(choices_column, "")
            if choices_column is not None
            else _first_present(
                row,
                "Answer Choices (only for Single/Multiple Choice - separate with ' / ')",
                "Answer Choices (separate with ' / ', leave blank for open write-in)",
            )
        )
        survey_questions.append((number, group_key, question_text, choices))

        question_type = _first_present(row, "Question Type").strip().lower()
        if question_type in _QUESTION_TYPE_RULES:
            is_multi, vision_check = _QUESTION_TYPE_RULES[question_type]
            if is_multi:
                multi_select.add(number)
        else:
            # Older workbook with no "Question Type" column yet (or a typo
            # that didn't match any of the five dropdown values) - fall
            # back to reading the two columns it replaced.
            if _is_yes(row.get("Multi-Select? (Y/N)")):
                multi_select.add(number)
            vision_check = row.get("Cloud Vision Check (token / freeform / none)", "").lower() or "none"

        if vision_check == "token":
            vision_token.add(number)
        elif vision_check == "freeform":
            vision_freeform.add(number)

        if _is_yes(_first_present(
            row,
            "If the AI's answer and the automatic checkbox-detector disagree, trust the AI's answer instead of the detector? (Y/N)",
            "Model Overrules Pixel on Disagreement? (Y/N)",
        )):
            model_overrules_pixel.add(number)
        if _is_yes(_first_present(
            row,
            "OK to leave this question blank without flagging it for review? (Y/N)",
            "Exempt From Blank-Answer Review? (Y/N)",
        )):
            blank_exempt.add(number)
        if _is_yes(_first_present(
            row,
            "If Cloud Vision's OCR and the AI disagree on this written answer, trust Cloud Vision instead of the AI? (Y/N)",
            "Cloud Vision Is Authoritative on Disagreement? (Y/N)",
        )):
            vision_authoritative.add(number)

        page_1based = _first_present(row, "Question Page (1-based)")
        if page_1based:
            page_index = int(float(page_1based)) - 1
        else:
            legacy_page = _first_present(row, *legacy_page_column)
            if legacy_page == "":
                page_index = None
            else:
                page_value = int(float(legacy_page))
                page_index = page_value - 1 if legacy_pages_one_based else page_value
        if page_index is not None:
            if page_index < 0 or page_index >= PAGES_PER_SURVEY:
                raise ValueError(
                    f"Question {number!r} page {page_index + 1} is outside "
                    f"the configured {PAGES_PER_SURVEY}-page form"
                )
            question_metadata.setdefault(number, {})["page"] = page_index
            if vision_check != "none":
                written_text_page[number] = page_index

        metadata_fields = {
            "Expected Answer Format": "expected_format",
            "Expected Digit Count": "expected_digits",
            "Expected Date Format": "date_format",
            "Expected Year": "expected_year",
            "Minimum Value": "minimum",
            "Maximum Value": "maximum",
            "Vision Anchor Text": "vision_anchor",
            "Vision Anchor Direction": "vision_direction",
            "Vision Anchor Reach (pixels)": "vision_reach",
            "Vision Stop Phrase": "vision_stop_phrase",
        }
        metadata = question_metadata.setdefault(number, {})
        for column, key in metadata_fields.items():
            value = row.get(column, "")
            if value == "":
                continue
            if key in ("expected_digits", "expected_year", "vision_reach"):
                metadata[key] = int(float(value))
            elif key in ("minimum", "maximum"):
                metadata[key] = float(value)
            else:
                metadata[key] = value
        expected_format = str(metadata.get("expected_format", "")).lower()
        if expected_format and expected_format not in {"digits", "integer", "decimal", "date"}:
            raise ValueError(
                f"Question {number!r} has unsupported Expected Answer Format "
                f"{expected_format!r}; use digits, integer, decimal, or date."
            )
        if metadata.get("expected_digits") is not None and metadata["expected_digits"] < 1:
            raise ValueError(f"Question {number!r} Expected Digit Count must be positive")
        if (
            metadata.get("minimum") is not None
            and metadata.get("maximum") is not None
            and metadata["minimum"] > metadata["maximum"]
        ):
            raise ValueError(f"Question {number!r} Minimum Value exceeds Maximum Value")
        direction = str(metadata.get("vision_direction", "right")).lower()
        if metadata.get("vision_anchor") and direction not in {"right", "below"}:
            raise ValueError(
                f"Question {number!r} Vision Anchor Direction must be right or below"
            )
        if metadata.get("vision_anchor"):
            metadata["vision_direction"] = direction

    if not survey_questions:
        raise ValueError(
            "the workbook's 'Survey Questions' sheet has no rows with a "
            "Number - there is no built-in question list to fall back on"
        )

    g["SURVEY_QUESTIONS"] = survey_questions
    g["MULTI_SELECT_QUESTION_NUMBERS"] = multi_select
    g["MODEL_OVERRULES_PIXEL_QUESTION_NUMBERS"] = model_overrules_pixel
    g["_BLANK_ANSWER_EXEMPT_FIELDS"] = blank_exempt
    g["_VISION_TOKEN_FIELDS"] = vision_token
    g["_VISION_FREEFORM_FIELDS"] = vision_freeform
    g["_VISION_AUTHORITATIVE_FIELDS"] = vision_authoritative
    g["_WRITTEN_TEXT_QUESTION_PAGE"] = written_text_page
    g["QUESTION_METADATA_BY_NUMBER"] = question_metadata


def _apply_form_setup(rows: list) -> None:
    """Apply generated form profile values; draft profiles are never runnable."""
    globals()["FORM_PROFILE_CONFIGURED"] = True
    values = {
        _clean(row.get("Parameter", "")): _clean(row.get("Value", ""))
        for _, row in rows.iterrows()
        if _clean(row.get("Parameter", "")) != ""
    }
    if values.get("FORM_SETUP_STATUS", FORM_SETUP_STATUS).upper() != "APPROVED":
        raise ValueError(
            "The form setup is still DRAFT. Review the generated question and "
            "control mappings in the workbook, then set FORM_SETUP_STATUS to APPROVED."
        )
    if values.get("FORM_NAME"):
        globals()["FORM_NAME"] = values["FORM_NAME"]
    if values.get("PAGES_PER_SURVEY"):
        pages = int(float(values["PAGES_PER_SURVEY"]))
        if pages < 1:
            raise ValueError("PAGES_PER_SURVEY must be at least 1")
        globals()["PAGES_PER_SURVEY"] = pages
    if values.get("FORM_DETECTION_MODE"):
        mode = values["FORM_DETECTION_MODE"].lower()
        if mode not in {"hybrid", "model_only"}:
            raise ValueError("FORM_DETECTION_MODE must be hybrid or model_only")
        globals()["FORM_DETECTION_MODE"] = mode
    if values.get("SURVEY_LANGUAGE"):
        globals()["SURVEY_LANGUAGE"] = values["SURVEY_LANGUAGE"]
    if values.get("DECLINED_KEYWORDS"):
        globals()["DECLINED_KEYWORDS"] = split_choices(values["DECLINED_KEYWORDS"])
    for key, value in (
        ("FORM_BASELINE_MARKS", "BASELINE_MARKS_JSON"),
        ("FORM_GRID_COLUMN_CENTERS", "GRID_COLUMN_CENTERS_JSON"),
    ):
        if values.get(value):
            import json
            parsed = json.loads(values[value])
            if key == "FORM_BASELINE_MARKS":
                parsed = {
                    int(page): tuple(tuple(float(coord) for coord in point) for point in points)
                    for page, points in parsed.items()
                }
            else:
                parsed = tuple(float(center) for center in parsed)
            globals()[key] = parsed
    if values.get("FORM_INK_THRESHOLD"):
        globals()["FORM_INK_THRESHOLD"] = int(float(values["FORM_INK_THRESHOLD"]))
    if values.get("FORM_GRID_BOX_EXPECTED_SIZE"):
        globals()["FORM_GRID_BOX_EXPECTED_SIZE"] = float(values["FORM_GRID_BOX_EXPECTED_SIZE"])


def _apply_control_calibration(rows: list) -> None:
    question_choices = {
        number: split_choices(choices) if is_choice_list(choices) else []
        for number, _group, _text, choices in SURVEY_QUESTIONS
    }
    calibration = []
    for _, row in rows.iterrows():
        if _clean(row.get("Approval Status", "")).upper() != "APPROVED":
            continue
        question = _clean(row.get("Question Number", ""))
        label = _clean(row.get("Choice Label", ""))
        control_type = _clean(row.get("Control Type", "")).lower()
        if not question or not label or control_type not in {"checkbox", "circle"}:
            raise ValueError(
                "Approved control calibration rows need a question number, "
                "choice label, and Control Type of checkbox or circle."
            )
        if question not in question_choices or label not in question_choices[question]:
            raise ValueError(
                f"Approved control calibration for {question!r}/{label!r} does not "
                "match a configured question and answer choice."
            )
        coords = tuple(
            float(row.get(column))
            for column in ("Left", "Right", "Top", "Bottom")
        )
        if not (coords[0] < coords[1] and coords[2] < coords[3]):
            raise ValueError(f"Invalid approved rectangle for question {question!r}, label {label!r}")
        page = int(float(row.get("Page (1-based)"))) - 1
        if page < 0 or page >= PAGES_PER_SURVEY:
            raise ValueError(
                f"Approved calibration for question {question!r} uses page "
                f"{page + 1}, outside the configured {PAGES_PER_SURVEY}-page form"
            )
        calibration.append({
            "question": question,
            "page": page,
            "control_type": control_type,
            "label": label,
            "rect": coords,
            "layout": _clean(row.get("Layout", "")) or "1",
        })
        question_meta = QUESTION_METADATA_BY_NUMBER.setdefault(question, {})
        prior_page = question_meta.setdefault("page", page)
        if prior_page != page:
            raise ValueError(
                f"Approved controls for question {question!r} span multiple pages; "
                "the question page must be reviewed explicitly."
            )
    globals()["FORM_CONTROL_CALIBRATION"] = calibration


def _apply_calibration_reference(rows: list) -> None:
    """Load generated per-control geometry used only as Step 2's reference."""
    question_choices = {
        number: split_choices(choices) if is_choice_list(choices) else []
        for number, _group, _text, choices in SURVEY_QUESTIONS
    }
    reference = []
    for _, row in rows.iterrows():
        if not any(_clean(value) for value in row.values):
            continue
        question = _clean(row.get("Question Number", ""))
        label = _clean(row.get("Choice Label", ""))
        control_type = _clean(row.get("Control Type", "")).lower()
        if not question or not label or control_type not in {"checkbox", "circle"}:
            raise ValueError(
                "Calibration Reference rows need a question number, choice "
                "label, and Control Type of checkbox or circle."
            )
        if question not in question_choices or label not in question_choices[question]:
            raise ValueError(
                f"Calibration Reference for {question!r}/{label!r} does not "
                "match a configured question and answer choice."
            )
        rect = tuple(float(row.get(column)) for column in ("Left", "Right", "Top", "Bottom"))
        if not (rect[0] < rect[1] and rect[2] < rect[3]):
            raise ValueError(
                f"Invalid reference rectangle for question {question!r}, label {label!r}"
            )
        page = int(float(row.get("Page (1-based)"))) - 1
        if page < 0 or page >= PAGES_PER_SURVEY:
            raise ValueError(
                f"Calibration Reference for question {question!r} uses page "
                f"{page + 1}, outside the configured {PAGES_PER_SURVEY}-page form"
            )
        reference.append({
            "question": question,
            "page": page,
            "control_type": control_type,
            "label": label,
            "rect": rect,
            "layout": _clean(row.get("Layout", "")) or "1",
            "source_pdf": _clean(row.get("Source PDF", "")),
            "confidence": float(row.get("Confidence") or 0),
        })
    globals()["FORM_CALIBRATION_REFERENCE"] = reference


def ink_cluster_limits() -> tuple[int, int, int]:
    """Return (minimum samples, minimum marks, maximum marks) for ink clustering.

    Legacy workbooks retain their validated TPS bounds. Configured profiles
    derive the sample and mark limits from approved checkbox geometry, so a
    smaller form is not rejected by TPS-specific box counts.
    """
    if not FORM_PROFILE_CONFIGURED:
        return 12, 8, 25

    checkbox_count = sum(
        item["control_type"] == "checkbox"
        for item in FORM_CONTROL_CALIBRATION
    )
    if checkbox_count < 2:
        return 2, 1, 1
    return min(12, checkbox_count), 1, checkbox_count - 1


def _validate_form_question_pages() -> None:
    if not FORM_PROFILE_CONFIGURED or FORM_SETUP_STATUS.upper() != "APPROVED":
        return
    missing_pages = [
        number for number, _group, _text, _choices in SURVEY_QUESTIONS
        if "page" not in QUESTION_METADATA_BY_NUMBER.get(number, {})
    ]
    if missing_pages:
        raise ValueError(
            "Approved form profiles need a Question Page (1-based) for every question; "
            f"missing: {', '.join(missing_pages)}"
        )


def _validate_calibration_review(rows: list) -> None:
    if not FORM_PROFILE_CONFIGURED or FORM_SETUP_STATUS.upper() != "APPROVED":
        return
    statuses = {
        _clean(row.get("Status", "")).upper()
        for _, row in rows.iterrows()
    }
    required_statuses = statuses - {"REFERENCE"}
    if not statuses or (required_statuses and required_statuses != {"APPROVED"}):
        raise ValueError(
            "An approved form profile requires every review item to be APPROVED; "
            "rows marked REFERENCE are informational geometry baselines and do not "
            "require approval."
        )


def _load_overrides() -> None:
    location = f"gs://{_CONFIG_GCS_BUCKET}/{_CONFIG_GCS_BLOB}"
    try:
        from google.cloud import storage
        import pandas as pd

        client = storage.Client()
        data = client.bucket(_CONFIG_GCS_BUCKET).blob(_CONFIG_GCS_BLOB).download_as_bytes()

        settings_df = pd.read_excel(io.BytesIO(data), sheet_name="Settings")
        settings = {
            _clean(row["Parameter"]): _clean(row["Answer"])
            for _, row in settings_df.iterrows()
            if _clean(row.get("Parameter", "")) != ""
        }
        _apply_settings(settings)

        workbook = pd.ExcelFile(io.BytesIO(data))
        if "Form Setup" in workbook.sheet_names:
            form_setup_df = pd.read_excel(io.BytesIO(data), sheet_name="Form Setup")
            _apply_form_setup(form_setup_df)

        questions_df = pd.read_excel(io.BytesIO(data), sheet_name="Survey Questions")
        rows = [
            {col: _clean(row[col]) for col in questions_df.columns}
            for _, row in questions_df.iterrows()
        ]
        _apply_survey_questions(rows)

        if "Control Calibration" in workbook.sheet_names:
            calibration_df = pd.read_excel(io.BytesIO(data), sheet_name="Control Calibration")
            if (
                FORM_PROFILE_CONFIGURED
                and FORM_SETUP_STATUS.upper() == "APPROVED"
                and FORM_DETECTION_MODE == "hybrid"
                and any(
                    _clean(row.get("Approval Status", "")).upper() != "APPROVED"
                    for _, row in calibration_df.iterrows()
                    if any(_clean(row.get(column, "")) for column in calibration_df.columns)
                )
            ):
                raise ValueError(
                    "Hybrid mode requires every populated Control Calibration row to be "
                    "marked APPROVED; pending mappings cannot activate detectors."
                )
            _apply_control_calibration(calibration_df)
            if FORM_DETECTION_MODE == "hybrid" and not FORM_CONTROL_CALIBRATION:
                raise ValueError(
                    "FORM_DETECTION_MODE is hybrid, but the workbook contains no "
                    "approved control calibration rows. Use model_only or approve "
                    "the mapped controls before enabling calibrated detectors."
                )
            if FORM_DETECTION_MODE == "hybrid" and any(
                item["control_type"] == "circle" and item["question"] != "H3"
                for item in FORM_CONTROL_CALIBRATION
            ):
                raise ValueError(
                    "Hybrid mode's circle detector currently supports H3 only. "
                    "Use model_only until a circle mapping has a compatible detector."
                )
        if "Calibration Reference" in workbook.sheet_names:
            reference_df = pd.read_excel(
                io.BytesIO(data),
                sheet_name="Calibration Reference",
                keep_default_na=False,
            )
            _apply_calibration_reference(reference_df)
        if "Calibration Review" in workbook.sheet_names:
            review_df = pd.read_excel(io.BytesIO(data), sheet_name="Calibration Review")
            _validate_calibration_review(review_df)
        elif FORM_PROFILE_CONFIGURED and FORM_SETUP_STATUS.upper() == "APPROVED":
            raise ValueError(
                "An approved Form Setup profile requires a Calibration Review sheet."
            )
        _validate_form_question_pages()

        print(f"[pipeline_config] Loaded settings + survey schema from {location}.")
    except Exception as e:  # noqa: BLE001 - re-raised below with context
        # The workbook is the ONLY source of the survey schema - there are no
        # built-in question defaults to fall back on any more, so a failure
        # here must be fatal. Silently continuing would run the pipeline with
        # an empty question list and write a whole batch of empty/garbage
        # answers to BigQuery.
        raise RuntimeError(
            f"could not load the pipeline configuration workbook {location} "
            f"({e}). The survey questions and answer choices live only in "
            "that workbook, so the pipeline cannot run without it. Check the "
            "workbook exists at that path, that this environment can reach "
            "GCS, and that google-cloud-storage/pandas/openpyxl are installed."
        ) from e


_load_overrides()
