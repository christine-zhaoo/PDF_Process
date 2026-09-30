"""Central configuration for the LADPH Treatment Perceptions Survey (TPS)
pipeline - step1_merge_pdf.py through step6_load_feedback.py.

WHY THIS FILE EXISTS: every step of this pipeline used to hardcode its own
copy of the GCS bucket/prefixes, GCP project, BigQuery dataset/table names,
model names, and (in step4) the entire survey question schema. Porting the
pipeline to a DIFFERENT survey PDF (different bucket, different questions,
different answer choices) meant hunting through six separate files for
every place one of those values was duplicated. This file is the single
source of truth for all of that - every step imports from here instead of
defining its own copy.

WHAT'S DELIBERATELY *NOT* HERE: pixel-level calibration (exact checkbox
x/y coordinates, anchor label text like "agency"/"address", confusable
handwritten-digit pairs, ink thresholds). Those live in step1-4's own code
because they're tied to this exact form's physical print layout - porting
to a genuinely different PDF format requires re-measuring that geometry
against the new form (see step2_pdf_calibration.py), not just editing a
config value. Everything in THIS file, by contrast, is either an
operational setting (where things live, which model/table to use) or pure
data about the survey's questions - both are safe to redefine for a new
form/deployment without touching any pipeline code.

WHERE THE VALUES BELOW ACTUALLY COME FROM: every constant below is a
FALLBACK default, used only if the GCS-hosted configuration workbook (see
_CONFIG_GCS_BUCKET/_CONFIG_GCS_BLOB just below) can't be read. On a normal
run, this module downloads and parses that workbook at import time and
OVERRIDES these defaults with whatever it finds - see _load_overrides()
near the bottom of this file. This means the real, current values a given
run is using can differ from what's printed here; read the workbook (or
this module's own `import pipeline_config; print(pipeline_config.GCS_BUCKET)`
etc. after import) to see what actually applied. Regenerate the workbook's
starting point with generate_pipeline_config_doc.py.

To port this pipeline to a different survey PDF: edit the "Survey
Questions" tab of that workbook (and the "Settings" tab for a different
bucket/project/table/threshold) - no code change needed. If the workbook is
unreachable, every step falls back to the values hardcoded below and prints
a warning; a stale fallback still WORKS, it just means the workbook's edits
aren't taking effect yet.
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
_CONFIG_GCS_BLOB = "Pipeline_Config/pipeline_configuration.xlsx"

# ==========================================================================
# GCS locations
# ==========================================================================
GCS_BUCKET = "tps_survey"
# step1's input: raw, multi-survey combined scans as they land from scanning.
GCS_RAW_PREFIX = "TPS_Scanned_2025/"
# step1's output, and step2/step3/step4's input: one 2-page PDF per survey,
# organized into <date folder>/<batch subfolder>/ - see step1_merge_pdf.py's
# split_combined_pdf()/organize_loose_pdfs().
GCS_SPLIT_PREFIX = "TPS_Scanned_2025_Reorgnized/"

# step6's input: the human-reviewed feedback/corrections spreadsheet.
GCS_FEEDBACK_BUCKET = "tps_survey"
GCS_FEEDBACK_BLOB = "TPS_Feedback/feedback_test.xlsx"
# step4's output / step6's input: the folder step4's --export-needs-review-
# feedback writes one "tps_feedback_{datetime}.xlsx" file into per run, and
# step6 reads the LATEST (by name, since the datetime suffix sorts
# lexicographically) file from - see export_needs_review_feedback() in
# step4_process_pdf.py and step6_load_feedback.py's find_latest_feedback_blob().
GCS_FEEDBACK_PREFIX = "TPS_Feedback/"

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
# step1: TPS-number extraction, the content/blank/declined check, and the
# survey-language gate all share this one model (explicit user request: keep
# a single model across every step1 Gemini call rather than mixing in a
# stronger/costlier one for just part of it - see validate_survey_language()
# and assess_page_content_and_declined() in step1_merge_pdf.py).
TPS_EXTRACTION_MODEL = "gemini-3.8-flash"
# step4: the main survey question/answer extraction model.
GEMINI_MODEL = "gemini-3.8-flash"
# None -> Cloud Vision uses the application-default GCP project, same
# convention as step4's own VERTEX_PROJECT_ID/BQ_PROJECT_ID.
VISION_PROJECT_ID = None

# ==========================================================================
# Report year
# ==========================================================================
# The year this batch of surveys was collected - used wherever a written
# date/digit reading needs a plausibility check (e.g. step4's H6 "does the
# year look right" sanity check, step1/step2's folder-name year fallback).
REPORT_YEAR = "2025"

# ==========================================================================
# step4 thresholds / feature flags
# ==========================================================================
# Model self-reported confidence (0-1) below which a model-only answer (no
# pixel/Vision corroboration) is flagged needs_review.
MODEL_CONFIDENCE_THRESHOLD = 0.7
# Word-level coverage (0-1) below which a freeform written-text field
# (H4/H5/H6/24) disagrees with Cloud Vision's OCR and is flagged.
VISION_FREEFORM_COVERAGE_THRESHOLD = 0.7
# Character-level overlap (0-1) tolerance for H2's short alphanumeric code.
H2_CHAR_OVERLAP_THRESHOLD = 0.8
# Character-level overlap (0-1) tolerance for H4's short agency-name answers
# (e.g. "N/A" vs Cloud Vision dropping the handwritten slash to read "NA").
H4_CHAR_OVERLAP_THRESHOLD = 0.5
# H6 (Today's Date) last-resort digit-overlap coverage (0-1): when Vision
# drops/misses whole digits outright (not a wrong-but-present digit), the
# fraction of digits that still line up before treating it as a pass.
H6_DIGIT_COVERAGE_THRESHOLD = 0.5
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
# This is the part to replace wholesale when porting to a different survey
# PDF. SURVEY_QUESTIONS is the single source of truth - step4 derives
# QUESTION_TEXT_BY_NUMBER, CHOICE_LISTS_BY_NUMBER, and WRITTEN_TEXT_QUESTION_
# NUMBERS straight from it, so those never need separate maintenance. The
# sets below it (MULTI_SELECT_QUESTION_NUMBERS onward) are the parts that
# genuinely can't be derived automatically - they encode real,
# case-by-case decisions about how each question's answer should be
# verified, made and documented over many real scans (see step4_process_
# pdf.py's own inline history for each one).

# No grouped "big question -> lettered sub-items" structure in this
# template (e.g. no "Q5a / Q5b / Q5c" under one shared stem) - a form that
# has one would populate this as {group_key: "shared stem text"}.
GROUP_HEADERS = {}

_SIX_POINT_SCALE = "Strongly Agree / Agree / I am Neutral / Disagree / Strongly Disagree / Not Applicable"
_SIX_POINT_SCALE_NA = "Strongly Agree / Agree / I am Neutral / Disagree / Strongly Disagree / N/A"
_MULTI_SELECT_NOTE = " (mark all that apply — more than one choice may be marked)"

# Each entry: (question_number, group_key, sub_text, choices).
# - question_number: the label used everywhere downstream ("H1".."H6", "1".."35").
# - group_key: None unless this question is a lettered sub-item of a shared
#   stem question (see GROUP_HEADERS above).
# - sub_text: the question's own printed text (combined with the group's
#   stem, if any, by build_full_question_text()).
# - choices: " / "-separated answer choices for a fixed-choice question, or
#   a plain descriptive string (no " / ") for an open write-in field - this
#   is exactly what step4 uses to tell the two question TYPES apart (see
#   WRITTEN_TEXT_QUESTION_NUMBERS's derivation).
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

# Questions where more than one choice can legitimately be marked at once.
MULTI_SELECT_QUESTION_NUMBERS = {"33", "34"}

# Every pixel-checked choice question (19-35, excluding 24/26 which are
# written-text/Vision-checked, not pixel-checked) where, on disagreement
# between the model's own reading and the deterministic pixel detector, the
# MODEL's answer is kept and the disagreement is only surfaced via
# needs_review, rather than the pixel reading being auto-applied. See
# step4_process_pdf.py's own inline history for the real per-file failures
# (Q20/Q23/Q25/Q29 pixel-detector errors, Q31/33/34 false-blanks) that
# established this policy.
MODEL_OVERRULES_PIXEL_QUESTION_NUMBERS = {
    "19", "20", "21", "22", "23", "25", "27", "28", "29", "30",
    "31", "32", "33", "34", "35",
}

# Of the open write-in fields (derived as WRITTEN_TEXT_QUESTION_NUMBERS from
# SURVEY_QUESTIONS itself - any entry with no " / " in its choices), H1/H2/26
# are short enough to realistically be ONE token on the page, so they're
# compared against Cloud Vision's individually-segmented word-level OCR
# tokens for an exact match. The rest can legitimately span multiple
# words/lines, so they're compared against Vision's fuzzy freeform coverage
# check instead. See cross_check_written_field_with_vision() in step4.
_VISION_TOKEN_FIELDS = {"H1", "H2", "26"}
_VISION_FREEFORM_FIELDS = {"H4", "H5", "H6", "24"}

# Written-text fields that are legitimately left blank far more often than
# the near-mandatory ID (H1) and date (H6) fields - exempted from the
# generic "blank final answer always needs review" backstop (any OTHER
# check, e.g. a Vision disagreement, can still flag them for its own
# reason).
_BLANK_ANSWER_EXEMPT_FIELDS = {"H4", "H5"}

# Written-text fields where, on disagreement, Cloud Vision's own anchored
# OCR reading is treated as MORE trustworthy than the model's and is used
# as survey_answer instead (still always flagged needs_review so a human
# confirms). Explicit user request, extended field by field as each one
# was confirmed to have Vision consistently right more often than the
# model on real scans - see _format_vision_authoritative_value()'s
# docstring in step4.
_VISION_AUTHORITATIVE_FIELDS = {"H2", "H5"}

# Which page (0-indexed) of a survey's 2-page PDF each written-text
# question's field actually sits on - used to fetch the right page's Cloud
# Vision OCR result without re-running it per question.
_WRITTEN_TEXT_QUESTION_PAGE = {"H1": 0, "H2": 0, "H4": 0, "H5": 0, "H6": 0, "24": 1, "26": 1}


# ==========================================================================
# Load overrides from the GCS-hosted configuration workbook (generated by
# generate_pipeline_config_doc.py). Every module-level name assigned above
# is a fallback default; this section OVERRIDES those names in place with
# whatever the workbook says, if it can be read at all. Never fatal - a
# missing/unreachable workbook, a missing sheet, a malformed row, or a
# missing optional dependency (google-cloud-storage/pandas/openpyxl) all
# just leave the built-in defaults above in effect, with a printed warning
# so the fallback is visible rather than silent.
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
        "GCS_FEEDBACK_BLOB", "GCS_FEEDBACK_PREFIX", "GCP_PROJECT_ID", "BQ_DATASET", "BQ_TABLE_MANIFEST",
        "BQ_TABLE_CALIBRATION", "BQ_TABLE_QUALITY", "BQ_TABLE_SURVEY_RESPONSES",
        "BQ_TABLE_CORRECTIONS", "BQ_TABLE_PIPELINE_CONFIG", "BQ_TABLE_FILE_QUALITY",
        "BQ_TABLE_SURVEY_RESPONSES_WITH_FEEDBACK", "VERTEX_LOCATION",
        "TPS_EXTRACTION_MODEL", "GEMINI_MODEL", "REPORT_YEAR",
    )
    float_params = (
        "MODEL_CONFIDENCE_THRESHOLD", "VISION_FREEFORM_COVERAGE_THRESHOLD",
        "H2_CHAR_OVERLAP_THRESHOLD", "H4_CHAR_OVERLAP_THRESHOLD", "H6_DIGIT_COVERAGE_THRESHOLD",
    )
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
        elif name in bool_params:
            g[name] = _is_yes(raw)
        elif name == "VISION_PROJECT_ID":
            g[name] = raw or None
        # else: a recognized-but-unhandled name (shouldn't happen given the
        # lists above cover every settings-sheet row) - leave the default.


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

    for row in rows:
        number = row.get("Number", "")
        if not number:
            continue  # blank template/example row - skip rather than error
        group_key = row.get("Group Key") or None
        question_text = row.get("Question Text", "")
        choices = _first_present(
            row,
            "Answer Choices (only for Single/Multiple Choice - separate with ' / ')",
            "Answer Choices (separate with ' / ', leave blank for open write-in)",
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

        page = _first_present(
            row,
            "Which page of the 2-page form is this on? (0 = first page, 1 = second page; leave blank for a Single/Multiple Choice question)",
            "Page (0 or 1)",
        )
        if page != "":
            written_text_page[number] = int(float(page))

    if not survey_questions:
        return  # empty/template-only sheet - keep the built-in default question list

    g["SURVEY_QUESTIONS"] = survey_questions
    g["MULTI_SELECT_QUESTION_NUMBERS"] = multi_select
    g["MODEL_OVERRULES_PIXEL_QUESTION_NUMBERS"] = model_overrules_pixel
    g["_BLANK_ANSWER_EXEMPT_FIELDS"] = blank_exempt
    g["_VISION_TOKEN_FIELDS"] = vision_token
    g["_VISION_FREEFORM_FIELDS"] = vision_freeform
    g["_VISION_AUTHORITATIVE_FIELDS"] = vision_authoritative
    g["_WRITTEN_TEXT_QUESTION_PAGE"] = written_text_page


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

        questions_df = pd.read_excel(io.BytesIO(data), sheet_name="Survey Questions")
        rows = [
            {col: _clean(row[col]) for col in questions_df.columns}
            for _, row in questions_df.iterrows()
        ]
        _apply_survey_questions(rows)

        print(f"[pipeline_config] Loaded settings + survey schema from {location}.")
    except Exception as e:  # noqa: BLE001 - a config-doc problem must never block the pipeline; fall back to built-in defaults
        print(
            f"[pipeline_config] WARNING: could not load {location} ({e}) - "
            "using this module's built-in fallback defaults instead. If this "
            "is unexpected, check the workbook exists at that path and that "
            "this environment can reach GCS."
        )


_load_overrides()
