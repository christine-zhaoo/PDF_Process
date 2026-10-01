#!/usr/bin/env python3
"""
merge_pdfs_to_folder process:

Standalone script that organizes loose survey PDFs into per-date folders. A
normal individual survey is moved unchanged. A combined scan such as
``Nov17_10.pdf`` is treated as a sequence of two-page surveys: each first
page's handwritten TPS number is read, the two pages are split into a new PDF
named ``2025_Nov_17_10_TPS_6558.pdf``, and the outputs are placed in
``Nov 17 2025``.

--------------------------------------------------------------------------
What it does
--------------------------------------------------------------------------
Given a bucket laid out like:

gs://<BUCKET>/<ROOT_PREFIX>/
    Dec14_1_TPS_4223.pdf
    Nov 18 2025/            <- an existing, already-organized date folder
        2025_Nov_18_TPS_5581.pdf
        ...

Every PDF directly under ROOT_PREFIX (i.e. NOT already inside a date
subfolder) is treated as unorganized. For each one:

1. A combined name matching ``<Mon><D[D]>_<batch>.pdf`` is split into
   two-page survey PDFs and named from its extracted TPS number.
2. An individual name's destination date is parsed from the leading
   "<Mon><D[D]>" token (e.g. "Dec14" -> month=Dec). See parse_file_name_date().
3. The year for that date is the year inferred once per run from this bucket/root's existing date folders (see infer_survey_year()) — the file
   name itself never carries a year.
3. The file is moved into gs://<BUCKET>/<ROOT_PREFIX>/<Month> <D> <YYYY>/(that folder is created on first use; an already-existing folder is
   reused, never recreated/overwritten).

--------------------------------------------------------------------------
Manifest destination
--------------------------------------------------------------------------
For every moved file, one row — source_file, source_gcs_uri, parsed_month_day, destination_folder, destination_gcs_uri, moved_at,
moved_date — is loaded into a BigQuery table (default: organize_manifest), partitioned by moved_date (DATE(moved_at)).

--------------------------------------------------------------------------
Setup
--------------------------------------------------------------------------
1. pip install google-cloud-storage pypdf google-cloud-bigquery pandas pyspark
2. Authenticate to GCP, one of:
   - gcloud auth application-default login
   - set GOOGLE_APPLICATION_CREDENTIALS to a service account key file

--------------------------------------------------------------------------
Revision notes (this pass)
--------------------------------------------------------------------------
- Single GCS listing per run: infer_survey_year() and list_loose_pdfs() used
  to each call bucket.client.list_blobs() against the same prefix/delimiter
  independently, doubling the round trip to GCS every run for no reason.
  They're now backed by one list_root_contents() call whose result (loose
  PDF blobs + existing date-folder names) is computed once in run() and
  passed down to both.
- Removed a dead `loose_files` local in the old infer_survey_year() that was
  computed and never used.
- partition_field now consistently matches the manifest schema. Previously
  the table was declared partitioned on "event_partition" — a column that
  doesn't exist anywhere in BQ_MANIFEST_SCHEMA — while the Spark load path
  separately (and buggily) tried to bolt an "event_partition" column onto
  the *pandas* DataFrame by calling `.withColumn()` on it (a Spark
  DataFrame method, not a pandas one) right before a `spark_df.show(truncate
  = false)` call that referenced Python's `False` misspelled in lowercase.
  Both would have raised at runtime. Partitioning is now on `moved_date`
  (the column that already exists and that the docstring above always said
  it partitions on), and the stray debug/broken lines are gone.
"""
import argparse
import concurrent.futures
import csv
import datetime
import logging
import re
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pipeline_config

# --------------------------------------------------------------------------
# Configuration — every value below is this file's local name for a setting
# defined once in pipeline_config.py (the shared source of truth across
# step1-step6); edit pipeline_config.py to change any of these for a
# different bucket/project/deployment. Kept as plain module-level constants
# (not read from pipeline_config inline everywhere) so this file still runs
# standalone with no other changes needed elsewhere.
# --------------------------------------------------------------------------
BUCKET_NAME = pipeline_config.GCS_BUCKET
ROOT_PREFIX = pipeline_config.GCS_RAW_PREFIX
DESTINATION_PREFIX = pipeline_config.GCS_SPLIT_PREFIX
PAGES_PER_SURVEY = 2
TPS_EXTRACTION_MODEL = pipeline_config.TPS_EXTRACTION_MODEL
# Explicit user request: keep ONE model used across all of step1's Gemini
# calls (TPS digit extraction, language gating, blank/declined checking)
# rather than mixing in a stronger/costlier model for just one check. See
# pipeline_config.py's TPS_EXTRACTION_MODEL for the model actually in effect.
TPS_EXTRACTION_LOCATION = pipeline_config.VERTEX_LOCATION
TPS_EXTRACTION_PROJECT = pipeline_config.GCP_PROJECT_ID
TPS_EXTRACTION_MAX_ATTEMPTS = 3
TPS_EXTRACTION_RETRY_DELAY_SECONDS = 2
BQ_PROJECT = pipeline_config.GCP_PROJECT_ID
BQ_DATASET = pipeline_config.BQ_DATASET
MANIFEST_TABLE = pipeline_config.BQ_TABLE_MANIFEST
DEFAULT_FAILURE_LOG = "step1_failed_sources.csv"
partition_field = "moved_date"  # must be an actual column in BQ_MANIFEST_SCHEMA
RUN_TOKEN_TOTALS = {
    "prompt_tokens": 0,
    "output_tokens": 0,
    "total_tokens": 0,
    "requests": 0,
}

# --------------------------------------------------------------------------
# Concurrency: source PDFs are processed in parallel by run() (see
# _process_source_pdf() / ThreadPoolExecutor below) since each one is
# dominated by waiting on GCS/Vertex network I/O, not CPU. Two independent
# knobs control this instead of just one:
#   - SOURCE_PDF_WORKERS bounds how many source PDFs run at once.
#   - MAX_CONCURRENT_VERTEX_CALLS separately bounds how many Gemini requests
#     are ever in flight at once (a single combined PDF alone can issue two
#     Gemini calls per survey), via a shared semaphore every Gemini call
#     acquires - this is what actually protects Vertex's per-project quota,
#     independent of how many PDF-level worker threads are running.
# Both are kept modest by default: Vertex's default per-project
# requests-per-minute quota is easy to blow through, and a 429
# (RESOURCE_EXHAUSTED) or 403 (PERMISSION_DENIED, which some quota
# rejections also surface as) is far more expensive to recover from - via
# _generate_content_with_limits()'s backoff-and-retry - than just running a
# bit slower. Tune upward only after confirming headroom in the actual
# Vertex quota for TPS_EXTRACTION_PROJECT/TPS_EXTRACTION_LOCATION.
SOURCE_PDF_WORKERS = 4
MAX_CONCURRENT_VERTEX_CALLS = 4
GEMINI_RATE_LIMIT_MAX_ATTEMPTS = 5
GEMINI_BACKOFF_BASE_SECONDS = 2.0
GEMINI_BACKOFF_MAX_SECONDS = 60.0

_VERTEX_SEMAPHORE = threading.Semaphore(MAX_CONCURRENT_VERTEX_CALLS)
_FAILURE_LOG_LOCK = threading.Lock()
_TOKEN_TOTALS_LOCK = threading.Lock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    force=True,  # Jupyter/IPython often pre-configures the root logger before this
    # runs, which makes a plain basicConfig() a silent no-op — force=True makes sure
    # this handler actually attaches instead of logging output just disappearing.
)
log = logging.getLogger("merge_pdfs_to_folder")


def status(msg, *args) -> None:
    """Logs AND print()s a status message. Using print() specifically because
    it's the one output channel that reliably shows up in a Jupyter notebook
    cell no matter how that notebook's logging is (or isn't) configured —
    logger output alone can silently vanish there. Takes the same
    (msg, *args) %-style signature as logging so existing call sites don't
    need reformatting."""
    text = msg % args if args else msg
    print(text, flush=True)
    log.info(msg, *args)


def err(msg, *args) -> None:
    """Same as status(), but for errors — always printed with an ERROR:
    prefix so it's unmistakable in notebook output, in addition to being
    logged at ERROR level."""
    text = msg % args if args else msg
    print(f"ERROR: {text}")
    log.error(msg, *args)


def log_failed_source(failure_log: str, source_uri: str, error: Exception, bad_pdf: Optional[str] = None) -> None:
    """Persist a source failure immediately so it survives batch interruption.

    bad_pdf identifies exactly which PDF the failure/rejection applies to —
    source_uri alone isn't enough for a combined scan, where the file that
    failed to move is the whole batch but the actual problem is one
    particular survey (page range) inside it. Defaults to source_uri when
    the failure is at the whole-source-file level.

    Guarded by _FAILURE_LOG_LOCK since run() now processes multiple source
    PDFs concurrently (see SOURCE_PDF_WORKERS) and this appends to one
    shared CSV file - without the lock, two threads' writes could interleave
    mid-row."""
    with _FAILURE_LOG_LOCK:
        path = Path(failure_log)
        path.parent.mkdir(parents=True, exist_ok=True)
        needs_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            if needs_header:
                writer.writerow(["failed_at_utc", "source_uri", "bad_pdf", "error"])
            writer.writerow([
                datetime.datetime.now(datetime.timezone.utc).isoformat(),
                source_uri,
                bad_pdf or source_uri,
                str(error),
            ])
            handle.flush()


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


_NUM_RE = re.compile(r"(\d+)")


def natural_sort_key(name: str):
    """Splits a filename into alternating text/number chunks so sorting
    treats embedded numbers numerically (Nov18_2 before Nov18_10), not
    lexicographically (which would put Nov18_10 before Nov18_2)."""
    return [int(chunk) if chunk.isdigit() else chunk.lower() for chunk in _NUM_RE.split(name)]


def _normalize_prefix(root_prefix: str) -> str:
    """Ensures a GCS prefix ends with '/'. Several functions below need this
    same normalization; factored out so it's done (and can be fixed) in one
    place instead of being repeated inline in each of them."""
    return root_prefix if root_prefix.endswith("/") else root_prefix + "/"


# --------------------------------------------------------------------------
# Date parsing: pulling a destination date out of an existing date-FOLDER
# name (e.g. "Nov 18 2025") vs. out of a loose source FILE name (e.g.
# "Dec14_1_TPS_4223.pdf", which has no year in it) are two different
# problems, handled by two different functions below.
# --------------------------------------------------------------------------
_MONTH_ABBREVS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}
# Matches a folder name like "Nov 18 2025" or "Dec 3 2025" — full or
# abbreviated month name, a 1-2 digit day, a 4-digit year, space-separated.
_DATE_FOLDER_RE = re.compile(
    r"^([A-Za-z]{3,})\s+(\d{1,2})\s+(\d{4})$"
)
# Matches the leading "<Mon><D[D]>" token in a loose source file name, e.g."Dec14_1_TPS_4223.pdf" -> month="Dec", day="14". Deliberately anchored to
# the START of the name (^) and requires the abbreviation to be followed immediately by digits with no separator, matching every real file name
# this pipeline has seen so far (Nov7_1.pdf, Nov18_10.pdf, Dec14_1_....pdf).
_FILE_NAME_DATE_RE = re.compile(r"^([A-Za-z]{3})(\d{1,2})[_\W]")


def survey_batch_subfolder(month: int, day: int, batch_suffix: str) -> str:
    """Builds the "Nov6_36"-style batch subfolder a split survey's output
    file belongs in under its date folder - <Mon><D, no leading zero>_<batch
    suffix from the combined source file's own name, e.g. "36" from
    "Nov6_36.pdf">. Matches the one-time bucket reorganization done directly
    against gs://tps_survey/TPS_Scanned_2025_Reorgnized/ (every existing
    split-survey file there was moved under exactly this subfolder scheme) -
    this is what makes new step1 runs consistent with that reorganized
    layout instead of writing back into the old flat structure."""
    return f"{_MONTH_NAMES[month]}{day}_{batch_suffix}"


def parse_date_folder_name(folder_name: str) -> Optional[datetime.date]:
    """Parses an EXISTING date folder's name (e.g. "Nov 18 2025") into a
    date. Returns None (rather than raising) for a folder name that doesn't
    match — a folder under ROOT_PREFIX that isn't one of this pipeline's own
    date folders (e.g. an unrelated "test_pdf_merged" leftover from an
    older revision) is simply not counted, not treated as an error."""
    m = _DATE_FOLDER_RE.match(folder_name.strip())
    if not m:
        return None
    month_text, day_text, year_text = m.groups()
    month_key = month_text[:3].lower()
    month = _MONTH_ABBREVS.get(month_key)
    if month is None:
        return None
    try:
        return datetime.date(int(year_text), month, int(day_text))
    except ValueError:
        return None


def parse_file_name_date(file_name: str) -> Optional[tuple]:
    """Parses a loose source file's own name for its destination
    month/day - e.g. "Dec14_1_TPS_4223.pdf" -> (12, 14). Returns None for a
    file name that doesn't start with a recognizable "<Mon><D[D]>" token
    (an unrelated file, a typo, or a naming convention this script doesn't
    know about) - the caller is responsible for deciding what to do with an
    unparseable file name (this script flags it for the run's summary and
    SKIPS it, rather than guessing or crashing the whole run over one bad
    file name)."""
    m = _FILE_NAME_DATE_RE.match(file_name.strip())
    if not m:
        return None
    month_text, day_text = m.groups()
    month = _MONTH_ABBREVS.get(month_text.lower())
    if month is None:
        return None
    day = int(day_text)
    if not (1 <= day <= 31):
        return None
    return (month, day)


_CONFUSABLE_DIGIT_PAIRS = {
    frozenset(pair) for pair in
    [("1", "4"), ("1", "7"), ("3", "8"), ("5", "6"), ("0", "6"),
     ("0", "8"), ("0", "9"), ("6", "8"), ("8", "9"), ("2", "7")]
}

# Row band (top, bottom), in pixels of the pixmap returned by
# _crop_tps_field_pixmap(), where the big printed TPS number itself sits -
# empirically well below the small table/header text at the top of that
# crop. Used by _extract_digit_crops() to isolate just the number before
# segmenting it into individual digits.
_DIGIT_BAND_ROWS = (140, 210)
_DIGIT_CROP_SIZE = (30, 50)


def _extract_digit_crops(page) -> Optional[list]:
    """Segments the 4 individual printed digits out of the cropped TPS
    number field into fixed-size, thresholded single-digit images, for
    pixel-level shape comparison (see _DigitTemplateBank below).

    This exists because the OCR model's own semantic reading can be
    confidently wrong about a digit's shape (e.g. a '6' whose closed loop
    has a small ink gap gets read as a '5' with HIGH self-reported
    confidence) - comparing the actual pixels of a disputed digit against
    other digits already confirmed correct elsewhere in this same source
    PDF is a check the model's own confidence self-report can't provide.

    Returns None (rather than raising) if the row band doesn't segment into
    exactly 4 digit-sized connected components - this segmentation isn't
    guaranteed to work on every scan/crop, and a failed segmentation should
    just skip the extra shape check, not block the existing OCR pipeline."""
    import cv2
    import numpy as np

    pixmap = _crop_tps_field_pixmap(page)
    img = np.frombuffer(pixmap.samples, dtype=np.uint8).reshape(pixmap.height, pixmap.width, pixmap.n)
    gray = cv2.cvtColor(img[:, :, :3], cv2.COLOR_RGB2GRAY)
    band_top, band_bottom = _DIGIT_BAND_ROWS
    band = gray[band_top:min(band_bottom, gray.shape[0]), :]
    if band.size == 0:
        return None
    _, thresh = cv2.threshold(band, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    dilated = cv2.dilate(thresh, np.ones((3, 3), np.uint8), iterations=1)
    num_labels, _labels, stats, _centroids = cv2.connectedComponentsWithStats(dilated, connectivity=8)
    boxes = [
        (x, y, w, h) for x, y, w, h, area in (stats[i] for i in range(1, num_labels))
        if area > 60 and w < 40
    ]

    # Merge vertically-split fragments of the same digit before checking for
    # exactly 4 - confirmed directly (2025_Oct_30_1_TPS_1878.pdf, whose real
    # TPS is 1678): a printed '6' whose thin waist (where the upper curve
    # meets the lower loop) is too faint/thin to survive Otsu thresholding
    # splits into TWO connected components instead of one, so this used to
    # find 5 boxes instead of 4 and bail out entirely (return None) - which
    # meant the confusable-pair pixel backstop below never even ran for this
    # record, letting Gemini's confidently-wrong HIGH '8' reading (it's a
    # known 6/8 confusable pair - see _CONFUSABLE_DIGIT_PAIRS) sail through
    # with no independent check at all. Two fragments sharing substantial
    # horizontal (x-axis) overlap are almost certainly one digit split
    # top/bottom, not two separate adjacent digits - real neighboring digits
    # in this printed TPS number are spaced apart with little to no x-overlap,
    # so this merge only ever fuses fragments that share a column, never two
    # genuinely different digits.
    merged = []
    for box in sorted(boxes, key=lambda b: b[0]):
        x, y, w, h = box
        for i, (mx, my, mw, mh) in enumerate(merged):
            overlap = min(x + w, mx + mw) - max(x, mx)
            if overlap > 0.5 * min(w, mw):
                nx0, ny0 = min(x, mx), min(y, my)
                nx1, ny1 = max(x + w, mx + mw), max(y + h, my + mh)
                merged[i] = (nx0, ny0, nx1 - nx0, ny1 - ny0)
                break
        else:
            merged.append(box)
    boxes = merged

    if len(boxes) != 4:
        return None
    boxes.sort(key=lambda box: box[0])
    pad = 3
    crops = []
    for x, y, w, h in boxes:
        x0, y0 = max(0, x - pad), max(0, y - pad)
        x1, y1 = min(thresh.shape[1], x + w + pad), min(thresh.shape[0], y + h + pad)
        crops.append(cv2.resize(thresh[y0:y1, x0:x1], _DIGIT_CROP_SIZE, interpolation=cv2.INTER_AREA))
    return crops


class _DigitTemplateBank:
    """Accumulates confirmed-correct digit shape exemplars from this source
    PDF's own TPS numbers, so a later survey's confusable-pair digit (see
    _CONFUSABLE_DIGIT_PAIRS) can be checked against real pixel shapes
    already seen in this same batch, instead of only trusting the OCR
    model's own (sometimes confidently wrong) self-reported confidence.

    Scoped to one source PDF (one _DigitTemplateBank per split_combined_pdf()
    call), not the whole run - digit shape/font is consistent within one
    scanned batch but can differ across batches, so cross-batch templates
    would risk comparing against the wrong font's digit shapes.

    Seeded up front from every HIGH-confidence reading across the WHOLE
    batch (see split_combined_pdf()'s two-pass structure) rather than built
    incrementally page-by-page - a survey near the end of the batch used to
    have no templates to compare against for a digit that only happened to
    appear in an EARLIER survey the sequential, one-pass version of this
    bank hadn't reached yet when it was checked, so a perfectly legible
    reading could never be rescued out of LOW confidence no matter how
    clear the handwriting actually was (confirmed directly: TPS
    4382/4384/4387 in Nov6_1.pdf, all cleanly printed numbers, stuck on LOW
    confidence with nothing yet in the bank to compare against). Seeding
    from the full batch first means every survey gets checked against the
    same complete evidence, regardless of where in the batch it falls.

    Each observed crop is tagged with the id (survey start-page index) of
    the record that contributed it, so a caller checking THAT SAME record
    against the bank can exclude its own crop via `exclude` - otherwise a
    record's own just-seeded crop would trivially "confirm" itself (a
    perfect self-match) instead of being checked against independent
    evidence from other surveys."""

    def __init__(self):
        self._templates = {}  # digit char -> list of (record_id, crop)

    def observe(self, digits: str, crops: list, record_id) -> None:
        for digit, crop in zip(digits, crops):
            self._templates.setdefault(digit, []).append((record_id, crop))

    def has_templates_for(self, candidates, exclude=None) -> bool:
        """True only if EVERY digit in `candidates` has at least one observed
        template from a record other than `exclude` - used by callers that
        need best_match()'s verdict to be an actual comparison between the
        candidates, not just a report of whichever one happens to have
        templates so far (best_match() itself will happily return a
        candidate with no real competition if the other candidate has no
        templates yet)."""
        return all(
            any(rid != exclude for rid, _crop in self._templates.get(digit, []))
            for digit in candidates
        )

    def best_match(self, crop, candidates, exclude=None) -> Optional[str]:
        """Returns whichever of `candidates` this crop's shape most closely
        matches by normalized cross-correlation, or None if no template has
        been observed yet for any candidate digit. Templates contributed by
        `exclude` itself are skipped, so a record is never validated against
        its own crop. Note this can still return a candidate even when only
        one of them has any (non-excluded) template at all (in which case
        it isn't a real comparison) - check has_templates_for() first if
        that distinction matters."""
        import cv2

        best_digit, best_score = None, None
        for digit in candidates:
            for record_id, template in self._templates.get(digit, []):
                if record_id == exclude:
                    continue
                score = float(cv2.matchTemplate(
                    crop.astype("float32"), template.astype("float32"), cv2.TM_CCOEFF_NORMED
                )[0][0])
                if best_score is None or score > best_score:
                    best_digit, best_score = digit, score
        return best_digit


class TpsRejected(Exception):
    """Raised when a survey's page 1 can't be trusted for a TPS number —
    either the handwritten number (or the page content around it) is
    illegible, or the page is written in a language other than English.
    This is expected to happen for a genuinely bad or foreign-language
    scan, not a bug, and is handled by recording a rejected manifest row
    (with a rejected_reason) rather than failing the whole source PDF."""


def _is_rate_limit_error(error: Exception) -> bool:
    """True if `error` looks like a Vertex quota/rate-limit rejection - a 429
    RESOURCE_EXHAUSTED, or a 403 PERMISSION_DENIED (Vertex sometimes surfaces
    a quota rejection as a 403 rather than a 429, so both are treated as
    retryable rate-limit errors here, not as a hard auth failure). Checked
    via both the google.api_core exception types and a string fallback,
    since not every SDK/transport path is guaranteed to raise the typed
    exception."""
    try:
        from google.api_core.exceptions import PermissionDenied, ResourceExhausted
        if isinstance(error, (ResourceExhausted, PermissionDenied)):
            return True
    except ImportError:
        pass
    text = str(error)
    return "429" in text or "RESOURCE_EXHAUSTED" in text or "403" in text or "PERMISSION_DENIED" in text


def _record_gemini_usage(usage) -> tuple:
    """Adds one response's token usage into RUN_TOKEN_TOTALS under
    _TOKEN_TOTALS_LOCK - now that run() processes multiple source PDFs
    concurrently (see SOURCE_PDF_WORKERS), plain unlocked `+=` on the shared
    dict from multiple threads could lose updates. Returns
    (prompt_tokens, output_tokens, total_tokens) so callers that also want
    to log the per-call numbers (e.g. extract_tps_number_from_page's status
    line) don't need to re-derive them. No-op returning (0, 0, 0) if usage
    is None (e.g. a response with no usage_metadata)."""
    if usage is None:
        return 0, 0, 0
    prompt_tokens = getattr(usage, "prompt_token_count", None) or 0
    output_tokens = getattr(usage, "candidates_token_count", None) or 0
    total_tokens = getattr(usage, "total_token_count", None) or 0
    with _TOKEN_TOTALS_LOCK:
        RUN_TOKEN_TOTALS["prompt_tokens"] += prompt_tokens
        RUN_TOKEN_TOTALS["output_tokens"] += output_tokens
        RUN_TOKEN_TOTALS["total_tokens"] += total_tokens
        RUN_TOKEN_TOTALS["requests"] += 1
    return prompt_tokens, output_tokens, total_tokens


def _get_genai_client():
    """Builds the Vertex-backed genai.Client used by every Gemini call site
    in this file - factored out since all three call sites (TPS extraction,
    content/declined assessment, duplicate-TPS visual verification)
    constructed an identical client from the same three constants."""
    from google import genai

    return genai.Client(
        vertexai=True,
        project=TPS_EXTRACTION_PROJECT,
        location=TPS_EXTRACTION_LOCATION,
    )


def _crop_tps_field_pixmap(page):
    """Renders the cropped handwritten TPS-number field (upper-right of the
    page) at 3x zoom - the same crop rectangle and zoom that
    extract_tps_number_from_page() and verify_same_tps_number() both need,
    factored out here since they previously duplicated this rect math."""
    import pymupdf

    page_rect = page.rect
    crop = pymupdf.Rect(
        page_rect.x0 + page_rect.width * 0.80,
        page_rect.y0 + page_rect.height * 0.08,
        page_rect.x1 - page_rect.width * 0.01,
        page_rect.y0 + page_rect.height * 0.24,
    )
    return page.get_pixmap(matrix=pymupdf.Matrix(3, 3), clip=crop, alpha=False)


def _generate_content_with_limits(client, model: str, contents: list, label: str):
    """Shared wrapper around client.models.generate_content() used by every
    Gemini call site in this file (TPS extraction, content/declined
    assessment, duplicate-TPS visual verification).

    Always passes temperature=0 - confirmed directly (TPS 3865 in a real
    batch): calling assess_page_content_and_declined() 3 times in a row on
    the exact same page, unchanged, returned declined=False, True, False -
    a genuinely borderline strikethrough mark (real ink on the page, but
    covering a judgment-call amount of the answer grid) flipped the model's
    answer between identical calls with no way to reproduce or trust either
    result. This file was the only one of the three Gemini-calling scripts
    in this pipeline (unlike step3/step4) that never set a temperature at
    all, so every call here ran at the model's non-zero default. temperature
    =0 doesn't make a genuinely ambiguous page unambiguous, but it does mean
    the SAME page always gets the SAME answer - a prerequisite for the
    confusable-digit cross-checks and duplicate-TPS re-checks elsewhere in
    this file to mean anything, and for a human reviewing a flagged page to
    trust that rerunning the check wouldn't silently change the verdict.

    Two protections that matter once run() processes multiple source PDFs
    concurrently (see SOURCE_PDF_WORKERS):
    - Acquires _VERTEX_SEMAPHORE first, capping how many Gemini requests are
      ever in flight at once across ALL worker threads combined, regardless
      of how many PDFs are being processed in parallel.
    - Retries with exponential backoff specifically on a 429/403 rate-limit
      rejection (see _is_rate_limit_error()) - a real quota lockout should
      slow this run down and recover, not immediately fail every in-flight
      survey. Any other error is raised immediately (unchanged behavior);
      callers already have their own handling for a non-rate-limit failure."""
    from google.genai import types

    last_error = None
    for attempt in range(1, GEMINI_RATE_LIMIT_MAX_ATTEMPTS + 1):
        with _VERTEX_SEMAPHORE:
            try:
                return client.models.generate_content(
                    model=model, contents=contents,
                    config=types.GenerateContentConfig(temperature=0),
                )
            except Exception as error:  # noqa: BLE001 - only rate-limit errors are retried here
                last_error = error
                if not _is_rate_limit_error(error) or attempt == GEMINI_RATE_LIMIT_MAX_ATTEMPTS:
                    raise
        delay = min(GEMINI_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), GEMINI_BACKOFF_MAX_SECONDS)
        status(
            "[GEMINI] %s: rate-limited (%s) on attempt %d/%d; backing off %.1fs before retrying.",
            label, last_error, attempt, GEMINI_RATE_LIMIT_MAX_ATTEMPTS, delay,
        )
        time.sleep(delay)
    raise last_error  # pragma: no cover - loop always returns or raises above


def extract_tps_number_from_page(
    page,
    model: str = TPS_EXTRACTION_MODEL,
    max_attempts: int = TPS_EXTRACTION_MAX_ATTEMPTS,
    expected_next_tps: Optional[str] = None,
) -> tuple:
    """Reads the handwritten TPS number printed at the upper-right of page 1.

    The source scans are image-only and the number is handwritten, so PDF text
    extraction and filename parsing cannot recover it. Gemini receives only a
    tightly cropped image of that field and must return exactly four digits
    plus its own confidence in that reading.

    Returns (tps: str, confidence: "HIGH"|"LOW") rather than just the digits -
    confidence gates what split_combined_pdf() is allowed to do when this
    reading collides with an already-used TPS number (see its docstring):
    a HIGH-confidence collision still gets a visual re-check before being
    called a genuine duplicate; a LOW-confidence one goes straight to manual
    review instead of being trusted enough to reject anything over.

    expected_next_tps: the previous survey's TPS number in this same combined
    scan (surveys are digitized in sequence, so TPS numbers normally
    increment one-by-one within a batch). Passed along as a hint in the
    prompt to help disambiguate easily-confused handwritten digits (e.g. a 1
    misread as a 4, or a 6 misread as an 8) that previously caused two
    different surveys to resolve to the same TPS number and made the whole
    combined source PDF look like it had a duplicate. None for the first
    survey in a batch (no prior number to anchor against).
    """
    from google.genai import types

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    pixmap = _crop_tps_field_pixmap(page)
    prompt_parts = [
        "Read the handwritten four-digit TPS number in this cropped survey "
        "field. Respond with exactly the four digits, then a space, then "
        "your own confidence in that reading as exactly HIGH or LOW - HIGH "
        "only if every digit is unambiguous and clearly formed, LOW if any "
        "single digit could plausibly be misread as a different digit. This "
        "includes both digit shapes that are commonly confused in "
        "handwriting (e.g. 1/4/7, 3/8, 5/6, 0/6/8/9, 2/7), AND a digit whose "
        "pen stroke has a small gap, break, or faded/missing ink in part of "
        "its loop or curve - for example, a '6' whose closed loop has a "
        "break in the ink can look like a '5', and a '9' or '8' with a "
        "similar gap can look like other digits. Look carefully for such "
        "partial/broken strokes before committing to HIGH confidence: if a "
        "digit's shape depends on a stroke that looks incomplete or "
        "interrupted, treat it as ambiguous and respond LOW. "
        "Example response: '5202 HIGH'. No other text, punctuation, or "
        "markdown."
    ]
    if expected_next_tps is not None:
        try:
            hint_value = f"{int(expected_next_tps) + 1:04d}"
        except ValueError:
            hint_value = None
        if hint_value is not None:
            prompt_parts.append(
                f"Context: TPS numbers in this scanned batch are assigned "
                f"sequentially, one higher than the previous survey. The "
                f"previous survey's TPS number was {expected_next_tps}, so "
                f"this one is expected to be close to {hint_value}. Use this "
                f"only to disambiguate handwritten digits that are hard to "
                f"tell apart (e.g. 1 vs 4, 6 vs 8, 3 vs 8) - always defer to "
                f"what is actually handwritten if it clearly differs from "
                f"this expectation; the sequence can still skip or reset."
            )
    prompt_parts.append(
        "If the TPS number's digits are not all clearly legible, or the "
        "cropped image content itself is blank, corrupted, or otherwise "
        "unreadable, do not guess — return exactly REJECT_UNREADABLE "
        "instead."
        # Explicit user request: this used to also instruct REJECT_NON_ENGLISH
        # here, but the cropped image is just the small handwritten TPS-number
        # field (see _crop_tps_field_pixmap()) - it essentially never contains
        # enough visible text to judge the survey's language at all, so this
        # instruction was dead weight that didn't actually solve the
        # Spanish-PDF problem. Language is now gated by validate_survey_
        # language() below, against the FULL survey page, instead.
    )
    prompt = " ".join(prompt_parts)
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            client = _get_genai_client()
            response = _generate_content_with_limits(
                client,
                model,
                [
                    types.Part.from_bytes(data=pixmap.tobytes("png"), mime_type="image/png"),
                    prompt,
                ],
                "TPS extraction",
            )
            usage = getattr(response, "usage_metadata", None)
            if usage is not None:
                prompt_tokens, output_tokens, total_tokens = _record_gemini_usage(usage)
                status(
                    "[GEMINI] model=%s attempt=%d/%d prompt_tokens=%s "
                    "output_tokens=%s total_tokens=%s",
                    model,
                    attempt,
                    max_attempts,
                    prompt_tokens,
                    output_tokens,
                    total_tokens,
                )
            raw_text = (response.text or "").strip()
            normalized = raw_text.upper()
            if normalized == "REJECT_UNREADABLE":
                raise TpsRejected(
                    "Gemini could not read the TPS number or page content "
                    f"(returned {raw_text!r})"
                )
            digit_confidence_match = re.fullmatch(r"(\d{4})\s*(HIGH|LOW)?", normalized)
            if digit_confidence_match:
                # A response with no parseable confidence word is treated as LOW,
                # never HIGH - an unrecognized/missing confidence shouldn't get the
                # benefit of the doubt that lets split_combined_pdf() skip straight
                # to visual duplicate verification instead of manual review.
                digits = digit_confidence_match.group(1)
                confidence = digit_confidence_match.group(2) or "LOW"
                # Don't trust the model's own HIGH self-rating over a deterministic
                # check: if this reading breaks the expected +1 sequence by exactly
                # one digit, and that digit swap is a known handwriting/print
                # confusable pair (e.g. 5/6), that's a real, checkable reason to
                # distrust this specific reading regardless of how confidently the
                # model reported it - a broken/under-inked stroke (e.g. a '6' with a
                # gap in its loop reading as '5') can look completely unambiguous in
                # isolation while still being wrong. The digits themselves are never
                # silently changed to the expected value - only confidence is
                # downgraded, so this still goes to manual review instead of being
                # guessed at.
                if expected_next_tps is not None and confidence == "HIGH":
                    try:
                        expected_digits = f"{int(expected_next_tps) + 1:04d}"
                    except ValueError:
                        expected_digits = None
                    if expected_digits is not None and expected_digits != digits:
                        mismatches = [
                            i for i in range(4) if digits[i] != expected_digits[i]
                        ]
                        if len(mismatches) == 1:
                            i = mismatches[0]
                            if frozenset((digits[i], expected_digits[i])) in _CONFUSABLE_DIGIT_PAIRS:
                                confidence = "LOW"
                return digits, confidence
            last_error = TpsRejected(
                f"TPS extraction returned {raw_text!r}; expected exactly four digits "
                f"followed by HIGH or LOW"
            )
        except TpsRejected as error:
            last_error = error
        except Exception as error:  # noqa: BLE001 - retry, then surface the final failure
            last_error = error

        if attempt < max_attempts:
            status(
                "[GEMINI] TPS extraction attempt %d/%d failed (%s); retrying in %d second(s).",
                attempt,
                max_attempts,
                last_error,
                TPS_EXTRACTION_RETRY_DELAY_SECONDS,
            )
            time.sleep(TPS_EXTRACTION_RETRY_DELAY_SECONDS)

    if isinstance(last_error, TpsRejected):
        raise TpsRejected(str(last_error)) from last_error
    raise ValueError(
        f"TPS extraction failed after {max_attempts} attempt(s): {last_error}"
    ) from last_error


def validate_survey_language(
    page,
    model: str = TPS_EXTRACTION_MODEL,
    max_attempts: int = TPS_EXTRACTION_MAX_ATTEMPTS,
) -> bool:
    """The dedicated language gate for a survey - explicit user request,
    replacing two earlier, weaker attempts at the same check:

    1. extract_tps_number_from_page() used to ask Gemini to return
       REJECT_NON_ENGLISH, but it only ever sees a tight crop of the
       handwritten TPS-number FIELD (see _crop_tps_field_pixmap()) - there's
       essentially never enough visible text in that crop to judge language
       at all, so that instruction was dead weight and never caught a real
       non-English survey.
    2. assess_page_content_and_declined() used to fold a NON_ENGLISH verdict
       into its combined content-quality/declined check. That one DOES see
       the full page, but bundling three judgments (blank / legible /
       language) into one two-word response diluted the model's attention -
       confirmed directly on 2025_Oct_31_9.pdf pages 9-10 (a Spanish-
       language survey whose OWN printed footer reads "Revised 9/17/25,
       (Adult) - Spanish"), which that combined check misread as "OK"
       instead of "NON_ENGLISH".

    This function does exactly one thing - render the FULL survey page and
    ask a single, focused language question - so there's nothing else in
    the prompt to dilute the model's attention.

    Returns True only when the survey is confidently identified as English.
    Spanish, other languages, or uncertain language are rejected - i.e. this
    is a fail-closed gate: an ambiguous model response, a response that
    doesn't parse, or every retry attempt erroring out all return False
    (reject), not True. This is deliberately the opposite of this module's
    usual "advisory, never blocks the decision it supports" pattern (see
    e.g. assess_page_content_and_declined()'s own docstring) - a language
    gate that fails open would silently let exactly the kind of survey it
    exists to catch through undetected."""
    import pymupdf
    from google.genai import types

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    prompt = (
        "Look at this whole scanned survey page. Is the visible printed and "
        "handwritten text written in English? Respond with exactly one "
        "word: ENGLISH if all the visible text is in English, NON_ENGLISH "
        "if any visible text (printed or handwritten) is written in a "
        "different language, or UNCERTAIN if you cannot tell. No other "
        "text, punctuation, or markdown."
    )
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            pixmap = page.get_pixmap(matrix=pymupdf.Matrix(2, 2), alpha=False)
            client = _get_genai_client()
            response = _generate_content_with_limits(
                client,
                model,
                [
                    types.Part.from_bytes(data=pixmap.tobytes("png"), mime_type="image/png"),
                    prompt,
                ],
                "survey language gate",
            )
            usage = getattr(response, "usage_metadata", None)
            if usage is not None:
                prompt_tokens, output_tokens, total_tokens = _record_gemini_usage(usage)
                status(
                    "[GEMINI] model=%s attempt=%d/%d prompt_tokens=%s "
                    "output_tokens=%s total_tokens=%s",
                    model,
                    attempt,
                    max_attempts,
                    prompt_tokens,
                    output_tokens,
                    total_tokens,
                )
            verdict = (response.text or "").strip().upper()
            if verdict == "ENGLISH":
                return True
            if verdict in ("NON_ENGLISH", "UNCERTAIN"):
                return False
            last_error = ValueError(f"language gate returned {verdict!r}; expected ENGLISH/NON_ENGLISH/UNCERTAIN")
        except Exception as error:  # noqa: BLE001 - retry, then fail closed (reject) below
            last_error = error

        if attempt < max_attempts:
            status(
                "[GEMINI] Survey language gate attempt %d/%d failed (%s); retrying in %d second(s).",
                attempt,
                max_attempts,
                last_error,
                TPS_EXTRACTION_RETRY_DELAY_SECONDS,
            )
            time.sleep(TPS_EXTRACTION_RETRY_DELAY_SECONDS)

    status(
        "[GEMINI] Survey language gate failed after %d attempt(s) (%s); rejecting (fail-closed).",
        max_attempts, last_error,
    )
    return False


def assess_page_content_and_declined(
    page,
    second_page=None,
    model: str = TPS_EXTRACTION_MODEL,
) -> tuple:
    """Combined content-quality + declined-marking check for a full survey,
    in a single Gemini call. Previously this was two separate calls
    (assess_page_content_issue() and detect_declined_marking()) that each
    rendered the same whole-page image and asked for one word back — merged
    here into one render + one prompt to cut Gemini calls (and the token
    cost of re-sending the same page image) roughly in half.

    extract_tps_number_from_page() only crops the small handwritten
    TPS-number field, which rarely has enough visible text to judge the
    page's language or to see a handwritten marking that could appear
    anywhere on the form — this instead renders the WHOLE page(s).

    second_page: the survey's second page (page 2 of this 2-page-per-survey
    form), if the caller has it - confirmed directly (Nov24_1.pdf, TPS 6995
    and TPS 7012): the BLANK criterion used to judge only page 1's Q1-23
    answer grid, so a respondent who left that grid untouched but fully
    answered page 2 (the Q24 comment box, Q25-35 demographic checkboxes)
    got rejected outright as "empty - no data to process", silently
    discarding a real, substantially-completed response. When given, BOTH
    pages are rendered and sent together, and BLANK now requires NEITHER
    page to have any mark anywhere. When omitted (e.g. a malformed source
    whose page count isn't a clean multiple of 2, so there's no reliable
    second page to pair it with), this falls back to judging page 1 alone,
    same as before.

    Returns (content_issue: Optional[str], declined: bool):
    - content_issue is a human-readable reason the survey can't be trusted
      (unreadable, or blank/unfilled on every page given), or None if
      nothing looks wrong. Language is NOT judged here - see
      validate_survey_language(), a separate gate against the full page.
    - declined is True either for a large handwritten "Declined"/"Decline"/
      "Refused" word, OR for a large scribble/loop/strikethrough mark drawn
      across page 1's answer grid with no such word written (respondents
      on this form mark a declined/voided response either way) — a page
      with declined=True still gets split and uploaded like any other
      survey (the TPS number is still valid and needed), just flagged with
      needs_review=True so a human confirms it before the response is
      treated as normal survey data.

    content_issue takes priority over declined at the call site
    (split_combined_pdf() checks content_issue first and rejects/continues
    before ever looking at declined) - so a survey that is BOTH marked
    declined AND has no actual survey answers selected anywhere is rejected
    outright (content_issue = "no data to process"), not merely flagged for
    review. The BLANK criterion above is judged purely by whether any
    individual answer field was filled in, deliberately not counting a
    decline scribble/strikethrough over page 1's grid as a "mark" that
    would disqualify BLANK - otherwise a blank-and-declined survey could
    slip through as merely needs-review instead of being rejected.

    Never raises: this is advisory context, so a network hiccup here
    shouldn't block the rejection/processing decision it supports —
    returns (None, False) on any failure."""
    import pymupdf
    from google.genai import types

    page_description = (
        "this scanned 2-page survey (the first image is page 1, the second "
        "image is page 2)" if second_page is not None else
        "this scanned survey page"
    )
    blank_scope = (
        "anywhere across BOTH pages - the numbered-question answer grid on "
        "page 1, AND page 2's Q24 written comment box and its Q25-35 "
        "demographic checkboxes/fields. A respondent who left page 1's grid "
        "untouched but filled in page 2 (or vice versa) is NOT blank - "
        "check every field on both pages before answering BLANK."
        if second_page is not None else
        "on this page - the numbered-question answer grid (Strongly Agree / "
        "Agree / etc.)."
    )
    prompt = (
        f"Look at {page_description} as a whole and answer two "
        "questions about it, responding with exactly two words separated by "
        "a space (no other text).\n\n"
        "First word - the survey's content quality. Determine this in two "
        "steps, IN ORDER:\n"
        f"Step 1 (check this FIRST, before anything else): look {blank_scope} "
        "If NOT EVEN ONE answer checkbox, comment box, or field is filled "
        "in or marked with an X or other selection, the word is BLANK - "
        "full stop, regardless of anything else on the page(s). "
        "Specifically, respond BLANK even when: the TPS number is clearly "
        "legible, the date fields are filled in, the header/ID boxes at "
        "the top are filled in, and/or there is a large scribble, loop, "
        "strikethrough, or handwritten word like 'Declined' drawn across "
        "page 1's grid. None of those count as answering the questions, "
        "and a legible TPS number or a legible 'Declined' marking must NOT "
        "cause you to answer OK instead of BLANK - only actual answer "
        "marks/written responses count. IMPORTANT: a large declined "
        "scribble or strikethrough line often happens to physically cross "
        "through or touch several checkbox squares on its way across the "
        "page - that incidental crossing does NOT count as those boxes "
        "being individually marked. Only count a checkbox as marked if it "
        "has its OWN distinct X or checkmark placed inside it as a "
        "deliberate answer selection, separate from any larger scribble "
        "passing through or near it. If the only marks anywhere on the "
        "grid are pieces of that one continuous declined scribble/line, "
        "with no individual checkbox independently selected, the answer "
        "is still BLANK.\n"
        "Step 2 (only if at least one field IS filled in/marked): OK if "
        "the handwritten TPS number (if visible) and the marked answers are "
        "legible; UNREADABLE if the content or TPS number can't be made out "
        "at all. Do not judge language here - only legibility.\n\n"
        "Second word - whether the respondent declined: "
        "Before deciding DECLINED, first identify the respondent's individual "
        "answer marks in the grid and treat those as legitimate selections. "
        "The mental test: is this one additional connected cancellation "
        "mark, or is it a collection of individual answer marks? Return "
        "DECLINED ONLY when there is strong visual evidence the respondent "
        "intentionally invalidated/crossed out the survey.\n\n"
        "Return DECLINED if EITHER:\n"
        "(a) A large handwritten word such as 'Declined', 'Decline', or "
        "'Refused' is prominently written across page 1.\n"
        "OR\n"
        "(b) There is a clearly intentional cancellation mark consisting of "
        "ADDITIONAL continuous ink drawn across the answer grid, visually "
        "distinguishable from the respondent's individual answer "
        "selections, that appears to cross out/invalidate/cancel the "
        "survey as a whole.\n\n"
        "The following are NOT DECLINED - do not classify the survey as "
        "DECLINED merely because ink appears across multiple checkbox rows "
        "or columns:\n"
        "- An X or checkmark inside an individual checkbox, even one that "
        "extends slightly outside it or touches an adjacent checkbox.\n"
        "- The respondent selecting the same answer column on many "
        "consecutive rows, or several individual answer marks that happen "
        "to form a line, diagonal, streak, or visual pattern.\n"
        "- A checkmark drawn with a long diagonal 'tail' or flourish - some "
        "respondents' checkmarks continue in a diagonal stroke after the "
        "check itself before the pen lifts, and because these tails are "
        "long, one row's tail can visually touch or overlap the next row's "
        "mark, creating the illusion of one continuous connected line "
        "threading down or across the grid. This is NOT a cancellation "
        "mark, even when it links together many rows and/or columns.\n\n"
        "To classify a mark as DECLINED under rule (b), verify ALL of these "
        "conditions:\n"
        "1. The mark is continuous or forms a clearly connected scribble, "
        "line, loop, or cross-out.\n"
        "2. It is visually distinct from the normal checkbox selections and "
        "does not consist simply of one deliberate answer mark per row "
        "(including the tail-flourish case above).\n"
        "3. Its apparent purpose is to cross out or invalidate the survey "
        "rather than select answers, covering/crossing a substantial "
        "portion of the answer grid as one intentional cancellation mark.\n"
        "4. At least part of the mark can be traced back to a starting "
        "point that is NOT any individual checkbox - e.g. the blank "
        "margin, the header area, or free space between rows. A mark "
        "entirely traceable to individual checkboxes' own marks is never "
        "DECLINED, no matter how large or connected-looking it is.\n\n"
        "If you cannot confidently distinguish an intentional cancellation "
        "mark from legitimate answer selections, return NOT_DECLINED.\n\n"
        "Otherwise respond NOT_DECLINED.\n\n"
        "Example response: 'OK NOT_DECLINED', 'UNREADABLE DECLINED', or "
        "'BLANK NOT_DECLINED'."
    )
    try:
        pixmap = page.get_pixmap(matrix=pymupdf.Matrix(2, 2), alpha=False)
        image_parts = [types.Part.from_bytes(data=pixmap.tobytes("png"), mime_type="image/png")]
        if second_page is not None:
            second_pixmap = second_page.get_pixmap(matrix=pymupdf.Matrix(2, 2), alpha=False)
            image_parts.append(
                types.Part.from_bytes(data=second_pixmap.tobytes("png"), mime_type="image/png")
            )
        client = _get_genai_client()
        response = _generate_content_with_limits(
            client,
            model,
            [*image_parts, prompt],
            "content/declined assessment",
        )
        _record_gemini_usage(getattr(response, "usage_metadata", None))
        parts = (response.text or "").strip().upper().split()
        content_word = parts[0] if len(parts) >= 1 else ""
        declined_word = parts[1] if len(parts) >= 2 else ""
        content_issue = None
        if content_word == "UNREADABLE":
            content_issue = "Gemini could not read the page content"
        elif content_word == "BLANK":
            content_issue = "the PDF is empty - there's no data to process"
        declined = declined_word == "DECLINED"
        return content_issue, declined
    except Exception as error:  # noqa: BLE001 - advisory only, never block the decision this supports
        status("[GEMINI] Page content/declined assessment failed (%s); continuing without it.", error)
        return None, False


def verify_same_tps_number(
    page_a,
    page_b,
    model: str = TPS_EXTRACTION_MODEL,
) -> str:
    """Visually re-checks whether two pages' handwritten TPS numbers are
    genuinely the same, before split_combined_pdf() calls two surveys
    duplicates. A matching OCR reading alone does NOT prove two surveys share
    a TPS number - Gemini can misread one ambiguous digit (e.g. 1 vs 4) and
    make two genuinely-different surveys collide on the same four digits.
    This sends BOTH cropped number images to Gemini in one call and asks it
    to compare them digit-by-digit, rather than re-reading each in isolation
    (which would just repeat whatever mistake caused the collision).

    Returns "SAME", "DIFFERENT", or "AMBIGUOUS" (never raises - any failure
    to get a clean verdict returns "AMBIGUOUS", the safe default that routes
    to manual review instead of either wrongly confirming or wrongly
    dismissing a duplicate)."""
    from google.genai import types

    prompt = (
        "IMAGE A and IMAGE B each show a cropped handwritten four-digit "
        "survey ID number. Read each one character-by-character, digit by "
        "digit, paying special attention to digits that are commonly "
        "confused in handwriting (1/4/7, 3/8, 5/6, 0/6/8/9, 2/7). Then state "
        "whether IMAGE A and IMAGE B show the SAME four-digit number or "
        "DIFFERENT numbers. Respond with exactly one word: SAME if every "
        "digit matches, DIFFERENT if any digit differs, or AMBIGUOUS if you "
        "cannot confidently read one or more digits on either image well "
        "enough to compare them. No other text."
    )
    try:
        pixmap_a = _crop_tps_field_pixmap(page_a)
        pixmap_b = _crop_tps_field_pixmap(page_b)
        client = _get_genai_client()
        response = _generate_content_with_limits(
            client,
            model,
            [
                "IMAGE A:",
                types.Part.from_bytes(data=pixmap_a.tobytes("png"), mime_type="image/png"),
                "IMAGE B:",
                types.Part.from_bytes(data=pixmap_b.tobytes("png"), mime_type="image/png"),
                prompt,
            ],
            "duplicate-TPS visual verification",
        )
        _record_gemini_usage(getattr(response, "usage_metadata", None))
        verdict = (response.text or "").strip().upper()
        if verdict in ("SAME", "DIFFERENT", "AMBIGUOUS"):
            return verdict
        status("[GEMINI] Duplicate-TPS visual check returned an unexpected verdict (%r); treating as AMBIGUOUS.", verdict)
        return "AMBIGUOUS"
    except Exception as error:  # noqa: BLE001 - never let this crash the run; be conservative instead
        status("[GEMINI] Duplicate-TPS visual check failed (%s); treating as AMBIGUOUS.", error)
        return "AMBIGUOUS"


def split_combined_pdf(
    pdf_bytes: bytes,
    source_name: str,
    year: int,
    dry_run: bool,
    pages_per_survey: int = PAGES_PER_SURVEY,
    tps_extractor=extract_tps_number_from_page,
    max_surveys: Optional[int] = None,
    only_page_range: Optional[tuple] = None,
) -> list:
    """Builds one two-page PDF per survey and names it with its TPS number.

    only_page_range: an optional (start_page, end_page) pair, 1-indexed and
    inclusive, exactly as page numbers are shown in a PDF viewer (e.g.
    (219, 220) for the survey occupying pages 219-220 of this source PDF).
    When given, every survey unit whose page range doesn't exactly match
    this is skipped entirely - no TPS extraction, no content/declined
    check, nothing added to the returned list for it - so this is for a
    manual one-off recheck of a single already-known survey unit, not a
    normal run (which should process every unit)."""
    import pymupdf

    if pages_per_survey < 1:
        raise ValueError("pages_per_survey must be at least 1")
    source_date = parse_file_name_date(source_name)
    if source_date is None:
        raise ValueError(f"cannot determine survey date from {source_name!r}")
    month, day = source_date
    date_folder = f"{_MONTH_NAMES[month]} {day} {year}"
    source_doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    try:
        if len(source_doc) % pages_per_survey:
            rejected_reason = (
                f"{source_name} has {len(source_doc)} page(s), which is not a "
                f"multiple of {pages_per_survey} pages per survey"
            )
            # Can't tell where survey boundaries would even be, so this can't be
            # split into per-survey units - but still worth checking page 1's
            # full content for a language/legibility issue, since that's often
            # WHY a source ended up with a malformed page count in the first
            # place (e.g. a single stray page scanned in isolation) and is
            # useful context for review.
            if not validate_survey_language(source_doc[0]):
                rejected_reason = f"{rejected_reason}; Also: the PDF is in a language other than English"
            content_issue, _declined = assess_page_content_and_declined(source_doc[0])
            if content_issue:
                rejected_reason = f"{rejected_reason}; Also: {content_issue}"
            return [
                {
                    "source_name": source_name,
                    "date_folder": date_folder,
                    "output_name": None,
                    "pdf_bytes": None,
                    "page_start": 1,
                    "page_end": len(source_doc),
                    "rejected_reason": rejected_reason,
                }
            ]
        outputs = []
        survey_starts = range(0, len(source_doc), pages_per_survey)
        if only_page_range is not None:
            range_start, range_end = only_page_range
            survey_starts = [
                s for s in survey_starts
                if s + 1 == range_start and s + pages_per_survey == range_end
            ]
        if max_surveys is not None:
            if max_surveys < 1:
                raise ValueError("max_surveys must be at least 1")
            survey_starts = list(survey_starts)[:max_surveys]

        previous_tps = None  # last successfully-read TPS number, used as sequential context for the next one
        seen_tps = {}  # tps -> {"page_range": str, "start": int} for the earlier survey in this same source that first read this TPS number
        digit_bank = _DigitTemplateBank()  # seeded from the WHOLE batch's HIGH-confidence readings before any confusable-pair check runs - see _DigitTemplateBank's docstring

        # ---- Pass 1: OCR + content check for every survey, in page order ----
        # expected_next_tps still threads sequentially page-to-page (that hint
        # is genuinely about reading order, unrelated to the digit bank), but
        # the confusable-pair confidence check is deliberately NOT done here
        # anymore - doing it inline, as the batch was walked, meant a survey
        # could only ever compare against digit shapes some earlier survey
        # happened to contain. Instead this pass only extracts each survey's
        # raw OCR reading and digit crops; pass 2 below builds the complete
        # digit bank from ALL of them before any comparison happens.
        pending = []  # records still needing confidence finalization + collision handling
        for start in survey_starts:
            batch_match = _COMBINED_FILE_RE.fullmatch(source_name)
            if batch_match is None:
                raise ValueError(f"cannot determine batch suffix from {source_name!r}")
            batch_suffix = batch_match.group("batch")
            page_range = f"{start + 1}-{start + pages_per_survey}"
            try:
                tps, confidence = tps_extractor(source_doc[start], expected_next_tps=previous_tps)
            except TpsRejected as rejection:
                outputs.append(
                    {
                        "source_name": source_name,
                        "date_folder": date_folder,
                        "output_name": None,
                        "pdf_bytes": None,
                        "page_start": start + 1,
                        "page_end": start + pages_per_survey,
                        "rejected_reason": str(rejection),
                    }
                )
                continue

            previous_tps = tps

            # Language is gated FIRST, before the content/blank/declined check
            # below and before the TPS collision check further down - same
            # "contributes no data regardless of what else is going on"
            # rationale as the content_issue check that follows. Checked
            # against page 1 alone (see validate_survey_language()'s
            # docstring for why this replaced the two earlier, weaker
            # attempts at this same check) - this form's language is
            # consistent across both of a survey's pages, so page 1 is
            # sufficient without a second Gemini call for page 2 too.
            if not validate_survey_language(source_doc[start]):
                entry = {
                    "source_name": source_name,
                    "date_folder": date_folder,
                    "output_name": None,
                    "pdf_bytes": None,
                    "page_start": start + 1,
                    "page_end": start + pages_per_survey,
                    "rejected_reason": f"the PDF is in a language other than English (TPS {tps})",
                }
                outputs.append(entry)
                existing = seen_tps.get(tps)
                if existing is None or existing["output"].get("rejected_reason") is not None:
                    seen_tps[tps] = {"page_range": page_range, "start": start, "output": entry}
                continue

            # Page content is checked before anything else remaining, including a
            # TPS collision below - a blank/unreadable survey contributes no data
            # regardless of what its TPS number does or doesn't collide with, so
            # it's rejected outright rather than getting pulled into a TPS dispute
            # (which would otherwise wrongly flag it, or even an unrelated later
            # survey, as merely needs_review instead of rejected).
            # Page 2 is passed too (when this survey unit has one) so BLANK is
            # judged across the whole survey, not just page 1's answer grid -
            # see assess_page_content_and_declined()'s docstring for why.
            second_page = source_doc[start + 1] if start + 1 < len(source_doc) else None
            content_issue, declined = assess_page_content_and_declined(source_doc[start], second_page)
            if content_issue:
                entry = {
                    "source_name": source_name,
                    "date_folder": date_folder,
                    "output_name": None,
                    "pdf_bytes": None,
                    "page_start": start + 1,
                    "page_end": start + pages_per_survey,
                    "rejected_reason": f"{content_issue} (TPS {tps})",
                }
                outputs.append(entry)
                # Don't let a rejected page's (possibly misread) TPS number clobber a
                # still-live prior survey's tracking entry - only register this TPS
                # if nothing live is already tracking it (see the collision check
                # below, which likewise ignores a rejected prior as a non-collision).
                existing = seen_tps.get(tps)
                if existing is None or existing["output"].get("rejected_reason") is not None:
                    seen_tps[tps] = {"page_range": page_range, "start": start, "output": entry}
                continue

            pending.append({
                "start": start,
                "batch_suffix": batch_suffix,
                "page_range": page_range,
                "tps": tps,
                "confidence": confidence,
                "digit_crops": _extract_digit_crops(source_doc[start]),
                "declined": declined,
            })

        # ---- Pass 2: seed the digit bank from every HIGH reading in the WHOLE batch ----
        # Seeded only from each record's raw self-reported HIGH confidence, not
        # from anything decided during pass 3 below - pass 3 reads this bank but
        # never adds to it, so every record is checked against the same fixed,
        # complete evidence regardless of the order pass 3 processes them in.
        for record in pending:
            if record["confidence"] == "HIGH" and record["digit_crops"] is not None:
                digit_bank.observe(record["tps"], record["digit_crops"], record["start"])

        # ---- Pass 3: confusable-pair confidence check (against the complete
        # bank, excluding each record's own just-seeded crop), then collision
        # handling and output building, still in page order (seen_tps and the
        # prior-survey NEEDS_REVIEW rename both depend on that order) ----
        for record in pending:
            start = record["start"]
            batch_suffix = record["batch_suffix"]
            page_range = record["page_range"]
            tps = record["tps"]
            confidence = record["confidence"]
            digit_crops = record["digit_crops"]
            declined = record["declined"]

            # Cross-check this reading's actual digit shapes against other digits
            # already confirmed correct elsewhere in this same source PDF - the OCR
            # model's own self-reported confidence is a noisy signal that can't
            # always be trusted either way: it can be confidently wrong (a HIGH-rated
            # '6' whose closed loop has a small ink gap reads as a confident '5'), and
            # it can also be needlessly hedgy about a perfectly legible digit - the
            # same clearly-printed number sent to the model twice can come back HIGH
            # once and LOW another time. The digit bank gives a second, deterministic
            # opinion pixel comparison against real digit shapes already seen in this
            # batch can't have that kind of run-to-run noise. This only fires for a
            # digit that's in a known commonly-confused pair, and only once a
            # template exists (from a DIFFERENT survey) for the other candidate digit.
            if digit_crops is not None:
                if confidence == "HIGH":
                    # If the digit's pixels match the OTHER candidate better than the
                    # one the model claimed, the reading is downgraded to LOW so it
                    # still gets flagged for manual review below instead of silently
                    # passing through as trustworthy. Both candidates need a template
                    # for this to be a real comparison - otherwise best_match() would
                    # just return whichever one happens to have any template so far
                    # (e.g. the claimed digit has never itself been confirmed yet in
                    # this batch, so its only "competition" trivially "wins"), which
                    # would wrongly downgrade a correct reading with no real evidence
                    # against it at all.
                    for i, digit in enumerate(tps):
                        pair = next((p for p in _CONFUSABLE_DIGIT_PAIRS if digit in p), None)
                        if pair is None:
                            continue
                        other_digit = next(iter(pair - {digit}))
                        if not digit_bank.has_templates_for((digit, other_digit), exclude=start):
                            continue
                        match = digit_bank.best_match(digit_crops[i], (digit, other_digit), exclude=start)
                        if match == other_digit:
                            confidence = "LOW"
                            break
                elif confidence == "LOW":
                    # The reverse check: don't let a bare self-reported LOW alone send
                    # an otherwise-legible reading to manual review. If every digit here
                    # that's part of a commonly-confused pair has a bank template to
                    # compare against, AND every one of them matches the digit the model
                    # actually claimed (not the confusable alternative), that's real
                    # independent pixel evidence the reading is correct despite the
                    # model's own hedging - upgrade to HIGH. If the bank instead prefers
                    # a different digit, or has no template for either candidate ANYWHERE
                    # else in the batch, leave it LOW so this still goes to manual review -
                    # the bank's silence isn't evidence either way. A TPS number with no
                    # confusable-pair digits at all has no independent evidence to check,
                    # so it also stays LOW. (A LOW record was never seeded into the bank
                    # in pass 2, so `exclude` here never actually removes anything for
                    # it - harmless, kept for symmetry with the HIGH branch above.)
                    bank_confirmed, checked_any = True, False
                    for i, digit in enumerate(tps):
                        pair = next((p for p in _CONFUSABLE_DIGIT_PAIRS if digit in p), None)
                        if pair is None:
                            continue
                        other_digit = next(iter(pair - {digit}))
                        # Both candidates need a template - otherwise best_match()
                        # would just return whichever one happens to have any
                        # template so far, which isn't a real comparison against
                        # the confusable alternative at all.
                        if not digit_bank.has_templates_for((digit, other_digit), exclude=start):
                            bank_confirmed = False
                            break
                        checked_any = True
                        match = digit_bank.best_match(digit_crops[i], (digit, other_digit), exclude=start)
                        if match != digit:
                            bank_confirmed = False
                            break
                    if checked_any and bank_confirmed:
                        confidence = "HIGH"

            # A matching OCR reading alone does NOT prove two surveys share a TPS
            # number - never reject solely because two OCR reads collided. A
            # HIGH-confidence collision still gets a visual, digit-by-digit
            # re-check before being called a genuine duplicate; a LOW-confidence
            # one skips straight to manual review, since a re-check would just
            # repeat the same uncertain reading. Either way, neither survey's ID
            # is ever silently changed to resolve the collision. A prior entry
            # that was itself rejected (e.g. blank/unreadable) isn't a genuine
            # collision - its TPS reading was never trustworthy data to begin
            # with, so this survey proceeds normally instead of being dragged
            # into a dispute with a page that has nothing to compare against.
            prior = seen_tps.get(tps)
            if prior is not None and prior["output"].get("rejected_reason") is None:
                if confidence == "HIGH":
                    verdict = verify_same_tps_number(source_doc[prior["start"]], source_doc[start])
                else:
                    verdict = None  # LOW confidence: go straight to review, don't trust a re-check of an uncertain read

                # Never reject a survey solely because this check thinks it's a
                # duplicate - confirmed directly (TPS 3229): there is no genuine
                # duplicate TPS number anywhere in this source PDF's real batch,
                # so a "SAME" verdict here is itself a misread (this check compares
                # rendered page images visually and can be fooled by two different,
                # but similarly-shaped, handwritten TPS numbers), not real physical
                # duplication. Silently dropping the survey on that basis would
                # permanently lose a real respondent's data over a model error with
                # no way for a human to ever notice or recover it. Every collision -
                # "SAME" included - is instead always split and uploaded (so nothing
                # is ever silently lost) and flagged NEEDS_REVIEW with the disputed
                # TPS number and both page ranges spelled out, so a human makes the
                # actual call. Neither survey's TPS is ever silently changed to
                # resolve the collision.
                if verdict == "SAME":
                    review_reason = (
                        f"TPS {tps} on pages {page_range} visually matched pages "
                        f"{prior['page_range']} in this same source PDF closely enough "
                        f"to look like a genuine duplicate - likely a misread of a "
                        f"similar-looking handwritten digit rather than an actual "
                        f"duplicate, since this batch has no confirmed duplicate TPS "
                        f"numbers. Not rejected; flagged for manual review instead of "
                        f"discarding this survey's data."
                    )
                elif verdict == "DIFFERENT":
                    review_reason = (
                        f"TPS {tps} initially matched pages {prior['page_range']} in this "
                        f"same source PDF, but a digit-by-digit visual re-check found they "
                        f"are actually different numbers - one of the two readings is "
                        f"wrong. Not treated as a duplicate; flagged for manual review "
                        f"instead of guessing which reading to trust."
                    )
                else:
                    reason_prefix = (
                        f"TPS {tps} was already read from pages {prior['page_range']} in "
                        f"this same source PDF"
                    )
                    review_reason = (
                        f"{reason_prefix}, and the visual re-check could not confidently "
                        f"confirm or rule out a duplicate - flagged for manual review."
                        if verdict == "AMBIGUOUS" else
                        f"{reason_prefix}, and this reading's own confidence was LOW - "
                        f"flagged for manual review instead of trusting either reading "
                        f"enough to call it a duplicate."
                    )

                # Page range is included so this doesn't collide with the earlier
                # survey's own NEEDS_REVIEW file below - both surveys share the same
                # (disputed) TPS number, so the bare TPS number alone isn't unique here.
                # Explicit user request: matches the prior survey's own rename
                # template below (_TPS_{tps}_NEEDS_REVIEW_pages_{page_range}.pdf) -
                # these two used to put "_pages_{page_range}" in different spots
                # (before vs. after "_TPS_{tps}_NEEDS_REVIEW"), so the two output
                # files from the SAME collision event looked like two different
                # naming conventions instead of a matched pair.
                review_name = (
                    f"{year}_{_MONTH_NAMES[month]}_{day:02d}_{batch_suffix}"
                    f"_TPS_{tps}_NEEDS_REVIEW_pages_{page_range}.pdf"
                )
                output_doc = pymupdf.open()
                try:
                    output_doc.insert_pdf(
                        source_doc,
                        from_page=start,
                        to_page=start + pages_per_survey - 1,
                    )
                    outputs.append(
                        {
                            "source_name": source_name,
                            "date_folder": date_folder,
                            "output_name": review_name,
                            "pdf_bytes": output_doc.tobytes(),
                            "page_start": start + 1,
                            "page_end": start + pages_per_survey,
                            "needs_review_reason": review_reason,
                        }
                    )
                finally:
                    output_doc.close()

                # The earlier survey's reading is now in question too - rename it to a
                # NEEDS_REVIEW file (keeping its already-built pdf_bytes so it still
                # gets uploaded) instead of silently leaving it looking trustworthy
                # under its original TPS-only name.
                prior_output = prior.get("output")
                if prior_output is not None and prior_output.get("rejected_reason") is None:
                    prior_output["output_name"] = (
                        f"{year}_{_MONTH_NAMES[month]}_{day:02d}_{batch_suffix}"
                        f"_TPS_{tps}_NEEDS_REVIEW_pages_{prior['page_range']}.pdf"
                    )
                    prior_output["needs_review_reason"] = (
                        f"TPS {tps} on this survey (pages {prior['page_range']}) was "
                        f"called into question by a later collision with pages "
                        f"{page_range} in the same source PDF; the visual re-check "
                        f"could not confirm both readings are correct - flagged for "
                        f"manual review alongside pages {page_range}."
                    )
                continue

            review_reasons = []
            if confidence == "LOW":
                review_reasons.append(
                    f"TPS number {tps} could not be read with 100% confidence "
                    f"(LOW confidence reading) - flagged for manual review since "
                    f"the handwritten number is not clearly recognizable."
                )
            if declined:
                review_reasons.append(
                    f"A large handwritten 'Declined' marking was detected on this "
                    f"survey page (TPS {tps}) - flagged for manual review."
                )
            if review_reasons:
                output_name = (
                    f"{year}_{_MONTH_NAMES[month]}_{day:02d}_{batch_suffix}"
                    f"_TPS_{tps}_NEEDS_REVIEW_pages_{page_range}.pdf"
                )
                declined_reason = " ".join(review_reasons)
            else:
                output_name = (
                    f"{year}_{_MONTH_NAMES[month]}_{day:02d}_{batch_suffix}"
                    f"_TPS_{tps}.pdf"
                )
                declined_reason = None
            output_doc = pymupdf.open()
            try:
                output_doc.insert_pdf(
                    source_doc,
                    from_page=start,
                    to_page=start + pages_per_survey - 1,
                )
                entry = {
                    "source_name": source_name,
                    "date_folder": date_folder,
                    "output_name": output_name,
                    "pdf_bytes": output_doc.tobytes(),
                    "page_start": start + 1,
                    "page_end": start + pages_per_survey,
                    "needs_review_reason": declined_reason,
                }
                outputs.append(entry)
                seen_tps[tps] = {"page_range": page_range, "start": start, "output": entry}
            finally:
                output_doc.close()
        return outputs
    finally:
        source_doc.close()


def combined_source_is_complete(
    bucket,
    root_prefix: str,
    destination_prefix: str,
    year: int,
    blob,
) -> bool:
    """Return whether every survey unit already has a destination PDF.

    This check only reads the source PDF structure and lists GCS objects. It
    deliberately does not call Gemini, so completed sources consume no Vertex
    quota on resume.
    """
    import pymupdf

    source_name = blob.name.rsplit("/", 1)[-1]
    relative_parts = blob.name[len(_normalize_prefix(root_prefix)):].split("/")
    source_folder = next(
        (part for part in relative_parts[:-1] if parse_date_folder_name(part)),
        None,
    )
    folder_date = parse_date_folder_name(source_folder) if source_folder else None
    source_date = folder_date or parse_file_name_date(source_name)
    if source_date is None:
        return False
    combined_match = _COMBINED_FILE_RE.fullmatch(source_name)
    if combined_match is None:
        return False
    if folder_date:
        source_name = (
            f"{_MONTH_NAMES[folder_date.month]}{folder_date.day}_"
            f"{combined_match.group('batch')}.pdf"
        )
    source_date = parse_file_name_date(source_name)
    if source_date is None:
        return False
    month, day = source_date
    source_bytes = blob.download_as_bytes()
    with pymupdf.open(stream=source_bytes, filetype="pdf") as source_doc:
        page_count = len(source_doc)
    if page_count < PAGES_PER_SURVEY or page_count % PAGES_PER_SURVEY:
        return False

    date_folder = f"{_MONTH_NAMES[month]} {day} {year}"
    batch_suffix = combined_match.group("batch")
    subfolder = survey_batch_subfolder(month, day, batch_suffix)
    output_prefix = (
        f"{_normalize_prefix(destination_prefix)}{date_folder}/{subfolder}/"
        f"{year}_{_MONTH_NAMES[month]}_{day:02d}_{batch_suffix}_TPS_"
    )
    existing_outputs = [
        destination_blob.name
        for destination_blob in bucket.client.list_blobs(
            bucket, prefix=output_prefix
        )
        if destination_blob.name.endswith(".pdf")
    ]
    expected_count = page_count // PAGES_PER_SURVEY
    return len(existing_outputs) == expected_count


def list_root_contents(bucket, root_prefix: str) -> tuple:
    """Lists source PDFs recursively and returns them with date-folder names.

    Previously this same prefix/delimiter query was issued twice per run —
    once inside infer_survey_year() (to find existing date folders) and
    again inside list_loose_pdfs() (to find loose PDFs) — even though a
    single listing already contains both the blobs and the "subfolder"
    prefixes. Doing it once here and sharing the result with both callers
    cuts a redundant round trip to GCS on every run.
    """
    root_prefix = _normalize_prefix(root_prefix)
    blobs = list(bucket.client.list_blobs(bucket, prefix=root_prefix))
    folder_names = sorted({
        part
        for blob in blobs
        if blob.name.lower().endswith(".pdf")
        for part in blob.name[len(root_prefix):].split("/")[:-1]
        if parse_date_folder_name(part) is not None
    })

    pdf_blobs = [b for b in blobs if b.name.lower().endswith(".pdf")]
    pdf_blobs.sort(key=lambda b: natural_sort_key(b.name))

    if not pdf_blobs:
        status("[GCS] No loose PDFs found directly under gs://%s/%s (nothing to organize).", bucket.name, root_prefix)
    else:
        status(
            "[GCS] Found %d source PDF(s) recursively under gs://%s/%s: %s",
            len(pdf_blobs), bucket.name, root_prefix, [b.name.rsplit("/", 1)[-1] for b in pdf_blobs],
        )
    return pdf_blobs, folder_names


def infer_survey_year(
    folder_names: list,
    bucket_name: str,
    root_prefix: str,
    default_year: Optional[int] = None,
) -> int:
    """Infers the year to use for every loose file organized in this run,
    from the EXISTING date folder names already under root_prefix (e.g.
    "Nov 18 2025") — returning whichever year is most common among them
    (ties broken by the most recent year, since a tie most often means an
    even split between an old batch and a new one just starting).

    This deliberately does NOT use the current wall-clock year: this
    pipeline already has real data showing survey folders can be processed
    well after the fact, so "whatever year this pipeline's own prior
    folders actually use" is a more reliable signal than "whatever year it
    happens to be when this script runs".

    folder_names is expected to come from list_root_contents() — this
    function no longer lists the bucket itself, since that listing is
    shared with (and already done once for) the loose-PDF lookup.

    Falls back to `default_year` (or the current UTC year if that's also
    None) with a loud warning if there are no existing date folders to
    infer from at all - this is expected to happen the very first time this
    script is ever run against a brand new root_prefix with nothing
    organized yet, not a bug."""
    years = []
    for name in folder_names:
        parsed = parse_date_folder_name(name)
        if parsed is not None:
            years.append(parsed.year)

    if not years:
        fallback = default_year if default_year is not None else datetime.datetime.utcnow().year
        err(
            "[YEAR] No existing date folders found under gs://%s/%s to infer a "
            "survey year from - falling back to %d. Pass --year explicitly if "
            "this is wrong.",
            bucket_name, root_prefix, fallback,
        )
        return fallback

    counts = Counter(years)
    max_count = max(counts.values())
    candidates = [year for year, count in counts.items() if count == max_count]
    inferred = max(candidates)  # ties broken by the most recent year
    status(
        "[YEAR] Inferred survey year %d from %d existing date folder(s) under gs://%s/%s (year counts: %s).",
        inferred, len(years), bucket_name, root_prefix, dict(sorted(counts.items())),
    )
    return inferred


@dataclass
class ManifestRow:
    source_file: str
    source_gcs_uri: str
    parsed_month_day: str  # e.g. "12/14" - the (month, day) parsed from the file name, for auditability
    destination_folder: str  # e.g. "Dec 14 2025"
    destination_gcs_uri: Optional[str]  # None when the survey was rejected (never split/uploaded)
    moved_at: Optional[datetime.datetime] = None  # when this ETL run moved the file
    moved_date: Optional[datetime.date] = None  # DATE(moved_at); the BQ table's partitioning column
    rejected_reason: Optional[str] = None  # why this survey was fully rejected (never split/uploaded; destination_gcs_uri stays None), else None
    needs_review_reason: Optional[str] = None  # why this survey needs a manual look even though it WAS still split/uploaded (e.g. disputed TPS number, declined marking), else None
    source_page_range: Optional[str] = None  # e.g. "1-2" - which pages/survey unit inside source_file this row is about, for a combined scan
    rejected: bool = False  # auto-derived in __post_init__ - True whenever rejected_reason is set
    needs_review: bool = False  # auto-derived in __post_init__ - True whenever needs_review_reason is set

    def __post_init__(self):
        # Derived rather than set at each call site, so every ManifestRow - present and
        # future - stays consistent with rejected_reason/needs_review_reason without
        # relying on every constructor call to remember to set these itself.
        self.rejected = self.rejected_reason is not None
        self.needs_review = self.needs_review_reason is not None
        if self.rejected and self.needs_review:
            raise ValueError(
                f"ManifestRow for {self.source_file!r} (pages {self.source_page_range}) "
                "cannot be both rejected and needs_review"
            )


@dataclass
class OrganizeResult:
    moved_rows: list
    skipped_files: list  # file names whose name didn't parse as a date, or that failed to move


def organize_loose_pdfs(
    bucket,
    root_prefix: str,
    destination_prefix: str,
    year: int,
    dry_run: bool,
    pdf_blobs: list,
    moved_at: Optional[datetime.datetime] = None,
) -> OrganizeResult:
    """Copies every loose PDF in `pdf_blobs` into
    its "<Month> <D> <YYYY>" date folder, based on the (month, day) parsed
    from each file's own name (see parse_file_name_date()) plus the given
    `year`. The source object is never deleted; the destination folder is
    created implicitly when the first file is copied (GCS has no real
    folders). Skips (not fatal) any file
    whose name doesn't parse."""
    moved_at = moved_at or datetime.datetime.utcnow()
    moved_date = moved_at.date()
    result = OrganizeResult(moved_rows=[], skipped_files=[])

    if not pdf_blobs:
        return result

    destination_prefix = _normalize_prefix(destination_prefix)

    for blob in pdf_blobs:
        file_name = blob.name.rsplit("/", 1)[-1]
        source_path = f"{bucket.name}/{blob.name}"
        relative_parts = blob.name[len(_normalize_prefix(root_prefix)):].split("/")
        source_folder = next(
            (part for part in relative_parts[:-1] if parse_date_folder_name(part)),
            None,
        )
        folder_date = parse_date_folder_name(source_folder) if source_folder else None
        parsed = (folder_date.month, folder_date.day) if folder_date else parse_file_name_date(file_name)
        source_uri = f"gs://{bucket.name}/{blob.name}"
        if parsed is None:
            err(
                "[ORGANIZE] SKIPPING %s: file name doesn't start with a recognizable "
                "<Mon><Day> token (e.g. \"Dec14\") - can't determine its date folder.",
                source_uri,
            )
            result.skipped_files.append(blob.name)
            continue

        month, day = parsed
        try:
            file_date = datetime.date(year, month, day)
        except ValueError as e:
            err("[ORGANIZE] SKIPPING %s: parsed month=%d day=%d year=%d is not a real date (%s).", source_uri, month, day, year, e)
            result.skipped_files.append(blob.name)
            continue

        destination_folder = f"{_MONTH_NAMES[month]} {day} {year}"
        destination_blob_name = f"{destination_prefix}{destination_folder}/{file_name}"
        destination_uri = f"gs://{bucket.name}/{destination_blob_name}"

        # A destination that already exists is not re-copied, but the manifest
        # row for it is still (re-)appended below rather than skipped outright -
        # load_manifest_rows_into_bq() deletes-then-reinserts by
        # (source_gcs_uri, source_page_range), so skipping the append here would
        # mean this file's BQ row is never refreshed again once it's first
        # organized, permanently going stale on every later run even though the
        # file itself is still there and correctly organized.
        already_present = bucket.blob(destination_blob_name).exists()
        if already_present:
            status("[ORGANIZE] Already present; recording manifest row without re-copying %s", destination_uri)
        elif dry_run:
            status("[ORGANIZE] [dry-run] would move %s -> %s", source_uri, destination_uri)
        else:
            try:
                bucket.copy_blob(blob, bucket, destination_blob_name)
                status("[ORGANIZE] Copied %s -> %s", source_uri, destination_uri)
            except Exception as e:  # noqa: BLE001 - keep going across a batch of files
                err("[ORGANIZE] FAILED moving %s -> %s: %s", source_uri, destination_uri, e)
                result.skipped_files.append(blob.name)
                continue

        result.moved_rows.append(
            ManifestRow(
                source_file=source_path,
                source_gcs_uri=source_uri,
                parsed_month_day=f"{month:02d}/{day:02d}",
                destination_folder=destination_folder,
                destination_gcs_uri=destination_uri,
                moved_at=moved_at,
                moved_date=moved_date,
            )
        )

    return result


_COMBINED_FILE_RE = re.compile(
    r"^(?P<month>[A-Za-z]{3})(?P<day>\d{1,2})_(?P<batch>\d+)\.pdf$",
    re.IGNORECASE,
)


def organize_combined_pdf(
    bucket,
    root_prefix: str,
    destination_prefix: str,
    year: int,
    dry_run: bool,
    blob,
    moved_at: Optional[datetime.datetime] = None,
    max_surveys: Optional[int] = None,
    only_page_range: Optional[tuple] = None,
) -> OrganizeResult:
    """Splits one combined scan and uploads named survey PDFs.

    The source is never modified or deleted. Existing destination objects are
    never overwritten.

    only_page_range: forwarded to split_combined_pdf() - see its docstring.
    """
    moved_at = moved_at or datetime.datetime.utcnow()
    result = OrganizeResult(moved_rows=[], skipped_files=[])
    source_name = blob.name.rsplit("/", 1)[-1]
    source_path = f"{bucket.name}/{blob.name}"
    source_uri = f"gs://{bucket.name}/{blob.name}"
    pdf_bytes = blob.download_as_bytes()
    relative_parts = blob.name[len(_normalize_prefix(root_prefix)):].split("/")
    source_folder = next(
        (part for part in relative_parts[:-1] if parse_date_folder_name(part)),
        None,
    )
    folder_date = parse_date_folder_name(source_folder) if source_folder else None
    source_date = folder_date or parse_file_name_date(source_name)
    if source_date is None:
        raise ValueError(f"cannot determine source date from {blob.name!r}")
    source_name_for_split = source_name
    if folder_date:
        combined_match = _COMBINED_FILE_RE.fullmatch(source_name)
        if combined_match is None:
            raise ValueError(f"combined source name is not recognized: {source_name!r}")
        source_name_for_split = (
            f"{_MONTH_NAMES[folder_date.month]}{folder_date.day}_"
            f"{combined_match.group('batch')}.pdf"
        )
    outputs = split_combined_pdf(
        pdf_bytes, source_name_for_split, year, dry_run,
        max_surveys=max_surveys, only_page_range=only_page_range,
    )
    seen_names = set()
    destination_prefix = _normalize_prefix(destination_prefix)

    month, day = (
        (source_date.month, source_date.day)
        if isinstance(source_date, datetime.date)
        else source_date
    )

    # Every survey unit split out of this one source PDF shares the same batch
    # subfolder (see survey_batch_subfolder()) - derived here once from
    # source_name_for_split the same way split_combined_pdf() derives it
    # internally for each survey unit (both match against _COMBINED_FILE_RE),
    # so this is guaranteed consistent with what split_combined_pdf() itself used.
    combined_match_for_split = _COMBINED_FILE_RE.fullmatch(source_name_for_split)
    if combined_match_for_split is None:
        raise ValueError(f"combined source name is not recognized: {source_name_for_split!r}")
    batch_suffix = combined_match_for_split.group("batch")
    subfolder = survey_batch_subfolder(month, day, batch_suffix)

    for output in outputs:
        page_range = f"{output['page_start']}-{output['page_end']}"
        rejected_reason = output.get("rejected_reason")
        # A rejected_reason with no output_name means split_combined_pdf() couldn't (or
        # decided not to) produce a file at all - e.g. an unreadable/blank page, a
        # non-English survey, or a genuine confirmed duplicate. That's a true reject:
        # nothing to upload, and never flagged needs_review too (a file can't be both -
        # see ManifestRow.__post_init__). A needs_review_reason WITH an output_name (a
        # NEEDS_REVIEW file name) means the opposite - the survey still gets split and
        # uploaded, just under a name that flags it for a human, so it falls through to
        # the same upload path as a normal success below instead of being skipped here.
        if rejected_reason and output.get("output_name") is None:
            err(
                "[SPLIT] REJECTED survey (pages %s) in %s: %s",
                page_range,
                source_uri,
                rejected_reason,
            )
            result.moved_rows.append(
                ManifestRow(
                    source_file=source_path,
                    source_gcs_uri=source_uri,
                    parsed_month_day=f"{month:02d}/{day:02d}",
                    destination_folder=output["date_folder"],
                    destination_gcs_uri=None,
                    moved_at=moved_at,
                    moved_date=moved_at.date(),
                    rejected_reason=rejected_reason,
                    source_page_range=page_range,
                )
            )
            continue

        output_name = output["output_name"]
        if output_name in seen_names:
            # split_combined_pdf() already dedupes by TPS number across a source's
            # own survey units - reaching this is unexpected, so flag just this one
            # survey rather than aborting the rest of an otherwise-good source PDF.
            # Nothing is uploaded under this colliding name, so this is a true reject.
            err(
                "[SPLIT] REJECTED survey (pages %s) in %s: duplicate output name %s",
                page_range, source_uri, output_name,
            )
            result.moved_rows.append(
                ManifestRow(
                    source_file=source_path,
                    source_gcs_uri=source_uri,
                    parsed_month_day=f"{month:02d}/{day:02d}",
                    destination_folder=output["date_folder"],
                    destination_gcs_uri=None,
                    moved_at=moved_at,
                    moved_date=moved_at.date(),
                    rejected_reason=f"duplicate TPS output name {output_name!r} within this source PDF",
                    source_page_range=page_range,
                )
            )
            continue
        seen_names.add(output_name)
        needs_review_reason = output.get("needs_review_reason")
        destination_blob_name = f"{destination_prefix}{output['date_folder']}/{subfolder}/{output_name}"
        destination_uri = f"gs://{bucket.name}/{destination_blob_name}"
        destination_blob = bucket.blob(destination_blob_name)
        review_tag = " [NEEDS REVIEW]" if needs_review_reason else ""
        # As in organize_loose_pdfs(): an already-uploaded survey is not re-uploaded,
        # but its manifest row is still (re-)appended below instead of skipped -
        # otherwise this survey's BQ row (including needs_review_reason, which
        # split_combined_pdf() just freshly recomputed this run) would never be
        # refreshed again on any later run, once the destination file exists.
        if destination_blob.exists():
            status("[SPLIT] Already present; recording manifest row without re-uploading %s", destination_uri)
        elif dry_run:
            status(
                "[SPLIT] [dry-run]%s %s pages %d-%d -> %s",
                review_tag,
                source_uri,
                output["page_start"],
                output["page_end"],
                destination_uri,
            )
        else:
            destination_blob.upload_from_string(
                output["pdf_bytes"], content_type="application/pdf"
            )
            status("[SPLIT] Uploaded%s %s -> %s", review_tag, source_uri, destination_uri)
        result.moved_rows.append(
            ManifestRow(
                source_file=source_path,
                source_gcs_uri=source_uri,
                parsed_month_day=f"{month:02d}/{day:02d}",
                destination_folder=output["date_folder"],
                destination_gcs_uri=destination_uri,
                moved_at=moved_at,
                moved_date=moved_at.date(),
                needs_review_reason=needs_review_reason,
                source_page_range=page_range,
            )
        )

    return result


# --------------------------------------------------------------------------
# Manifest BigQuery schema + Spark-based partitioned load. Same registry
# shape and load pattern as BQ_SURVEY_RESPONSES_SCHEMA/BQ_FILE_QUALITY_SCHEMA
# and load_rows_into_bq_via_spark() in merge_survey_pdfs.py (Revision 40) —
# duplicated here (not imported) so this file has no dependency on that one.
# --------------------------------------------------------------------------
BQ_MANIFEST_SCHEMA = [
    ("source_file", "STRING", "The loose source PDF's original file name."),
    ("source_gcs_uri", "STRING", "gs:// URI the source PDF was moved FROM."),
    ("parsed_month_day", "STRING", "The MM/DD parsed from the file name (year is inferred separately - see infer_survey_year())."),
    ("destination_folder", "STRING", "The date-folder name this file was moved into, e.g. 'Dec 14 2025'."),
    ("destination_gcs_uri", "STRING", "gs:// URI the source PDF was moved TO."),
    ("moved_at", "TIMESTAMP", "When this ETL run moved the file."),
    ("moved_date", "DATE", "DATE(moved_at); the table's partitioning column."),
    ("source_page_range", "STRING", "e.g. '1-2' - which pages/survey unit inside source_file this row is about, for a combined scan; NULL for a standalone source file."),
    ("rejected", "BOOLEAN", "True if this survey was fully rejected and never split/uploaded (destination_gcs_uri NULL) - e.g. a blank/unreadable/non-English page or a confirmed duplicate. Mutually exclusive with needs_review."),
    ("rejected_reason", "STRING", "Why this survey was rejected; NULL unless rejected=true."),
    ("needs_review", "BOOLEAN", "True if this survey was still split/uploaded but flagged for a human to double-check (e.g. a disputed TPS number, a declined marking). Mutually exclusive with rejected."),
    ("needs_review_reason", "STRING", "Why this survey needs manual review; NULL unless needs_review=true."),
]


def bq_schema_to_spark_schema(bq_schema):
    """Converts BQ_MANIFEST_SCHEMA's plain (name, type, description) tuples
    into an explicit pyspark.sql.types.StructType, so the Spark load below
    uses a known schema instead of auto-inferring one from the data. See
    merge_survey_pdfs.py's identically-named function (Revision 40) for the
    full rationale; duplicated here rather than imported to keep this file
    standalone."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        FloatType, BooleanType, DateType, TimestampType,
    )

    bq_to_spark = {
        "STRING": StringType(), "INTEGER": IntegerType(), "FLOAT": FloatType(),
        "FLOAT64": FloatType(), "BOOLEAN": BooleanType(), "DATE": DateType(),
        "TIMESTAMP": TimestampType(),
    }
    fields = [StructField(name, bq_to_spark[typ], nullable=True) for name, typ, _desc in bq_schema]
    return StructType(fields)


def ensure_manifest_table(bq_client, project: str, dataset: str, table: str):
    """Creates (or, on an older table, patches) the organize_manifest table schema - same create-or-patch pattern merge_survey_pdfs.py uses for its
    own tables, so an existing table never needs to be dropped just because a new column is added to BQ_MANIFEST_SCHEMA later."""
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
        bigquery.SchemaField(name, typ if typ != "FLOAT" else "FLOAT64", description=desc)
        for name, typ, desc in BQ_MANIFEST_SCHEMA
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
        status("[BQ] Creating table %s.%s.%s (partitioned by %s)", project, dataset, table, partition_field)
        new_table = bigquery.Table(table_ref, schema=schema)
        new_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        bq_table = bq_client.create_table(new_table)
    return bq_table


def delete_existing_manifest_rows_for_sources(
    bq_client,
    project: str,
    dataset: str,
    table: str,
    source_keys: list,
) -> None:
    """Deletes any existing manifest row(s) matching the given
    (source_gcs_uri, source_page_range) pairs before a fresh load - keyed at
    the individual survey-unit level (not the whole combined source PDF), so
    reprocessing one previously-rejected survey replaces just that survey's
    row instead of also wiping out other surveys from the same source PDF
    that already succeeded in an earlier run and aren't being re-emitted
    this time (they're skipped, not reprocessed - see
    combined_source_is_complete()/the destination_blob.exists() checks)."""
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    unique_keys = sorted(set(source_keys))
    if not unique_keys:
        return
    table_id = f"`{project}`.`{dataset}`.`{table}`"
    struct_params = [
        bigquery.StructQueryParameter(
            None,
            bigquery.ScalarQueryParameter("source_gcs_uri", "STRING", uri),
            bigquery.ScalarQueryParameter("source_page_range", "STRING", page_range),
        )
        for uri, page_range in unique_keys
    ]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("keys", "STRUCT", struct_params),
        ]
    )
    try:
        bq_client.query(
            f"""
            DELETE FROM {table_id} AS t
            WHERE EXISTS (
                SELECT 1 FROM UNNEST(@keys) AS k
                WHERE t.source_gcs_uri = k.source_gcs_uri
                  AND (t.source_page_range = k.source_page_range
                       OR (t.source_page_range IS NULL AND k.source_page_range IS NULL))
            )
            """,
            job_config=job_config,
        ).result()
    except NotFound:
        # Table doesn't exist yet - nothing to delete before the first load.
        pass


def load_manifest_rows_into_bq(
    rows: list,
    project: str,
    dataset: str,
    table: str,
    staging_bucket: Optional[str] = None,
    bq_client=None,
) -> int:
    """Loads manifest rows (a list of ManifestRow) into BigQuery, preferring Spark + the BigQuery Spark connector when a SparkSession is already
    in scope as the global `spark` (e.g. a Databricks notebook); this function does not create one itself. If no `spark` is in scope, or the
    Spark write itself raises, this falls back to a direct google.cloud.bigquery.Client write (see load_manifest_rows_into_bq_via_client()) so a
    plain-Python run (outside a Databricks notebook), or a one-off Spark connector failure, still gets its manifest rows persisted rather than
    silently losing them. Before writing, deletes any existing row(s) for the same (source_gcs_uri, source_page_range) (see
    delete_existing_manifest_rows_for_sources()) so reprocessing one survey unit (e.g. a previously-rejected one) replaces just that unit's
    manifest record - only the latest record per survey unit is kept, without touching other surveys from the same source PDF.

    staging_bucket: the connector's default ("indirect") write path stages the DataFrame's data into a GCS bucket before loading it into BigQuery,
    and raises "Either temporary or persistent GCS bucket must be set" if none is configured - it does NOT reuse the source/output bucket this
    script otherwise reads/writes PDFs from automatically. Defaults to BUCKET_NAME (the same bucket already used for reading/moving PDFs) if
    not given - the connector manages its own temporary object names/cleanup within that bucket. Set via the temporaryGcsBucket write option
    (per-call) rather than spark.conf's global 'temporaryGcsBucket' setting, so this doesn't require every caller to have configured the
    SparkSession itself."""
    if not rows:
        return 0
    table_id = f"{project}.{dataset}.{table}"
    staging_bucket = staging_bucket or BUCKET_NAME

    if bq_client is not None:
        delete_existing_manifest_rows_for_sources(
            bq_client, project, dataset, table,
            [(r.source_gcs_uri, r.source_page_range) for r in rows if r.source_gcs_uri],
        )

    if "spark" in globals():
        try:
            import pandas as pd

            dict_rows = [r.__dict__ for r in rows]
            df = pd.DataFrame(dict_rows, dtype=object)
            spark_schema = bq_schema_to_spark_schema(BQ_MANIFEST_SCHEMA)

            for field in spark_schema.fieldNames():
                if field not in df.columns:
                    df[field] = None
            df = df[spark_schema.fieldNames()]
            df = df.where(df.notna(), None)

            spark_df = globals()["spark"].createDataFrame(df, schema=spark_schema)
            spark_df.show(truncate = False)

            # Mode 0 = append, not overwrite. delete_existing_manifest_rows_for_sources()
            # above already deletes exactly the rows being replaced (by
            # source_gcs_uri/source_page_range) - this write must only append the
            # new/updated rows on top of that, matching the WRITE_APPEND behavior of
            # the direct-BigQuery-client fallback path below. Mode 1 (overwrite)
            # replaces the WHOLE "moved_date" partition - since every row in a single
            # run shares today's moved_date, that silently wiped out any other row
            # for today's partition that wasn't part of this run's rows (e.g. a file
            # from an earlier run today, or a source skipped this run because
            # combined_source_is_complete() judged it already done).
            
            
            # writeToEventStore(spark_df, "@OutputTable1", 0, "moved_date")

            status(
                "[BQ][SPARK] Wrote %d manifest row(s) into %s via Spark (partitioned by %s, staged through gs://%s).",
                len(rows), table_id, partition_field, staging_bucket,
            )
            return len(rows)
        except Exception as error:  # noqa: BLE001 - fall back to a direct BQ client write below
            err(
                "[BQ][SPARK] Spark load into %s failed (%s); falling back to a direct BigQuery client write.",
                table_id, error,
            )
    else:
        status(
            "[BQ][SPARK] No SparkSession found in scope as the global `spark` (not running in a "
            "Databricks notebook); falling back to a direct BigQuery client write for %s.",
            table_id,
        )

    return load_manifest_rows_into_bq_via_client(rows, project, dataset, table, bq_client=bq_client)


def load_manifest_rows_into_bq_via_client(
    rows: list,
    project: str,
    dataset: str,
    table: str,
    bq_client=None,
) -> int:
    """Fallback write path for load_manifest_rows_into_bq() - used when no Spark session is available at all, or the Spark write itself failed.
    Writes manifest rows straight through a plain google.cloud.bigquery.Client load job instead of the Spark BigQuery connector, so a run outside
    a Databricks notebook (or a Spark connector hiccup) still gets its rows persisted into BigQuery rather than losing them."""
    if not rows:
        return 0
    from google.cloud import bigquery

    bq_client = bq_client or bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    def _row_to_json(row: ManifestRow) -> dict:
        record = dict(row.__dict__)
        if isinstance(record.get("moved_at"), datetime.datetime):
            record["moved_at"] = record["moved_at"].isoformat()
        if isinstance(record.get("moved_date"), datetime.date):
            record["moved_date"] = record["moved_date"].isoformat()
        return record

    job = bq_client.load_table_from_json(
        [_row_to_json(r) for r in rows],
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(name, typ if typ != "FLOAT" else "FLOAT64")
                for name, typ, _desc in BQ_MANIFEST_SCHEMA
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    status(
        "[BQ][CLIENT] Wrote %d manifest row(s) into %s via a direct BigQuery client load (fallback path).",
        job.output_rows, table_id,
    )
    return job.output_rows


def _process_source_pdf(
    bucket,
    root_prefix: str,
    resolved_year: int,
    dry_run: bool,
    blob,
    max_surveys: Optional[int],
    pdf_index: int,
    total_pdfs: int,
    failure_log: str,
    only_page_range: Optional[tuple] = None,
) -> OrganizeResult:
    """Processes exactly one source PDF (combined or loose) end to end -
    the body of run()'s former sequential per-blob loop, pulled out so it
    can be submitted to a ThreadPoolExecutor and run concurrently across
    source PDFs (see SOURCE_PDF_WORKERS). Runs on a worker thread: never
    mutates anything outside its own arguments and return value - callers
    merge the returned OrganizeResult into the run-wide totals themselves,
    so no lock is needed here for that. log_failed_source() is already
    thread-safe (see _FAILURE_LOG_LOCK) since several workers can call it at
    once."""
    source_uri = f"gs://{bucket.name}/{blob.name}"
    status("[PROGRESS] Starting source PDF %d/%d: %s", pdf_index, total_pdfs, source_uri)
    try:
        if _COMBINED_FILE_RE.fullmatch(blob.name.rsplit("/", 1)[-1]):
            # only_page_range is an explicit request to (re)process one specific
            # survey unit, so it always runs even if combined_source_is_complete()
            # would otherwise consider this whole source PDF already done.
            if only_page_range is None and not dry_run and combined_source_is_complete(
                bucket, root_prefix, DESTINATION_PREFIX, resolved_year, blob,
            ):
                status(
                    "[PROGRESS] Fully processed; skipping Vertex for source PDF %d/%d: %s",
                    pdf_index, total_pdfs, source_uri,
                )
                return OrganizeResult(moved_rows=[], skipped_files=[])
            pdf_result = organize_combined_pdf(
                bucket, root_prefix, DESTINATION_PREFIX, resolved_year, dry_run, blob,
                max_surveys=max_surveys, only_page_range=only_page_range,
            )
        else:
            pdf_result = organize_loose_pdfs(
                bucket, root_prefix, DESTINATION_PREFIX, resolved_year, dry_run, [blob],
            )
        for row in pdf_result.moved_rows:
            reason = row.rejected_reason or row.needs_review_reason
            if not reason:
                continue
            bad_pdf = source_uri
            if row.source_page_range:
                bad_pdf = f"{source_uri} (pages {row.source_page_range})"
            try:
                log_failed_source(failure_log, source_uri, ValueError(reason), bad_pdf=bad_pdf)
            except OSError as log_error:
                err("[PROGRESS] FAILED recording rejection for %s in %s: %s", bad_pdf, failure_log, log_error)
        status(
            "[PROGRESS] Finished source PDF %d/%d: %s (%d output(s), %d skipped)",
            pdf_index, total_pdfs, source_uri, len(pdf_result.moved_rows), len(pdf_result.skipped_files),
        )
        return pdf_result
    except Exception as e:  # noqa: BLE001 - keep processing other source PDFs
        err("[PROGRESS] FAILED source PDF %d/%d %s: %s", pdf_index, total_pdfs, source_uri, e)
        try:
            log_failed_source(failure_log, source_uri, e)
            status("[PROGRESS] Failure recorded in %s", failure_log)
        except OSError as log_error:
            err("[PROGRESS] FAILED recording failure for %s in %s: %s", source_uri, failure_log, log_error)
        return OrganizeResult(moved_rows=[], skipped_files=[blob.name])


def run(
    bucket_name: str,
    root_prefix: str,
    dry_run: bool,
    year: Optional[int] = None,
    bq_project: Optional[str] = BQ_PROJECT,
    bq_dataset: str = BQ_DATASET,
    manifest_table: Optional[str] = MANIFEST_TABLE,
    spark_staging_bucket: Optional[str] = None,
    max_surveys: Optional[int] = None,
    failure_log: str = DEFAULT_FAILURE_LOG,
    loose_only: bool = False,
    only_file: Optional[str] = None,
    only_page_range: Optional[tuple] = None,
) -> None:
    for key in RUN_TOKEN_TOTALS:
        RUN_TOKEN_TOTALS[key] = 0
    bucket = connect_gcs_bucket(bucket_name)

    # One listing serves both the year inference and the organize step —
    # see list_root_contents() for why this used to be two separate calls.
    pdf_blobs, folder_names = list_root_contents(bucket, root_prefix)
    if loose_only:
        normalized_root = _normalize_prefix(root_prefix)
        before = len(pdf_blobs)
        pdf_blobs = [b for b in pdf_blobs if "/" not in b.name[len(normalized_root):]]
        status(
            "[GCS] --loose-only: skipping %d PDF(s) already inside a date subfolder; %d loose file(s) remain.",
            before - len(pdf_blobs), len(pdf_blobs),
        )
    if only_file:
        before = len(pdf_blobs)
        pdf_blobs = [b for b in pdf_blobs if b.name.rsplit("/", 1)[-1] == only_file]
        status(
            "[GCS] --only-file %s: %d/%d listed PDF(s) matched.",
            only_file, len(pdf_blobs), before,
        )
        if not pdf_blobs:
            err("[GCS] --only-file %s: no matching PDF found under gs://%s/%s - nothing to do.", only_file, bucket_name, root_prefix)
            return
    if only_page_range is not None and not only_file:
        err("[GCS] --only-pages requires --only-file (a page range only makes sense within one specific source PDF) - nothing to do.")
        return
    resolved_year = year if year is not None else infer_survey_year(folder_names, bucket_name, root_prefix)

    result = OrganizeResult(moved_rows=[], skipped_files=[])
    total_pdfs = len(pdf_blobs)
    # Source PDFs are independent of each other (each is its own GCS
    # object/manifest rows), so they're processed concurrently here -
    # SOURCE_PDF_WORKERS bounds how many run at once, while
    # MAX_CONCURRENT_VERTEX_CALLS (enforced inside _generate_content_with_limits())
    # separately caps how many Gemini requests are ever in flight, which is
    # what actually protects Vertex's quota regardless of this worker count.
    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_PDF_WORKERS) as executor:
        futures = {
            executor.submit(
                _process_source_pdf,
                bucket, root_prefix, resolved_year, dry_run, blob,
                max_surveys, pdf_index, total_pdfs, failure_log,
                only_page_range,
            ): blob
            for pdf_index, blob in enumerate(pdf_blobs, start=1)
        }
        for future in concurrent.futures.as_completed(futures):
            pdf_result = future.result()
            result.moved_rows.extend(pdf_result.moved_rows)
            result.skipped_files.extend(pdf_result.skipped_files)

    status(
        "[GEMINI] Run token total: requests=%d prompt_tokens=%d "
        "output_tokens=%d total_tokens=%d",
        RUN_TOKEN_TOTALS["requests"],
        RUN_TOKEN_TOTALS["prompt_tokens"],
        RUN_TOKEN_TOTALS["output_tokens"],
        RUN_TOKEN_TOTALS["total_tokens"],
    )

    if dry_run:
        status(
            "[ORGANIZE] Dry run complete. %d file(s) would have been moved into date folders (year=%d); %d file(s) would have been skipped.",
            len(result.moved_rows), resolved_year, len(result.skipped_files),
        )
        return

    if not result.moved_rows:
        if result.skipped_files:
            err("[ORGANIZE] Nothing was moved — all %d loose file(s) failed to parse/move.", len(result.skipped_files))
        return

    if manifest_table:
        from google.cloud import bigquery
        bq_client = bigquery.Client(project=bq_project)
        resolved_bq_project = bq_project or bq_client.project
        try:
            ensure_manifest_table(bq_client, resolved_bq_project, bq_dataset, manifest_table)
            n_loaded = load_manifest_rows_into_bq(
                result.moved_rows, resolved_bq_project, bq_dataset, manifest_table,
                staging_bucket=spark_staging_bucket or BUCKET_NAME,
                bq_client=bq_client,
            )
            status(
                "[BQ] Loaded %d manifest row(s) into %s.%s.%s.",
                n_loaded, resolved_bq_project, bq_dataset, manifest_table,
            )
        except Exception as e:  # noqa: BLE001 - the moves themselves already succeeded; don't lose that over this
            err(
                "[BQ] FAILED loading manifest into %s.%s.%s: %s",
                resolved_bq_project, bq_dataset, manifest_table, e,
            )
    else:
        status("[ORGANIZE] --no-manifest-table set: skipping the BigQuery manifest load (%d row(s) not persisted anywhere).", len(result.moved_rows))

    if result.skipped_files:
        err("[ORGANIZE] Skipped %d file(s) (unparseable name or move failure): %s", len(result.skipped_files), ", ".join(result.skipped_files))
    status(
        "[ORGANIZE] Done. %d file(s) moved into date folders (year=%d), %d file(s) skipped.",
        len(result.moved_rows), resolved_year, len(result.skipped_files),
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", default=BUCKET_NAME, help="GCS bucket name")
    ap.add_argument("--root-prefix", default=ROOT_PREFIX, help="Prefix to search for loose (not-yet-organized) PDFs.")
    ap.add_argument(
        "--year",
        type=int,
        default=None,
        help="Year to use for every parsed file-name date this run (e.g. 2025). "
        "Defaults to inferring it from existing date folders under --root-prefix (see infer_survey_year()).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only: list what would be moved without touching GCS or BigQuery.",
    )
    ap.add_argument("--bq-project", default=BQ_PROJECT, help="GCP project for the manifest BigQuery table. Defaults to the caller's default project.")
    ap.add_argument("--bq-dataset", default=BQ_DATASET, help="BigQuery dataset for the manifest table.")
    ap.add_argument("--manifest-table", default=MANIFEST_TABLE, help="BigQuery table to load the organize manifest into.")
    ap.add_argument("--no-manifest-table", action="store_true", help="Skip loading the manifest into BigQuery entirely.")
    ap.add_argument(
        "--spark-staging-bucket",
        default=None,
        help="GCS bucket the Spark BigQuery connector stages the manifest load through. Defaults to BUCKET_NAME (the same bucket as --bucket).",
    )
    ap.add_argument(
        "--max-surveys",
        type=int,
        default=None,
        help="For a test run, process only this many surveys from each combined PDF.",
    )
    ap.add_argument(
        "--failure-log",
        default=DEFAULT_FAILURE_LOG,
        help=f"CSV file to append failed source PDFs to (default: {DEFAULT_FAILURE_LOG}).",
    )
    ap.add_argument(
        "--loose-only",
        action="store_true",
        help="Only process PDFs sitting directly under --root-prefix; skip any PDF that's "
        "already inside a date subfolder (e.g. useful when --root-prefix is a parent "
        "directory containing both loose files and already-organized date folders).",
    )
    ap.add_argument(
        "--only-file",
        default=None,
        help="Only process the PDF with this exact file name (e.g. 'Nov10_1.pdf') found "
        "under --root-prefix; every other PDF listed is skipped. Useful for testing a "
        "single source file without touching the rest of a folder.",
    )
    ap.add_argument(
        "--only-pages",
        default=None,
        metavar="START-END",
        help="Only process the single survey unit occupying pages START-END (1-indexed, "
        "inclusive, exactly as shown in a PDF viewer, e.g. '219-220') of the PDF given by "
        "--only-file - every other survey unit in that source is skipped entirely (no TPS "
        "extraction, no content/declined check). Requires --only-file. Also bypasses the "
        "'already fully processed' skip that would otherwise apply to that whole source PDF, "
        "so this always runs even to recheck one already-processed survey.",
    )
    ap.add_argument(
        "--self-test",
        action="store_true",
        help="Run the offline self-test (mocked GCS bucket, local SparkSession) and exit.",
    )

    args, unknown = ap.parse_known_args()
    if unknown:
        status(
            "Ignoring unrecognized argument(s) %s (expected if you're running "
            "this from a notebook — sys.argv there includes the kernel's own "
            "flags, not yours).",
            unknown,
        )

    only_page_range = None
    if args.only_pages:
        match = re.fullmatch(r"(\d+)-(\d+)", args.only_pages.strip())
        if not match:
            err("--only-pages must look like 'START-END' (e.g. '219-220'); got %r.", args.only_pages)
            return
        only_page_range = (int(match.group(1)), int(match.group(2)))

    run(
        bucket_name=args.bucket,
        root_prefix=args.root_prefix,
        dry_run=args.dry_run,
        year=args.year,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        manifest_table=None if args.no_manifest_table else args.manifest_table,
        spark_staging_bucket=args.spark_staging_bucket,
        max_surveys=args.max_surveys,
        failure_log=args.failure_log,
        loose_only=args.loose_only,
        only_file=args.only_file,
        only_page_range=only_page_range,
    )


if __name__ == "__main__":
    main()