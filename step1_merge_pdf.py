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
import csv
import datetime
import logging
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# --------------------------------------------------------------------------
# Configuration — same defaults as merge_survey_pdfs.py had for its merge side, kept here so this file runs standalone with no shared config import.
# --------------------------------------------------------------------------
BUCKET_NAME = "tps_survey"
ROOT_PREFIX = "TPS_Scanned_2025/"
DESTINATION_PREFIX = "TPS_Scanned_2025_Reorgnized/"
PAGES_PER_SURVEY = 2
TPS_EXTRACTION_MODEL = "gemini-3.1-flash-lite"
TPS_EXTRACTION_LOCATION = "global"
TPS_EXTRACTION_PROJECT = "gcp-sapchoda-dev"
TPS_EXTRACTION_MAX_ATTEMPTS = 3
TPS_EXTRACTION_RETRY_DELAY_SECONDS = 2
BQ_PROJECT = "gcp-sapchoda-dev"
BQ_DATASET = "ladph_tps"
MANIFEST_TABLE = "pdf_manifest_list"
DEFAULT_FAILURE_LOG = "step1_failed_sources.csv"
partition_field = "moved_date"  # must be an actual column in BQ_MANIFEST_SCHEMA
RUN_TOKEN_TOTALS = {
    "prompt_tokens": 0,
    "output_tokens": 0,
    "total_tokens": 0,
    "requests": 0,
}

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
    the failure is at the whole-source-file level."""
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


class TpsRejected(Exception):
    """Raised when a survey's page 1 can't be trusted for a TPS number —
    either the handwritten number (or the page content around it) is
    illegible, or the page is written in a language other than English.
    This is expected to happen for a genuinely bad or foreign-language
    scan, not a bug, and is handled by recording a rejected manifest row
    (with a reject_reason) rather than failing the whole source PDF."""


def extract_tps_number_from_page(
    page,
    model: str = TPS_EXTRACTION_MODEL,
    max_attempts: int = TPS_EXTRACTION_MAX_ATTEMPTS,
    expected_next_tps: Optional[str] = None,
) -> str:
    """Reads the handwritten TPS number printed at the upper-right of page 1.

    The source scans are image-only and the number is handwritten, so PDF text
    extraction and filename parsing cannot recover it. Gemini receives only a
    tightly cropped image of that field and must return exactly four digits.

    expected_next_tps: the previous survey's TPS number in this same combined
    scan (surveys are digitized in sequence, so TPS numbers normally
    increment one-by-one within a batch). Passed along as a hint in the
    prompt to help disambiguate easily-confused handwritten digits (e.g. a 1
    misread as a 4, or a 6 misread as an 8) that previously caused two
    different surveys to resolve to the same TPS number and made the whole
    combined source PDF look like it had a duplicate. None for the first
    survey in a batch (no prior number to anchor against).
    """
    import pymupdf
    from google import genai
    from google.genai import types

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    page_rect = page.rect
    crop = pymupdf.Rect(
        page_rect.x0 + page_rect.width * 0.80,
        page_rect.y0 + page_rect.height * 0.08,
        page_rect.x1 - page_rect.width * 0.01,
        page_rect.y0 + page_rect.height * 0.24,
    )
    pixmap = page.get_pixmap(matrix=pymupdf.Matrix(3, 3), clip=crop, alpha=False)
    prompt_parts = [
        "Read the handwritten four-digit TPS number in this cropped survey "
        "field. Return only the four digits, with no spaces, punctuation, "
        "explanation, or markdown."
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
        "instead. If any text visible in the cropped image is written in a "
        "language other than English, do not guess — return exactly "
        "REJECT_NON_ENGLISH instead."
    )
    prompt = " ".join(prompt_parts)
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            client = genai.Client(
                vertexai=True,
                project=TPS_EXTRACTION_PROJECT,
                location=TPS_EXTRACTION_LOCATION,
            )
            response = client.models.generate_content(
                model=model,
                contents=[
                    types.Part.from_bytes(data=pixmap.tobytes("png"), mime_type="image/png"),
                    prompt,
                ],
            )
            usage = getattr(response, "usage_metadata", None)
            if usage is not None:
                prompt_tokens = getattr(usage, "prompt_token_count", None) or 0
                output_tokens = getattr(usage, "candidates_token_count", None) or 0
                total_tokens = getattr(usage, "total_token_count", None) or 0
                RUN_TOKEN_TOTALS["prompt_tokens"] += prompt_tokens
                RUN_TOKEN_TOTALS["output_tokens"] += output_tokens
                RUN_TOKEN_TOTALS["total_tokens"] += total_tokens
                RUN_TOKEN_TOTALS["requests"] += 1
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
            if normalized == "REJECT_NON_ENGLISH":
                raise TpsRejected("the PDF is in a language other than English")
            if normalized == "REJECT_UNREADABLE":
                raise TpsRejected(
                    "Gemini could not read the TPS number or page content "
                    f"(returned {raw_text!r})"
                )
            value = re.sub(r"\D", "", raw_text)
            if re.fullmatch(r"\d{4}", value):
                return value
            last_error = TpsRejected(
                f"TPS extraction returned {raw_text!r}; expected exactly four digits"
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


def assess_page_content_issue(
    page,
    model: str = TPS_EXTRACTION_MODEL,
) -> Optional[str]:
    """Best-effort content check for a page that can't be paired into a normal
    two-page survey unit (see split_combined_pdf()'s page-count-mismatch
    branch). extract_tps_number_from_page() only crops the small handwritten
    TPS-number field, which rarely has enough visible text to judge the
    page's language — this instead renders the WHOLE page so a non-English
    survey (or a page whose content is unreadable for any other reason) gets
    flagged even when a normal two-page pairing was never possible. Returns a
    human-readable reason, or None if nothing looks wrong. Never raises: this
    is advisory context appended to an already-decided rejection, so a
    network hiccup here shouldn't block reporting that rejection."""
    import pymupdf
    from google import genai
    from google.genai import types

    prompt = (
        "Look at this scanned survey page as a whole. Respond with exactly "
        "one word: OK if the page's text is in English and its handwritten "
        "TPS number (if visible) is legible; NON_ENGLISH if any visible "
        "text on the page is written in a language other than English; or "
        "UNREADABLE if the page content or TPS number can't be made out at "
        "all. No other text in your response."
    )
    try:
        pixmap = page.get_pixmap(matrix=pymupdf.Matrix(2, 2), alpha=False)
        client = genai.Client(
            vertexai=True,
            project=TPS_EXTRACTION_PROJECT,
            location=TPS_EXTRACTION_LOCATION,
        )
        response = client.models.generate_content(
            model=model,
            contents=[
                types.Part.from_bytes(data=pixmap.tobytes("png"), mime_type="image/png"),
                prompt,
            ],
        )
        usage = getattr(response, "usage_metadata", None)
        if usage is not None:
            RUN_TOKEN_TOTALS["prompt_tokens"] += getattr(usage, "prompt_token_count", None) or 0
            RUN_TOKEN_TOTALS["output_tokens"] += getattr(usage, "candidates_token_count", None) or 0
            RUN_TOKEN_TOTALS["total_tokens"] += getattr(usage, "total_token_count", None) or 0
            RUN_TOKEN_TOTALS["requests"] += 1
        verdict = (response.text or "").strip().upper()
        if verdict == "NON_ENGLISH":
            return "the PDF is in a language other than English"
        if verdict == "UNREADABLE":
            return "Gemini could not read the page content"
        return None
    except Exception as error:  # noqa: BLE001 - advisory only, never block the rejection this supports
        status("[GEMINI] Page content assessment failed (%s); continuing without it.", error)
        return None


def split_combined_pdf(
    pdf_bytes: bytes,
    source_name: str,
    year: int,
    dry_run: bool,
    pages_per_survey: int = PAGES_PER_SURVEY,
    tps_extractor=extract_tps_number_from_page,
    max_surveys: Optional[int] = None,
) -> list:
    """Builds one two-page PDF per survey and names it with its TPS number."""
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
            reject_reason = (
                f"{source_name} has {len(source_doc)} page(s), which is not a "
                f"multiple of {pages_per_survey} pages per survey"
            )
            # Can't tell where survey boundaries would even be, so this can't be
            # split into per-survey units - but still worth checking page 1's
            # full content for a language/legibility issue, since that's often
            # WHY a source ended up with a malformed page count in the first
            # place (e.g. a single stray page scanned in isolation) and is
            # useful context for review.
            content_issue = assess_page_content_issue(source_doc[0])
            if content_issue:
                reject_reason = f"{reject_reason}; Also: {content_issue}"
            return [
                {
                    "source_name": source_name,
                    "date_folder": date_folder,
                    "output_name": None,
                    "pdf_bytes": None,
                    "page_start": 1,
                    "page_end": len(source_doc),
                    "reject_reason": reject_reason,
                }
            ]
        outputs = []
        survey_starts = range(0, len(source_doc), pages_per_survey)
        if max_surveys is not None:
            if max_surveys < 1:
                raise ValueError("max_surveys must be at least 1")
            survey_starts = list(survey_starts)[:max_surveys]

        previous_tps = None  # last successfully-read TPS number, used as sequential context for the next one
        seen_tps = {}  # tps -> "start-end" page range already used by an earlier survey in this same source

        for start in survey_starts:
            batch_match = _COMBINED_FILE_RE.fullmatch(source_name)
            if batch_match is None:
                raise ValueError(f"cannot determine batch suffix from {source_name!r}")
            batch_suffix = batch_match.group("batch")
            page_range = f"{start + 1}-{start + pages_per_survey}"
            try:
                tps = tps_extractor(source_doc[start], expected_next_tps=previous_tps)
            except TpsRejected as rejection:
                outputs.append(
                    {
                        "source_name": source_name,
                        "date_folder": date_folder,
                        "output_name": None,
                        "pdf_bytes": None,
                        "page_start": start + 1,
                        "page_end": start + pages_per_survey,
                        "reject_reason": str(rejection),
                    }
                )
                continue
            # A real batch really can repeat a TPS number by mistake, but far more often
            # a duplicate here means one of the two reads is a misread of a similar-looking
            # digit (e.g. 1 vs 4) - flag just this survey for manual review instead of
            # aborting the whole combined source PDF over it.
            if tps in seen_tps:
                outputs.append(
                    {
                        "source_name": source_name,
                        "date_folder": date_folder,
                        "output_name": None,
                        "pdf_bytes": None,
                        "page_start": start + 1,
                        "page_end": start + pages_per_survey,
                        "reject_reason": (
                            f"TPS {tps} was already read from pages {seen_tps[tps]} in "
                            f"this same source PDF - likely a misread of a similar-"
                            f"looking handwritten digit rather than a genuine duplicate; "
                            f"flagged for manual review instead of being uploaded."
                        ),
                    }
                )
                previous_tps = tps
                continue
            previous_tps = tps

            content_issue = assess_page_content_issue(source_doc[start])
            if content_issue:
                seen_tps[tps] = page_range
                outputs.append(
                    {
                        "source_name": source_name,
                        "date_folder": date_folder,
                        "output_name": None,
                        "pdf_bytes": None,
                        "page_start": start + 1,
                        "page_end": start + pages_per_survey,
                        "reject_reason": f"{content_issue} (TPS {tps})",
                    }
                )
                continue

            seen_tps[tps] = page_range
            output_name = (
                f"{year}_{_MONTH_NAMES[month]}_{day:02d}_{batch_suffix}"
                f"_TPS_{tps}.pdf"
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
                        "output_name": output_name,
                        "pdf_bytes": output_doc.tobytes(),
                        "page_start": start + 1,
                        "page_end": start + pages_per_survey,
                        "reject_reason": None,
                    }
                )
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
    output_prefix = (
        f"{_normalize_prefix(destination_prefix)}{date_folder}/"
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
    reject_reason: Optional[str] = None  # why this survey was rejected (TPS/content unreadable, non-English), else None
    source_page_range: Optional[str] = None  # e.g. "1-2" - which pages/survey unit inside source_file this row is about, for a combined scan


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

        if bucket.blob(destination_blob_name).exists():
            status("[ORGANIZE] Already present; skipping %s", destination_uri)
            continue
        if dry_run:
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
                source_file=file_name,
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
) -> OrganizeResult:
    """Splits one combined scan and uploads named survey PDFs.

    The source is never modified or deleted. Existing destination objects are
    never overwritten.
    """
    moved_at = moved_at or datetime.datetime.utcnow()
    result = OrganizeResult(moved_rows=[], skipped_files=[])
    source_name = blob.name.rsplit("/", 1)[-1]
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
        pdf_bytes, source_name_for_split, year, dry_run, max_surveys=max_surveys
    )
    seen_names = set()
    destination_prefix = _normalize_prefix(destination_prefix)

    month, day = (
        (source_date.month, source_date.day)
        if isinstance(source_date, datetime.date)
        else source_date
    )

    for output in outputs:
        page_range = f"{output['page_start']}-{output['page_end']}"
        reject_reason = output.get("reject_reason")
        if reject_reason:
            err(
                "[SPLIT] REJECTED survey (pages %s) in %s: %s",
                page_range,
                source_uri,
                reject_reason,
            )
            result.moved_rows.append(
                ManifestRow(
                    source_file=source_name,
                    source_gcs_uri=source_uri,
                    parsed_month_day=f"{month:02d}/{day:02d}",
                    destination_folder=output["date_folder"],
                    destination_gcs_uri=None,
                    moved_at=moved_at,
                    moved_date=moved_at.date(),
                    reject_reason=reject_reason,
                    source_page_range=page_range,
                )
            )
            continue

        output_name = output["output_name"]
        if output_name in seen_names:
            # split_combined_pdf() already dedupes by TPS number across a source's
            # own survey units - reaching this is unexpected, so flag just this one
            # survey rather than aborting the rest of an otherwise-good source PDF.
            err(
                "[SPLIT] REJECTED survey (pages %s) in %s: duplicate output name %s",
                page_range, source_uri, output_name,
            )
            result.moved_rows.append(
                ManifestRow(
                    source_file=source_name,
                    source_gcs_uri=source_uri,
                    parsed_month_day=f"{month:02d}/{day:02d}",
                    destination_folder=output["date_folder"],
                    destination_gcs_uri=None,
                    moved_at=moved_at,
                    moved_date=moved_at.date(),
                    reject_reason=f"duplicate TPS output name {output_name!r} within this source PDF",
                    source_page_range=page_range,
                )
            )
            continue
        seen_names.add(output_name)
        destination_blob_name = f"{destination_prefix}{output['date_folder']}/{output_name}"
        destination_uri = f"gs://{bucket.name}/{destination_blob_name}"
        destination_blob = bucket.blob(destination_blob_name)
        if destination_blob.exists():
            status("[SPLIT] Already present; skipping %s", destination_uri)
            continue
        if dry_run:
            status(
                "[SPLIT] [dry-run] %s pages %d-%d -> %s",
                source_uri,
                output["page_start"],
                output["page_end"],
                destination_uri,
            )
        else:
            destination_blob.upload_from_string(
                output["pdf_bytes"], content_type="application/pdf"
            )
            status("[SPLIT] Uploaded %s -> %s", source_uri, destination_uri)
        result.moved_rows.append(
            ManifestRow(
                source_file=output_name,
                source_gcs_uri=source_uri,
                parsed_month_day=f"{month:02d}/{day:02d}",
                destination_folder=output["date_folder"],
                destination_gcs_uri=destination_uri,
                moved_at=moved_at,
                moved_date=moved_at.date(),
                reject_reason=None,
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
    ("reject_reason", "STRING", "Why this survey was rejected (unreadable TPS/content, or non-English PDF); NULL if not rejected."),
    ("source_page_range", "STRING", "e.g. '1-2' - which pages/survey unit inside source_file this row is about, for a combined scan; NULL for a standalone source file."),
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

            writeToEventStore(spark_df, "@OutputTable1", 1, "moved_date")

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
) -> None:
    for key in RUN_TOKEN_TOTALS:
        RUN_TOKEN_TOTALS[key] = 0
    bucket = connect_gcs_bucket(bucket_name)

    # One listing serves both the year inference and the organize step —
    # see list_root_contents() for why this used to be two separate calls.
    pdf_blobs, folder_names = list_root_contents(bucket, root_prefix)
    resolved_year = year if year is not None else infer_survey_year(folder_names, bucket_name, root_prefix)

    result = OrganizeResult(moved_rows=[], skipped_files=[])
    total_pdfs = len(pdf_blobs)
    for pdf_index, blob in enumerate(pdf_blobs, start=1):
        source_uri = f"gs://{bucket.name}/{blob.name}"
        status(
            "[PROGRESS] Starting source PDF %d/%d: %s",
            pdf_index,
            total_pdfs,
            source_uri,
        )
        try:
            if _COMBINED_FILE_RE.fullmatch(blob.name.rsplit("/", 1)[-1]):
                if not dry_run and combined_source_is_complete(
                    bucket,
                    root_prefix,
                    DESTINATION_PREFIX,
                    resolved_year,
                    blob,
                ):
                    status(
                        "[PROGRESS] Fully processed; skipping Vertex for source PDF %d/%d: %s",
                        pdf_index,
                        total_pdfs,
                        source_uri,
                    )
                    continue
                pdf_result = organize_combined_pdf(
                    bucket,
                    root_prefix,
                    DESTINATION_PREFIX,
                    resolved_year,
                    dry_run,
                    blob,
                    max_surveys=max_surveys,
                )
            else:
                pdf_result = organize_loose_pdfs(
                    bucket,
                    root_prefix,
                    DESTINATION_PREFIX,
                    resolved_year,
                    dry_run,
                    [blob],
                )
            result.moved_rows.extend(pdf_result.moved_rows)
            result.skipped_files.extend(pdf_result.skipped_files)
            for row in pdf_result.moved_rows:
                if not row.reject_reason:
                    continue
                bad_pdf = source_uri
                if row.source_page_range:
                    bad_pdf = f"{source_uri} (pages {row.source_page_range})"
                try:
                    log_failed_source(failure_log, source_uri, ValueError(row.reject_reason), bad_pdf=bad_pdf)
                except OSError as log_error:
                    err(
                        "[PROGRESS] FAILED recording rejection for %s in %s: %s",
                        bad_pdf,
                        failure_log,
                        log_error,
                    )
            status(
                "[PROGRESS] Finished source PDF %d/%d: %s (%d output(s), %d skipped)",
                pdf_index,
                total_pdfs,
                source_uri,
                len(pdf_result.moved_rows),
                len(pdf_result.skipped_files),
            )
        except Exception as e:  # noqa: BLE001 - keep processing other source PDFs
            err("[PROGRESS] FAILED source PDF %d/%d %s: %s", pdf_index, total_pdfs, source_uri, e)
            try:
                log_failed_source(failure_log, source_uri, e)
                status("[PROGRESS] Failure recorded in %s", failure_log)
            except OSError as log_error:
                err(
                    "[PROGRESS] FAILED recording failure for %s in %s: %s",
                    source_uri,
                    failure_log,
                    log_error,
                )
            result.skipped_files.append(blob.name)

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
    )


if __name__ == "__main__":
    main()