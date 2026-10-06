#!/usr/bin/env python3
"""Accuracy check: step1_evaluate_pdf.py's output vs. the original source
PDFs and gcp-sapchoda-dev.ladph_tps.pdf_manifest_list.

For every pdf_manifest_list row, checks:

  1. COMPLETENESS - for each distinct source PDF, its actual page count
     (pages_per_survey units) is compared against the manifest rows found
     for it: flags any missing survey unit (a page range with no manifest
     row at all) and any manifest row whose destination_gcs_uri no longer
     exists in GCS (orphaned/stale row).
  2. FOLDER PLACEMENT - destination_gcs_uri's path is checked against
     output_category (declined -> .../Declined/<date>/<subfolder>/...,
     rejected -> .../Rejected/..., normal -> plain .../<date>/<subfolder>/...)
     and against destination_folder.
  3. CATEGORY / REASON ACCURACY - re-renders the source PDF's actual pages
     for this row and re-runs the exact same gates step1 used
     (assess_page_content_and_declined() + validate_survey_language(), both
     imported from step1_evaluate_pdf.py) to independently re-derive what
     the category/reason SHOULD be, then compares that against what's
     stored in the manifest row.

Every row with any problem is written to a CSV (default:
step1_accuracy_issues.csv). Never modifies GCS or BigQuery - read-only.
"""
import argparse
import csv
import datetime
import sys
import threading
import concurrent.futures

import pymupdf
from google.cloud import bigquery, storage

import step1_evaluate_pdf as s1

PROJECT = "gcp-sapchoda-dev"
DATASET = "ladph_tps"
MANIFEST_TABLE = "pdf_manifest_list"
BUCKET_NAME = s1.BUCKET_NAME
DESTINATION_PREFIX = s1._normalize_prefix(s1.DESTINATION_PREFIX)
WORKERS = 6
# Confirmed directly: a GCS call with no timeout can hang indefinitely on a
# dropped connection (observed as stale CLOSE_WAIT sockets) instead of ever
# raising - same failure mode fixed for the Gemini client in
# step1_evaluate_pdf.py's _get_genai_client(). A bounded timeout turns a
# silent hang into a raised exception the caller already handles.
GCS_TIMEOUT_SECONDS = 60

_print_lock = threading.Lock()


def log(msg, *args):
    with _print_lock:
        print(f"[{datetime.datetime.utcnow().isoformat()}Z] {msg % args}", flush=True)


def fetch_manifest_rows(bq_client) -> list:
    query = f"""
        SELECT source_file, source_gcs_uri, destination_folder,
               destination_gcs_uri, source_page_range, output_category,
               rejected, rejected_reason, needs_review, needs_review_reason,
               moved_at
        FROM `{PROJECT}.{DATASET}.{MANIFEST_TABLE}`
    """
    return [dict(row) for row in bq_client.query(query).result()]


def expected_category_subdir(output_category: str) -> str:
    return {"normal": "", "declined": "Declined/", "rejected": "Rejected/"}.get(output_category, "")


def check_folder_placement(row: dict) -> list:
    """Returns a list of problem strings (empty if none)."""
    problems = []
    dest = row.get("destination_gcs_uri")
    category = row.get("output_category")
    if dest is None:
        # Whole-source failure rows legitimately have no destination file.
        return problems
    if not dest.startswith(f"gs://{BUCKET_NAME}/"):
        problems.append(f"destination_gcs_uri not under expected bucket: {dest!r}")
        return problems
    rel_path = dest[len(f"gs://{BUCKET_NAME}/"):]
    if not rel_path.startswith(DESTINATION_PREFIX):
        problems.append(f"destination_gcs_uri not under DESTINATION_PREFIX: {dest!r}")
        return problems
    rel_path = rel_path[len(DESTINATION_PREFIX):]
    expected_subdir = expected_category_subdir(category)
    if expected_subdir:
        if not rel_path.startswith(expected_subdir):
            problems.append(
                f"output_category={category!r} but destination path doesn't start with "
                f"{expected_subdir!r}: {rel_path!r}"
            )
        else:
            rel_path = rel_path[len(expected_subdir):]
    else:
        for bad_prefix in ("Declined/", "Rejected/"):
            if rel_path.startswith(bad_prefix):
                problems.append(
                    f"output_category={category!r} but destination path starts with "
                    f"{bad_prefix!r}: {rel_path!r}"
                )
    destination_folder = row.get("destination_folder")
    if destination_folder and not rel_path.startswith(f"{destination_folder}/"):
        problems.append(
            f"destination_folder={destination_folder!r} doesn't match destination_gcs_uri's "
            f"own date-folder segment: {rel_path!r}"
        )
    return problems


def check_destination_exists(bucket, row: dict) -> list:
    dest = row.get("destination_gcs_uri")
    if dest is None:
        return []
    prefix = f"gs://{BUCKET_NAME}/"
    if not dest.startswith(prefix):
        return []
    blob_name = dest[len(prefix):]
    if not bucket.blob(blob_name).exists(timeout=GCS_TIMEOUT_SECONDS):
        return [f"destination file does not exist in GCS: {dest}"]
    return []


def check_category_and_reason(source_doc, row: dict) -> list:
    """Re-runs step1's own gates against the real source pages for this row
    and compares the result to what's stored."""
    problems = []
    page_range = row.get("source_page_range")
    if not page_range:
        return problems
    try:
        start_str, end_str = page_range.split("-")
        start_page, end_page = int(start_str) - 1, int(end_str) - 1
    except ValueError:
        return [f"unparseable source_page_range: {page_range!r}"]
    if start_page < 0 or end_page >= len(source_doc) or start_page > end_page:
        return [f"source_page_range {page_range!r} out of bounds for a {len(source_doc)}-page source"]

    first_page = source_doc[start_page]
    second_page = source_doc[start_page + 1] if start_page + 1 <= end_page else None

    language_note = None
    if not s1.validate_survey_language(first_page):
        language_note = "the survey appears to be written in a language other than English"

    content_issue, decline_marking = s1.assess_page_content_and_declined(first_page, second_page)

    if decline_marking == "WORD":
        expected_category = "declined"
    elif decline_marking == "SCRIBBLE":
        expected_category = "declined"
    elif content_issue:
        expected_category = "rejected"
    else:
        expected_category = "normal"

    stored_category = row.get("output_category")
    if expected_category != stored_category:
        problems.append(
            f"re-check says output_category should be {expected_category!r} "
            f"(content_issue={content_issue!r}, decline_marking={decline_marking!r}, "
            f"language_note={language_note!r}), but manifest has {stored_category!r}"
        )

    stored_rejected = bool(row.get("rejected"))
    expected_rejected = expected_category == "rejected"
    if stored_rejected != expected_rejected:
        problems.append(
            f"re-check says rejected should be {expected_rejected} but manifest has {stored_rejected}"
        )

    stored_review = bool(row.get("needs_review"))
    expected_review = expected_category == "declined" or (
        expected_category == "normal" and bool(language_note)
    )
    if stored_review != expected_review:
        problems.append(
            f"re-check says needs_review should be {expected_review} but manifest has {stored_review}"
        )

    return problems


def process_source(bucket, bq_rows_for_source: list, source_gcs_uri: str) -> list:
    """Downloads one source PDF once and checks every manifest row that
    references it (completeness + category/reason re-check)."""
    issues = []
    prefix = f"gs://{BUCKET_NAME}/"
    if not source_gcs_uri or not source_gcs_uri.startswith(prefix):
        for row in bq_rows_for_source:
            issues.append((row, [f"unrecognized source_gcs_uri: {source_gcs_uri!r}"]))
        return issues
    blob_name = source_gcs_uri[len(prefix):]
    blob = bucket.blob(blob_name)
    if not blob.exists(timeout=GCS_TIMEOUT_SECONDS):
        for row in bq_rows_for_source:
            issues.append((row, [f"source PDF no longer exists in GCS: {source_gcs_uri}"]))
        return issues

    try:
        source_bytes = blob.download_as_bytes(timeout=GCS_TIMEOUT_SECONDS)
        source_doc = pymupdf.open(stream=source_bytes, filetype="pdf")
    except Exception as error:  # noqa: BLE001
        for row in bq_rows_for_source:
            issues.append((row, [f"failed to open source PDF: {error}"]))
        return issues

    try:
        page_count = len(source_doc)
        pages_per_survey = s1.PAGES_PER_SURVEY

        found_ranges = set()
        for row in bq_rows_for_source:
            row_issues = []
            page_range = row.get("source_page_range")
            if page_range:
                found_ranges.add(page_range)
            row_issues += check_folder_placement(row)
            row_issues += check_destination_exists(bucket, row)
            if page_count % pages_per_survey == 0:
                row_issues += check_category_and_reason(source_doc, row)
            if row_issues:
                issues.append((row, row_issues))

        if page_count % pages_per_survey == 0:
            expected_ranges = {
                f"{start + 1}-{start + pages_per_survey}"
                for start in range(0, page_count, pages_per_survey)
            }
            missing = expected_ranges - found_ranges
            for page_range in sorted(missing, key=lambda r: int(r.split("-")[0])):
                issues.append((
                    {
                        "source_file": bq_rows_for_source[0].get("source_file") if bq_rows_for_source else None,
                        "source_gcs_uri": source_gcs_uri,
                        "destination_folder": None,
                        "destination_gcs_uri": None,
                        "source_page_range": page_range,
                        "output_category": None,
                        "rejected": None,
                        "rejected_reason": None,
                        "needs_review": None,
                        "needs_review_reason": None,
                        "moved_at": None,
                    },
                    [f"no manifest row at all for page range {page_range} of this source PDF"],
                ))
        else:
            # Already correctly handled by step1 itself (see split_combined_pdf()'s
            # whole-source-failure branch) when there's exactly one manifest row for
            # this source, rejected, with a reason naming the actual malformed page
            # count - not a real discrepancy, so don't flag it.
            already_correctly_rejected = (
                len(bq_rows_for_source) == 1
                and bq_rows_for_source[0].get("rejected")
                and bq_rows_for_source[0].get("rejected_reason")
                and f"{page_count} page(s)" in bq_rows_for_source[0]["rejected_reason"]
                and "not a multiple of" in bq_rows_for_source[0]["rejected_reason"]
            )
            if not already_correctly_rejected:
                issues.append((
                    {
                        "source_file": bq_rows_for_source[0].get("source_file") if bq_rows_for_source else None,
                        "source_gcs_uri": source_gcs_uri,
                        "destination_folder": None,
                        "destination_gcs_uri": None,
                        "source_page_range": None,
                        "output_category": None,
                        "rejected": None,
                        "rejected_reason": None,
                        "needs_review": None,
                        "needs_review_reason": None,
                        "moved_at": None,
                    },
                    [f"source PDF has {page_count} page(s), not a multiple of {pages_per_survey} - "
                     "cannot verify completeness by page range"],
                ))
    finally:
        source_doc.close()
    return issues


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="step1_accuracy_issues.csv")
    parser.add_argument("--limit-sources", type=int, default=None,
                         help="Only check the first N distinct source PDFs (for a quick test run).")
    parser.add_argument("--resume", action="store_true",
                         help="Skip source PDFs already recorded as done in <out>.done (a one-"
                              "source-URI-per-line sidecar file), and append to --out instead of "
                              "overwriting it - for continuing after an interrupted run (e.g. the "
                              "machine slept mid-run) without re-spending Gemini calls on work "
                              "already done.")
    args = parser.parse_args()

    bq_client = bigquery.Client(project=PROJECT)
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.bucket(BUCKET_NAME)

    log("Fetching all pdf_manifest_list rows...")
    rows = fetch_manifest_rows(bq_client)
    log("Fetched %d manifest rows.", len(rows))

    by_source = {}
    for row in rows:
        by_source.setdefault(row.get("source_gcs_uri"), []).append(row)
    source_uris = sorted(by_source.keys(), key=lambda u: (u is None, u))
    if args.limit_sources:
        source_uris = source_uris[: args.limit_sources]

    done_log_path = f"{args.out}.done"
    already_done = set()
    if args.resume:
        try:
            with open(done_log_path) as f:
                already_done = {line.strip() for line in f if line.strip()}
        except FileNotFoundError:
            pass
        before = len(source_uris)
        source_uris = [uri for uri in source_uris if uri not in already_done]
        log("Resuming: skipping %d already-checked source PDF(s), %d remaining.",
            before - len(source_uris), len(source_uris))

    log("Checking %d distinct source PDFs across %d manifest rows.", len(source_uris), len(rows))

    all_issues = []
    completed = 0
    total = len(source_uris)
    lock = threading.Lock()

    def worker(source_uri):
        return process_source(bucket, by_source[source_uri], source_uri)

    csv_mode = "a" if (args.resume and already_done) else "w"
    csv_file = open(args.out, csv_mode, newline="")
    csv_writer = csv.writer(csv_file)
    if csv_mode == "w":
        csv_writer.writerow([
            "source_file", "source_gcs_uri", "destination_folder", "destination_gcs_uri",
            "source_page_range", "output_category", "rejected", "rejected_reason",
            "needs_review", "needs_review_reason", "moved_at", "issues",
        ])
        csv_file.flush()
    done_log_file = open(done_log_path, "a" if args.resume else "w")

    def write_issue_row(row, problems):
        csv_writer.writerow([
            row.get("source_file"), row.get("source_gcs_uri"), row.get("destination_folder"),
            row.get("destination_gcs_uri"), row.get("source_page_range"), row.get("output_category"),
            row.get("rejected"), row.get("rejected_reason"), row.get("needs_review"),
            row.get("needs_review_reason"), row.get("moved_at"), " | ".join(problems),
        ])
        csv_file.flush()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            future_to_source = {executor.submit(worker, uri): uri for uri in source_uris}
            for future in concurrent.futures.as_completed(future_to_source):
                source_uri = future_to_source[future]
                try:
                    issues = future.result()
                except Exception as error:  # noqa: BLE001
                    issues = [({
                        "source_file": None, "source_gcs_uri": source_uri,
                        "destination_folder": None, "destination_gcs_uri": None,
                        "source_page_range": None, "output_category": None,
                        "rejected": None, "rejected_reason": None,
                        "needs_review": None, "needs_review_reason": None, "moved_at": None,
                    }, [f"check failed with an exception: {error}"])]
                with lock:
                    all_issues.extend(issues)
                    completed += 1
                    for row, problems in issues:
                        log("ISSUE [%s] (%s): %s",
                            row.get("source_file") or row.get("source_gcs_uri"),
                            row.get("source_page_range"), " | ".join(problems))
                        write_issue_row(row, problems)
                    done_log_file.write(f"{source_uri}\n")
                    done_log_file.flush()
                    if completed % 10 == 0 or completed == total:
                        log("Progress: %d/%d source PDFs checked, %d issue(s) found so far.",
                            completed, total, len(all_issues))
    finally:
        done_log_file.close()
        csv_file.close()

    log("Done. %d issue(s) found across %d source PDFs. Wrote %s.",
        len(all_issues), total, args.out)


if __name__ == "__main__":
    main()
