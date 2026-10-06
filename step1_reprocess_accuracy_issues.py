#!/usr/bin/env python3
"""Reprocesses exactly the survey units flagged by step1_accuracy_check.py
(step1_accuracy_issues.csv), using the corrected language gate (see
validate_survey_language()'s updated prompt - now judges only the survey
FORM's own printed language, not a handwritten answer's language) and the
already-fixed cursive-Declined-word prompt.

For each (source_gcs_uri, source_page_range) in the CSV:
  1. Calls organize_combined_pdf() with only_page_range set to just that one
     survey unit - re-runs the real gates (language, declined-word/scribble,
     blank/unreadable) against the actual page(s) and uploads to whatever
     destination subfolder the FRESH result calls for. If that's a
     different subfolder than before (e.g. normal -> Declined), the new
     file is uploaded alongside the old one still sitting in its old
     subfolder - organize_combined_pdf() never deletes anything itself.
  2. cleanup_superseded_destination_files() then deletes the stale OLD
     destination file for that exact survey unit, only after confirming the
     new one exists.
  3. The fresh ManifestRow replaces the old one in pdf_manifest_list via the
     normal delete-then-load path.

Only touches the specific survey units listed in the CSV - never a bulk
re-run of an entire source PDF's other already-correct survey units."""
import argparse
import csv
import datetime

from google.cloud import bigquery, storage

import step1_evaluate_pdf as s1


def log(msg, *args):
    print(f"[{datetime.datetime.utcnow().isoformat()}Z] {msg % args}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="step1_accuracy_issues.csv")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with open(args.csv) as f:
        rows = [r for r in csv.DictReader(f) if r.get("source_gcs_uri") and r.get("source_page_range")]
    log("Found %d flagged survey unit(s) with a source_gcs_uri + source_page_range to reprocess.", len(rows))

    bq_client = bigquery.Client(project=s1.BQ_PROJECT)
    storage_client = storage.Client(project=s1.BQ_PROJECT)
    bucket = storage_client.bucket(s1.BUCKET_NAME)
    prefix = f"gs://{s1.BUCKET_NAME}/"

    _, folder_names = s1.list_root_contents(bucket, s1.ROOT_PREFIX)
    year = s1.infer_survey_year(folder_names, s1.BUCKET_NAME, s1.ROOT_PREFIX)
    log("Using inferred survey year=%d for every reprocessed unit.", year)

    all_moved_rows = []
    for row in rows:
        source_uri = row["source_gcs_uri"]
        page_range = row["source_page_range"]
        start_str, end_str = page_range.split("-")
        only_page_range = (int(start_str), int(end_str))

        if not source_uri.startswith(prefix):
            log("SKIP %s (%s): source_gcs_uri not under expected bucket.", source_uri, page_range)
            continue
        blob_name = source_uri[len(prefix):]
        blob = bucket.blob(blob_name)
        if not blob.exists():
            log("SKIP %s (%s): source PDF no longer exists in GCS.", source_uri, page_range)
            continue

        log("Reprocessing %s pages %s (year=%s)...", source_uri, page_range, year)
        if args.dry_run:
            continue

        result = s1.organize_combined_pdf(
            bucket, s1.ROOT_PREFIX, s1.DESTINATION_PREFIX, year,
            dry_run=False, blob=blob, only_page_range=only_page_range,
        )
        for moved_row in result.moved_rows:
            log("  -> new destination_gcs_uri=%s output_category=%s needs_review=%s reason=%s",
                moved_row.destination_gcs_uri, moved_row.output_category,
                moved_row.needs_review, moved_row.needs_review_reason)
        all_moved_rows.extend(result.moved_rows)

    if args.dry_run or not all_moved_rows:
        log("Dry run or nothing to write - stopping before any BQ/GCS write.")
        return

    log("Cleaning up any superseded old destination files for %d reprocessed unit(s)...", len(all_moved_rows))
    deleted = s1.cleanup_superseded_destination_files(
        bq_client, bucket, s1.BQ_PROJECT, s1.BQ_DATASET, s1.MANIFEST_TABLE, all_moved_rows,
    )
    log("Deleted %d superseded old destination file(s).", deleted)

    log("Replacing manifest row(s) in BigQuery...")
    s1.delete_existing_manifest_rows_for_sources(
        bq_client, s1.BQ_PROJECT, s1.BQ_DATASET, s1.MANIFEST_TABLE,
        [(r.source_gcs_uri, r.source_page_range) for r in all_moved_rows if r.source_gcs_uri],
    )
    s1.load_manifest_rows_into_bq_via_client(
        all_moved_rows, s1.BQ_PROJECT, s1.BQ_DATASET, s1.MANIFEST_TABLE, bq_client=bq_client,
    )
    log("Done. Reprocessed %d survey unit(s).", len(all_moved_rows))


if __name__ == "__main__":
    main()
