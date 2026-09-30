"""Ingests human-reviewed feedback (correct_answer) from the LATEST
tps_feedback_{datetime}.xlsx file step4_process_pdf.py's --export-needs-
review-feedback wrote to gs://<GCS_FEEDBACK_BUCKET>/<GCS_FEEDBACK_PREFIX>,
and applies it directly to BQ_TABLE_SURVEY_RESPONSES_WITH_FEEDBACK (never
to survey_responses itself - see step4_process_pdf.py's sync_survey_
responses_with_feedback() docstring for why feedback state lives only on
that separate table).

For every (folder_name, file_name, question_number) row the feedback file
covers, but ONLY if BOTH (a) that row isn't already marked updated_with_
feedback in survey_responses_with_feedback, and (b) correct_answer was
actually filled in for it - a row left blank is skipped entirely, not
marked reviewed, so it's exported again next time instead of being
silently dropped (see ingest_feedback_file()'s "WHEN MATCHED AND ..."
guard, which is also what makes re-running this against an old/already-
ingested file always safe):
  - survey_answer is overwritten with correct_answer;
  - correct_answer is stored too;
  - updated_with_feedback is set TRUE, feedback_updated_time to now, and
    ingested_from to this feedback file's own gs:// URI.

Run standalone: `python step6_load_feedback.py`.
"""
import argparse
import datetime
import io
import re
import sys

import pandas as pd

import pipeline_config
# Reuses step4_process_pdf.py's own BQ/GCS connection helpers and its
# sync_survey_responses_with_feedback() (which needs BQ_SURVEY_RESPONSES_
# SCHEMA/BQ_SURVEY_RESPONSES_WITH_FEEDBACK_SCHEMA, both defined there) so
# this file never has to duplicate that schema or its own sync logic.
import step4_process_pdf as step4


def err(msg, *args):
    print(msg % args if args else msg)


def status(msg, *args):
    print(msg % args if args else msg)


BQ_PROJECT = pipeline_config.GCP_PROJECT_ID
BQ_DATASET = pipeline_config.BQ_DATASET
BQ_SURVEY_TABLE = pipeline_config.BQ_TABLE_SURVEY_RESPONSES
BQ_FEEDBACK_TABLE = pipeline_config.BQ_TABLE_SURVEY_RESPONSES_WITH_FEEDBACK
FEEDBACK_BUCKET = pipeline_config.GCS_FEEDBACK_BUCKET
FEEDBACK_PREFIX = pipeline_config.GCS_FEEDBACK_PREFIX

_FEEDBACK_FILE_RE = re.compile(r"^tps_feedback_\d{8}_\d{6}\.xlsx$")


def find_latest_feedback_blob(bucket, prefix: str):
    """Returns the most-recently-EXPORTED tps_feedback_{datetime}.xlsx blob
    under prefix (the datetime suffix step4_process_pdf.py's --export-
    needs-review-feedback stamps into the name sorts lexicographically, so
    "greatest name" == "latest export run" - no need to trust GCS blob-
    updated timestamps), or None if no such file exists yet."""
    blobs = [
        b for b in bucket.client.list_blobs(bucket, prefix=prefix)
        if _FEEDBACK_FILE_RE.match(b.name.rsplit("/", 1)[-1])
    ]
    if not blobs:
        return None
    return max(blobs, key=lambda b: b.name)


def load_feedback_dataframe(blob) -> pd.DataFrame:
    excel_bytes = blob.download_as_bytes()
    df = pd.read_excel(io.BytesIO(excel_bytes))
    for col in ("folder_name", "file_name", "question_number", "correct_answer"):
        df[col] = df[col].apply(lambda value: None if pd.isna(value) else str(value).strip())
    return df


def ingest_feedback_file(
    bq_client,
    project: str,
    dataset: str,
    feedback_table: str,
    feedback_df: pd.DataFrame,
    ingested_from: str,
) -> int:
    """MERGEs feedback_df's correct_answer into feedback_table (survey_
    responses_with_feedback), keyed on (folder_name, file_name, question_
    number) - via a temporary staging table, so this is one BigQuery MERGE
    regardless of how many rows the feedback file covers, not one query per
    row. ingested_from is the gs:// URI of the feedback file feedback_df was
    loaded from, recorded on every row this call actually applies.

    A row is only actually applied when BOTH (a) feedback_table's OWN
    current updated_with_feedback for that key is still NULL - a row the
    file covers that's already been ingested (e.g. this same file was
    already ingested once, or a newer file has since superseded it) is
    matched but left untouched, so re-running this against an old file is
    always safe - and (b) this feedback row's own correct_answer is non-
    blank - a row a reviewer left blank is matched but left untouched too,
    so it stays pending and is exported again next time instead of being
    marked reviewed with nothing actually provided (see the "WHEN MATCHED
    AND ..." guard below). Returns the number
    of rows actually applied (not just matched)."""
    from google.cloud import bigquery

    now = datetime.datetime.now(datetime.timezone.utc)
    staging_df = feedback_df[["folder_name", "file_name", "question_number", "correct_answer"]].copy()
    staging_df["feedback_updated_time"] = now
    staging_df["ingested_from"] = ingested_from

    staging_table = f"{feedback_table}__feedback_staging"
    staging_table_id = f"{project}.{dataset}.{staging_table}"
    status("[FEEDBACK] Staging %d feedback row(s) into %s ...", len(staging_df), staging_table_id)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq_client.load_table_from_dataframe(staging_df, staging_table_id, job_config=job_config).result()

    full_feedback_table_id = f"{project}.{dataset}.{feedback_table}"
    query = f"""
        MERGE `{full_feedback_table_id}` T
        USING `{staging_table_id}` S
        ON T.folder_name = S.folder_name AND T.file_name = S.file_name AND T.question_number = S.question_number
        WHEN MATCHED AND T.updated_with_feedback IS NULL
             AND S.correct_answer IS NOT NULL AND S.correct_answer != '' THEN UPDATE SET
            -- A reviewer left correct_answer blank on a row - that's not feedback yet, so
            -- this row isn't touched at all (see the WHEN MATCHED condition above): it's
            -- left pending, to be exported again on the next --export-needs-review-feedback
            -- run rather than silently marked reviewed with nothing actually provided.
            T.survey_answer = S.correct_answer,
            T.correct_answer = S.correct_answer,
            T.updated_with_feedback = TRUE,
            T.feedback_updated_time = S.feedback_updated_time,
            T.ingested_from = S.ingested_from
    """
    status("[FEEDBACK] Ingesting feedback into %s (skipping any row already covered by earlier feedback)...", full_feedback_table_id)
    result = bq_client.query(query).result()
    n_applied = result.num_dml_affected_rows or 0

    bq_client.delete_table(staging_table_id, not_found_ok=True)
    return n_applied


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bq-project", default=BQ_PROJECT, help="GCP project for the BigQuery tables.")
    ap.add_argument("--bq-dataset", default=BQ_DATASET, help="BigQuery dataset name.")
    ap.add_argument("--survey-table", default=BQ_SURVEY_TABLE, help="survey_responses table name (synced FROM, never modified).")
    ap.add_argument("--feedback-table", default=BQ_FEEDBACK_TABLE, help="survey_responses_with_feedback table name (synced/ingested INTO).")
    ap.add_argument("--feedback-bucket", default=FEEDBACK_BUCKET, help="GCS bucket to look for the latest tps_feedback_{datetime}.xlsx file in.")
    ap.add_argument("--feedback-prefix", default=FEEDBACK_PREFIX, help="GCS prefix (folder) to look for the latest tps_feedback_{datetime}.xlsx file under.")
    args, _unknown = ap.parse_known_args()

    bucket = step4.connect_gcs_bucket(args.feedback_bucket)
    blob = find_latest_feedback_blob(bucket, args.feedback_prefix)
    if blob is None:
        err(
            "[FEEDBACK] No tps_feedback_*.xlsx file found under gs://%s/%s - "
            "run `python step4_process_pdf.py --export-needs-review-feedback` first.",
            args.feedback_bucket, args.feedback_prefix,
        )
        sys.exit(1)
    gcs_uri = f"gs://{args.feedback_bucket}/{blob.name}"
    status("[FEEDBACK] Loading latest feedback file: %s", gcs_uri)
    feedback_df = load_feedback_dataframe(blob)
    if feedback_df.empty:
        status("[FEEDBACK] %s is empty - nothing to ingest.", gcs_uri)
        return

    bq_client = step4.connect_bigquery(args.bq_project)
    resolved_bq_project = args.bq_project or bq_client.project

    # Catch up feedback_table with any extraction rows survey_table has
    # gained since the last sync (same sync export_needs_review_feedback()
    # runs before exporting) - a row referenced by this feedback file is
    # guaranteed to already be there from that export, but running this
    # again is harmless (idempotent) and keeps feedback_table current even
    # if this file was exported a while ago.
    step4.sync_survey_responses_with_feedback(
        bq_client, resolved_bq_project, args.bq_dataset, args.survey_table, args.feedback_table,
    )

    n_applied = ingest_feedback_file(
        bq_client, resolved_bq_project, args.bq_dataset, args.feedback_table, feedback_df, gcs_uri,
    )
    status(
        "[FEEDBACK] Done. %d of %d feedback row(s) from %s were newly applied to %s (survey_answer/"
        "correct_answer set from the reviewer's input; updated_with_feedback=TRUE, feedback_updated_time=now, "
        "ingested_from=source file) - any others either already had feedback ingested, or were left blank "
        "by the reviewer and remain pending for a future export.",
        n_applied, len(feedback_df), gcs_uri, f"{resolved_bq_project}.{args.bq_dataset}.{args.feedback_table}",
    )


if __name__ == "__main__":
    main()
