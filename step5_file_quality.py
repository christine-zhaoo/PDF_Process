#!/usr/bin/env python3
"""
build_file_quality_review.py

Standalone script that reads BigQuery's survey_responses table (produced by
merge_survey_pdfs.py's extraction step) and builds the file_quality_review
table from it — one row per source file, summarizing every needs_review=TRUE
question for quick triage.

Split out of merge_survey_pdfs.py (explicit user request: "I need to extract
the sections of pdf_quality_table from merge survey pdfs, to a separate py
file... it reads from the survey_responses table, and generates the
file_quality_review table... to make merge survey pdfs process smaller") —
this file is now a fully standalone tool with no import dependency on
merge_survey_pdfs.py, so it can be run/deployed/scheduled independently of
the Gemini/BigQuery extraction pipeline. merge_survey_pdfs.py no longer
builds or loads file_quality_review itself; run this script after each
extraction run (or on whatever schedule you like) to (re)build it.

--------------------------------------------------------------------------
What it does
--------------------------------------------------------------------------
1. Queries every row in survey_responses (optionally filtered to specific
   folder_name(s) via --folders).
2. Groups those rows by (folder_name, file_name) — one file's full set of
   question answers.
3. For each file, aggregates its rows into one file_quality_review row:
   whether ANY question needs_review, how many/out of how many, a
   human-readable issue_summary, and two REPEATED columns (issues,
   unreadable_areas) listing exactly which questions/pages to look at and
   why — using the same review-reason categorization
   (_classify_review_reason()) merge_survey_pdfs.py always has.
4. Loads the resulting rows into file_quality_review via the Spark BigQuery
   connector, partitioned by refreshed_date, replacing (delete-then-load)
   any existing rows for the same folder_name(s) processed this run.

--------------------------------------------------------------------------
Setup
--------------------------------------------------------------------------
1. pip install google-cloud-bigquery pandas pyspark
   (and the BigQuery Spark connector jar, e.g. via
   --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:...
   when launching the Spark session/cluster this script runs against)
2. Authenticate to GCP, one of:
   - gcloud auth application-default login
   - set GOOGLE_APPLICATION_CREDENTIALS to a service account key file


--------------------------------------------------------------------------
Running from a Jupyter / notebook cell
--------------------------------------------------------------------------
Do NOT do `python build_file_quality_review.py --dry-run` via `%run` and
expect flags to work the way they do in a terminal — inside a notebook
kernel, sys.argv holds the *kernel's* own launch arguments (e.g. "-f
/path/kernel.json"), not flags you typed, and argparse will error out on
them (SystemExit: 2) if it doesn't recognize them. This script tolerates
that (unknown args are ignored — you'll see a status line saying so instead
of a crash), but the more reliable pattern in a notebook is to skip the
command line entirely and call run() straight from a cell (this also
matters here because the load needs a SparkSession already in scope as the
global `spark`, which a %run subprocess-style invocation may not share
with your notebook the way a direct call does):

    from build_file_quality_review import run, BQ_DATASET, SURVEY_RESPONSES_TABLE, FILE_QUALITY_TABLE
    run(
        bq_project=None,             # None = your default GCP project
        bq_dataset=BQ_DATASET,
        survey_responses_table=SURVEY_RESPONSES_TABLE,
        file_quality_table=FILE_QUALITY_TABLE,
        folders=None,                # None = every folder in survey_responses
        dry_run=True,                # start with True to preview, then set False
    )
"""
import argparse
import datetime
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

# --------------------------------------------------------------------------
# Configuration — same defaults merge_survey_pdfs.py uses, kept here so this
# file runs standalone with no shared config import.
# --------------------------------------------------------------------------
BQ_PROJECT = "gcp-sapchoda-dev"
BQ_DATASET = "ladph_tps"
SURVEY_RESPONSES_TABLE = "survey_responses"
FILE_QUALITY_TABLE = "file_quality_review"

# The Spark BigQuery connector's default write path stages data through a
# GCS bucket before loading it into BigQuery (the connector's
# temporaryGcsBucket option only takes a bucket name, not a path/prefix -
# the connector manages its own temp object names/cleanup within it).
# There's no "main" bucket this script otherwise touches (it's pure
# BigQuery-to-BigQuery), so this has no default and must be set explicitly
# if the Spark write needs staging - most clusters have a
# spark.conf-level default already; pass --spark-staging-bucket if not.
SPARK_BQ_STAGING_BUCKET = None
# SPARK_BQ_STAGING_BUCKET = 'sytnasa-saas'
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    force=True,  # Jupyter/IPython often pre-configures the root logger before this
    # runs, which makes a plain basicConfig() a silent no-op — force=True makes sure
    # this handler actually attaches instead of logging output just disappearing.
)
log = logging.getLogger("build_file_quality_review")


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


# --------------------------------------------------------------------------
# Question number / page lookups — copied verbatim from merge_survey_pdfs.py
# (QUESTION_TEXT_BY_NUMBER / _QUESTION_NUMBER_BY_TEXT / _QUESTION_PAGE_NUMBER)
# so build_file_quality_row() below can turn a survey_responses row's
# survey_question text back into a bare question number, without importing
# merge_survey_pdfs.py's full SURVEY_QUESTIONS/build_full_question_text()
# machinery. If merge_survey_pdfs.py's own question set ever changes, this
# map needs to be updated here too — see the "Not in scope" note in this
# revision's project doc for why that tradeoff was accepted.
# --------------------------------------------------------------------------
_QUESTION_PAGE_NUMBER = {
    "H1": 1, "H2": 1, "H3": 1, "H4": 1, "H5": 1, "H6": 1,
    **{str(n): 1 for n in range(1, 24)},  # Q1-18 (grid) + Q19-23 (yesno_box, page_idx=0)
    "24": 2, "25": 2, "26": 2, "27": 2, "28": 2, "29": 2, "30": 2,
    "31": 2, "32": 2, "33": 2, "34": 2, "35": 2,
}


def _question_number_from_text(survey_question: str, question_number_by_text: dict) -> str:
    """Looks up a survey_responses row's stored survey_question TEXT back
    into its bare question number via the caller-supplied lookup (built once
    per run from the live survey_responses data itself — see
    build_question_number_lookup() — rather than a hardcoded copy of
    merge_survey_pdfs.py's own question text, which could drift out of sync
    with whatever text a given historical row actually stored)."""
    return question_number_by_text.get(survey_question, "?")


def build_question_number_lookup(rows: list) -> dict:
    """Builds a {survey_question TEXT: question number} lookup directly from
    the survey_responses rows being processed this run, by pulling the
    leading question number/label out of each row's own mark_position-
    adjacent conventions. Since survey_responses doesn't store a bare
    question number column (only the full question text), and this script
    is deliberately independent of merge_survey_pdfs.py's SURVEY_QUESTIONS
    definition, this infers the number from the text's own leading token —
    every question's stored text in this pipeline starts with its number
    followed by '.' or ')' (e.g. "H1. CalOMS Provider ID", "23) ..."), which
    is the one stable convention this script can rely on without importing
    merge_survey_pdfs.py."""
    lookup = {}
    for r in rows:
        text = r.get("survey_question") or ""
        stripped = text.strip()
        number = ""
        for sep in (". ", ") ", "."):
            if sep in stripped:
                candidate = stripped.split(sep, 1)[0].strip()
                if candidate and (candidate.isdigit() or (candidate[0] in "Hh" and candidate[1:].isdigit())):
                    number = candidate.upper() if candidate[0] in "Hh" else candidate
                    break
        if text and text not in lookup:
            lookup[text] = number or "?"
    return lookup


def _classify_review_reason(review_note: str) -> str:
    """Turns one row's (possibly multi-clause, semicolon-joined) review_note
    into a single short category phrase for the "issues" list - purely
    string-matching against the fixed phrasings merge_survey_pdfs.py's
    answers_to_qa_rows()/cross_check_answer() themselves generate, so this
    never invents information that isn't already in review_note. Copied
    verbatim from merge_survey_pdfs.py."""
    note = review_note.lower()
    # IMPORTANT: check the Cloud Vision cross-check phrasings BEFORE the
    # genuinely-blank check below. cross_check_written_field_with_vision()'s
    # own disagreement text for H1/H2/26 ("...possible hallucinated/
    # concatenated text...") and H4/H5/H6/24 ("...possible hallucination or
    # misread.") both use the word "hallucinat(ed/ion)" - but in BOTH cases
    # the model DID read real text (the note literally starts "model read
    # <the text it read> for <question>, but..."); Vision just couldn't
    # confirm it, which is a very different situation from the pixel
    # detector finding an actually-empty box. A naive "hallucinat" substring
    # match here previously mis-filed every one of these written-text Vision
    # disagreements as "scan appears blank" even when the field plainly had
    # handwritten content - this is that fix.
    if "cloud vision" in note:
        return "independent OCR cross-check disagreement (Vision couldn't confirm the model's written-text answer)"
    if "no box confidently marked" in note:
        return "model answered but scan appears blank here"
    if "auto-corrected to the pixel reading" in note:
        return "model/pixel disagreement (auto-corrected to the pixel reading)"
    if "reconciled to" in note or ("claimed" in note and "missed" in note) or "pixel found" in note:
        return "multi-select pixel reconciliation"
    if "self-reported confidence" in note:
        return "low model confidence"
    if "does not match choice at" in note or "is not a number" in note or "is out of range" in note \
            or "no mark_position" in note or "no answer text" in note or "mark position(s)" in note:
        return "model self-inconsistent (answer vs. mark position)"
    if "should be a 6-digit number" in note or "should be a date in mm/dd/yyyy format" in note:
        return "answer doesn't match this field's expected format (H1/H6)"
    if "model's own account of the scan" in note and len(review_note.split(";")) == 1:
        # nothing else fired but the model itself flagged a scan-quality issue
        # (rule 11) - surface that verbatim rather than a generic fallback.
        return "model-reported scan-quality issue"
    return "flagged for review"


def build_file_quality_row(
    folder: str,
    file_name: str,
    report_date: Optional[datetime.date],
    refreshed_at: datetime.datetime,
    rows: list,
    question_number_by_text: dict,
) -> dict:
    """Aggregates one file's full list of survey_responses rows (as plain
    dicts, straight from a BigQuery query result) into a single per-file
    quality-review row. Always returns a row (even when nothing needs
    review, so the table can also answer "how many files came back clean")
    - issue_summary/issues/unreadable_areas are simply empty/a
    clean-bill-of-health message in that case. Logic copied verbatim from
    merge_survey_pdfs.py's build_file_quality_row(), adapted to take plain
    dict rows (BigQuery query results) instead of in-process QARow objects."""
    report_date_str = report_date.isoformat() if report_date else None
    refreshed_at_str = refreshed_at.isoformat()
    refreshed_date_str = refreshed_at.date().isoformat()

    flagged = [r for r in rows if r.get("needs_review")]
    issues = []
    unreadable_areas = []
    for r in flagged:
        qnum = _question_number_from_text(r.get("survey_question") or "", question_number_by_text)
        page = _QUESTION_PAGE_NUMBER.get(qnum)
        review_note = r.get("review_note") or ""
        category = _classify_review_reason(review_note)
        issues.append(f"Q{qnum}: {category}")
        page_label = f"page {page}" if page else "page unknown"
        unreadable_areas.append(f"Question {qnum} ({page_label}): {review_note}")

    if flagged:
        issue_summary = (
            f"{len(flagged)} of {len(rows)} question(s) flagged for review "
            f"in {file_name}: " + "; ".join(issues) + "."
        )
    else:
        issue_summary = f"No issues detected in {file_name} - all {len(rows)} question(s) passed review."

    return {
        "folder_name": folder,
        "file_name": file_name,
        "report_date": report_date_str,
        "refreshed_at": refreshed_at_str,
        "refreshed_date": refreshed_date_str,
        "needs_review": bool(flagged),
        "flagged_question_count": len(flagged),
        "total_question_count": len(rows),
        "issue_summary": issue_summary,
        "issues": issues,
        "unreadable_areas": unreadable_areas,
    }


# --------------------------------------------------------------------------
# file_quality_review BigQuery schema + Spark-based partitioned load. Same
# registry shape and load pattern as merge_survey_pdfs.py's
# BQ_FILE_QUALITY_SCHEMA/load_rows_into_bq_via_spark() (Revision 40) —
# duplicated here (not imported) so this file has no dependency on that one.
# --------------------------------------------------------------------------
BQ_FILE_QUALITY_SCHEMA = [
    ("folder_name", "STRING", None),
    ("file_name", "STRING", None),
    ("report_date", "DATE", "Date the survey was filled out, parsed from the source date-folder name."),
    ("refreshed_at", "TIMESTAMP", "When this ETL run loaded the row."),
    ("refreshed_date", "DATE", "DATE(refreshed_at); the table's partitioning column."),
    ("needs_review", "BOOLEAN", "TRUE if ANY question in this file was flagged needs_review=TRUE in survey_responses."),
    ("flagged_question_count", "INTEGER", "How many of this file's questions were flagged needs_review=TRUE."),
    ("total_question_count", "INTEGER", "Total number of survey questions evaluated for this file."),
    ("issue_summary", "STRING", "One human-readable sentence summarizing every issue found in this file (or a clean-bill-of-health message if none)."),
    ("issues", "STRING", "Short 'Q<n>: <category>' phrase per flagged question - e.g. 'Q23: low model confidence'. Categories are derived from survey_responses.review_note (see _classify_review_reason() below) - not a new model call. REPEATED."),
    ("unreadable_areas", "STRING", "One 'Question <n> (page <p>): <review_note>' entry per flagged question, so a user can jump straight to the exact page/question that needs a look, with the full reason alongside it. REPEATED."),
]
_BQ_REPEATED_COLUMNS = {"issues", "unreadable_areas"}


def bq_schema_to_spark_schema(bq_schema):
    """Converts BQ_FILE_QUALITY_SCHEMA's plain (name, type, description)
    tuples into an explicit pyspark.sql.types.StructType, so the Spark load
    below uses a known schema instead of auto-inferring one from the data.
    See merge_survey_pdfs.py's identically-named function (Revision 40) for
    the full rationale; duplicated here rather than imported to keep this
    file standalone."""
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


def ensure_file_quality_table(bq_client, project: str, dataset: str, table: str):
    """Creates (or, on an older table, patches) the file_quality_review
    table schema - same create-or-patch pattern merge_survey_pdfs.py uses
    for its own tables, so an existing table never needs to be dropped just
    because a new column is added to BQ_FILE_QUALITY_SCHEMA later."""
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
        for name, typ, desc in BQ_FILE_QUALITY_SCHEMA
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
        status("[BQ] Creating table %s.%s.%s (partitioned by refreshed_date)", project, dataset, table)
        new_table = bigquery.Table(table_ref, schema=schema)
        new_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="refreshed_date",
        )
        bq_table = bq_client.create_table(new_table)
    return bq_table


def delete_existing_rows_for_folder(bq_client, table_ref, folder: str) -> None:
    """DELETEs any rows already loaded for this folder_name in
    file_quality_review, so re-running this script against the same folder
    replaces its rows instead of duplicating them. A no-op (deletes 0 rows)
    the first time a folder is loaded. Uses a query job (DML), not a
    streaming insert, so this delete-then-load pattern doesn't hit
    BigQuery's "can't UPDATE/DELETE rows that were just streamed in"
    restriction. Copied verbatim from merge_survey_pdfs.py."""
    from google.cloud import bigquery

    full_table_id = f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}"
    query = f"DELETE FROM `{full_table_id}` WHERE folder_name = @folder"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("folder", "STRING", folder)]
    )
    status("[BQ] Deleting existing rows (if any) for folder_name = %r in %s", folder, full_table_id)
    bq_client.query(query, job_config=job_config).result()


def load_rows_into_bq_via_spark(
    rows: list,
    bq_schema,
    project: str,
    dataset: str,
    table: str,
    staging_bucket: Optional[str] = None,
) -> int:
    """Loads `rows` (a list of plain dicts) into BigQuery through Spark +
    the BigQuery Spark connector. Requires a SparkSession already in scope
    as the global `spark` (e.g. injected by a notebook environment such as
    Databricks) - this function does not create one itself. Writes with
    mode='append', composing with delete_existing_rows_for_folder() above
    (the DELETE happens first; this call only appends). Copied verbatim
    (aside from the partition field name, unchanged here) from
    merge_survey_pdfs.py's identically-named function (Revision 40/42).

    staging_bucket: the connector's default ("indirect") write path stages
    the DataFrame's data into a GCS bucket before loading it into BigQuery,
    and raises "Either temporary or persistent GCS bucket must be set" if
    none is configured. Defaults to SPARK_BQ_STAGING_BUCKET if not given -
    unlike merge_survey_pdfs.py/merge_pdfs_to_folder.py, this script has no
    "main" GCS bucket of its own (it only touches BigQuery), so
    SPARK_BQ_STAGING_BUCKET defaults to None here and must be set explicitly
    (via --spark-staging-bucket, a config edit, or a cluster-level Spark
    conf) if the connector doesn't already have one configured."""
    if not rows:
        return 0
    if "spark" not in globals():
        raise RuntimeError(
            "load_rows_into_bq_via_spark() requires a SparkSession already in "
            "scope as the global `spark` (e.g. running inside a Databricks "
            "notebook) - none was found. Run this from a Spark-enabled "
            "notebook environment."
        )
    staging_bucket = staging_bucket or SPARK_BQ_STAGING_BUCKET
    import pandas as pd

    # build_file_quality_row() stores report_date/refreshed_at/refreshed_date
    # as ISO strings (matching merge_survey_pdfs.py's own QARow convention),
    # but Spark's DateType/TimestampType converters need real date/datetime
    # objects, not strings - PySparkTypeError: "DateType() can not accept
    # object '2025-11-18' in type <class 'str'>" otherwise. Convert any
    # DATE/TIMESTAMP-typed column's string values before handing rows to
    # pandas, rather than changing build_file_quality_row()'s own return
    # shape (which stays plain-JSON-serializable on purpose, since it's also
    # what a --dry-run prints/could be handed to a non-Spark loader).
    date_type_columns = {name for name, typ, _desc in bq_schema if typ == "DATE"}
    timestamp_type_columns = {name for name, typ, _desc in bq_schema if typ == "TIMESTAMP"}
    normalized_rows = []
    for row in rows:
        normalized = dict(row)
        for col in date_type_columns:
            if isinstance(normalized.get(col), str):
                normalized[col] = _parse_iso_date(normalized[col])
        for col in timestamp_type_columns:
            value = normalized.get(col)
            if isinstance(value, str):
                normalized[col] = datetime.datetime.fromisoformat(value)
        normalized_rows.append(normalized)

    df = pd.DataFrame(normalized_rows, dtype=object)
    spark_schema = bq_schema_to_spark_schema(bq_schema)
    for field in spark_schema.fieldNames():
        if field not in df.columns:
            df[field] = None
    df = df[spark_schema.fieldNames()]
    df = df.where(df.notna(), None)

    spark_df = globals()["spark"].createDataFrame(df, schema=spark_schema)
    full_table_id = f"{project}.{dataset}.{table}"
    writer = spark_df.write.format("bigquery").option("table", full_table_id)
    if staging_bucket:
        writer = writer.option("temporaryGcsBucket", staging_bucket)
#     (
#         writer
#         .option("partitionField", "refreshed_date")
#         .option("partitionType", "DAY")
#         .mode("append")
#         .save()
#     )
    
    status(
        "[BQ][SPARK] Wrote %d row(s) into %s via Spark (partitioned by refreshed_date%s).",
        len(rows), full_table_id, f", staged through gs://{staging_bucket}" if staging_bucket else "",
    )
    spark_df = spark_df.withColumn("event_partition", spark_df["report_date"])
    writeToEventStore(spark_df, '@OutputTable1', 1, "event_partition")
    
    return len(rows)


def query_survey_responses(bq_client, project: str, dataset: str, table: str, folders: Optional[list] = None) -> list:
    """Queries every row of survey_responses (optionally restricted to
    specific folder_name(s)), returning plain dicts. This is the one piece
    of genuinely new logic in this script (merge_survey_pdfs.py never reads
    survey_responses back — it only ever wrote it) since file_quality_review
    now has to be built FROM BigQuery data rather than from in-process QARow
    objects produced during extraction."""
    full_table_id = f"{project}.{dataset}.{table}"
    if folders:
        print(f'folders:{folders}')
        query = f"SELECT * FROM `{full_table_id}` WHERE folder_name IN UNNEST(@folders)"
        job_config = bq_client_query_config(folders)
        result = bq_client.query(query, job_config=job_config).result()
    else:
        query = f"SELECT * FROM `{full_table_id}`"
        result = bq_client.query(query).result()
    rows = [dict(row) for row in result]
    print (f'query:{query}')
    status("[BQ] Queried %d row(s) from %s%s.", len(rows), full_table_id, f" for folder(s) {folders}" if folders else "")
    return rows


def bq_client_query_config(folders: list):
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("folders", "STRING", folders)]
    )


def build_file_quality_rows(rows: list, refreshed_at: Optional[datetime.datetime] = None) -> list:
    """Groups a flat list of survey_responses rows (plain dicts) by
    (folder_name, file_name) and aggregates each group into one
    file_quality_review row via build_file_quality_row(). `refreshed_at`
    defaults to now (UTC) if not given, matching how merge_survey_pdfs.py
    always stamped this at extraction time — here it's stamped at the time
    this script actually runs, since that's a distinct (and possibly much
    later) event from when the underlying survey_responses rows were
    extracted."""
    refreshed_at = refreshed_at or datetime.datetime.utcnow()
    question_number_by_text = build_question_number_lookup(rows)

    grouped = defaultdict(list)
    report_dates = {}
    for r in rows:
        key = (r.get("folder_name"), r.get("file_name"))
        grouped[key].append(r)
        if key not in report_dates:
            rd = r.get("report_date")
            report_dates[key] = rd if isinstance(rd, (datetime.date, type(None))) else _parse_iso_date(rd)

    quality_rows = []
    for (folder, file_name), file_rows in grouped.items():
        quality_rows.append(
            build_file_quality_row(
                folder=folder,
                file_name=file_name,
                report_date=report_dates.get((folder, file_name)),
                refreshed_at=refreshed_at,
                rows=file_rows,
                question_number_by_text=question_number_by_text,
            )
        )
    return quality_rows


def _parse_iso_date(value) -> Optional[datetime.date]:
    if not value:
        return None
    if isinstance(value, datetime.date):
        return value
    try:
        return datetime.date.fromisoformat(str(value))
    except ValueError:
        return None


def run(
    bq_project: Optional[str] = BQ_PROJECT,
    bq_dataset: str = BQ_DATASET,
    survey_responses_table: str = SURVEY_RESPONSES_TABLE,
    file_quality_table: str = FILE_QUALITY_TABLE,
    folders: Optional[list] = None,
    dry_run: bool = False,
    replace_existing_folder_rows: bool = True,
    spark_staging_bucket: Optional[str] = None,
) -> None:
    from google.cloud import bigquery

    bq_client = bigquery.Client(project=bq_project)
    resolved_bq_project = bq_project or bq_client.project

    response_rows = query_survey_responses(bq_client, resolved_bq_project, bq_dataset, survey_responses_table, folders)
    if not response_rows:
        err(
            "[QUALITY] No survey_responses rows found in %s.%s.%s%s — nothing to build.",
            resolved_bq_project, bq_dataset, survey_responses_table,
            f" for folder(s) {folders}" if folders else "",
        )
        return

    refreshed_at = datetime.datetime.utcnow()
    quality_rows = build_file_quality_rows(response_rows, refreshed_at=refreshed_at)
    n_flagged_files = sum(1 for r in quality_rows if r["needs_review"])
    status(
        "[QUALITY] Built %d file_quality_review row(s) from %d survey_responses row(s) (%d file(s) flagged needs_review).",
        len(quality_rows), len(response_rows), n_flagged_files,
    )

    if dry_run:
        status("[QUALITY] Dry run complete. Nothing written to %s.%s.%s.", resolved_bq_project, bq_dataset, file_quality_table)
        return

    quality_table_ref = ensure_file_quality_table(bq_client, resolved_bq_project, bq_dataset, file_quality_table)

    if replace_existing_folder_rows:
        touched_folders = sorted({r["folder_name"] for r in quality_rows if r["folder_name"]})
        for folder in touched_folders:
            delete_existing_rows_for_folder(bq_client, quality_table_ref.reference, folder)

    n_loaded = load_rows_into_bq_via_spark(
        quality_rows, BQ_FILE_QUALITY_SCHEMA, resolved_bq_project, bq_dataset, file_quality_table,
        staging_bucket=spark_staging_bucket or SPARK_BQ_STAGING_BUCKET,
    )
    status(
        "[QUALITY] Done. Loaded %d row(s) into %s.%s.%s (%d file(s) flagged needs_review).",
        n_loaded, resolved_bq_project, bq_dataset, file_quality_table, n_flagged_files,
    )


# --------------------------------------------------------------------------
# Offline self-test: exercises the categorization, grouping, and aggregation
# logic against fake survey_responses rows, plus the Spark load against a
# real local SparkSession with the actual BigQuery write stubbed out. No
# network / GCP access needed.
# Run with: python build_file_quality_review.py --self-test
# --------------------------------------------------------------------------
def self_test() -> None:
    # --- _classify_review_reason() ---
    assert _classify_review_reason("model self-reported confidence 0.62 is below threshold 0.80") == "low model confidence"
    assert _classify_review_reason("no box confidently marked for this question") == "model answered but scan appears blank here"
    assert _classify_review_reason("model/pixel disagreement - auto-corrected to the pixel reading") == "model/pixel disagreement (auto-corrected to the pixel reading)"
    assert _classify_review_reason("independent Cloud Vision cross-check disagrees") == "independent OCR cross-check disagreement (Vision couldn't confirm the model's written-text answer)"
    assert _classify_review_reason("something totally unrecognized") == "flagged for review"

    # --- build_question_number_lookup() / _question_number_from_text() ---
    fake_rows = [
        {"survey_question": "H1. CalOMS Provider ID", "survey_answer": "123456", "needs_review": False, "review_note": ""},
        {"survey_question": "23) Some grid question text", "survey_answer": "Yes", "needs_review": True, "review_note": "model self-reported confidence 0.5 is below threshold 0.8"},
    ]
    lookup = build_question_number_lookup(fake_rows)
    assert lookup["H1. CalOMS Provider ID"] == "H1", lookup
    assert lookup["23) Some grid question text"] == "23", lookup
    assert _question_number_from_text("not in lookup", lookup) == "?"

    # --- build_file_quality_row(): flagged file ---
    refreshed_at = datetime.datetime(2025, 11, 18, 12, 0, 0)
    flagged_row = build_file_quality_row(
        folder="Nov 18 2025",
        file_name="Nov18_1.pdf",
        report_date=datetime.date(2025, 11, 18),
        refreshed_at=refreshed_at,
        rows=fake_rows,
        question_number_by_text=lookup,
    )
    assert flagged_row["needs_review"] is True, flagged_row
    assert flagged_row["flagged_question_count"] == 1, flagged_row
    assert flagged_row["total_question_count"] == 2, flagged_row
    assert flagged_row["issues"] == ["Q23: low model confidence"], flagged_row["issues"]
    assert "Question 23 (page 1)" in flagged_row["unreadable_areas"][0], flagged_row["unreadable_areas"]
    assert flagged_row["report_date"] == "2025-11-18", flagged_row
    assert flagged_row["refreshed_date"] == "2025-11-18", flagged_row

    # --- build_file_quality_row(): clean file (no flags) ---
    clean_rows = [{"survey_question": "H1. CalOMS Provider ID", "survey_answer": "123456", "needs_review": False, "review_note": ""}]
    clean_row = build_file_quality_row(
        folder="Nov 18 2025", file_name="Nov18_2.pdf", report_date=None,
        refreshed_at=refreshed_at, rows=clean_rows, question_number_by_text=lookup,
    )
    assert clean_row["needs_review"] is False, clean_row
    assert clean_row["flagged_question_count"] == 0, clean_row
    assert clean_row["issues"] == [], clean_row
    assert "No issues detected" in clean_row["issue_summary"], clean_row
    assert clean_row["report_date"] is None, clean_row

    # --- build_file_quality_rows(): groups multiple files correctly ---
    all_rows = [
        {"folder_name": "Nov 18 2025", "file_name": "Nov18_1.pdf", "report_date": "2025-11-18", **fake_rows[0]},
        {"folder_name": "Nov 18 2025", "file_name": "Nov18_1.pdf", "report_date": "2025-11-18", **fake_rows[1]},
        {"folder_name": "Nov 18 2025", "file_name": "Nov18_2.pdf", "report_date": "2025-11-18", **clean_rows[0]},
    ]
    grouped_rows = build_file_quality_rows(all_rows, refreshed_at=refreshed_at)
    assert len(grouped_rows) == 2, grouped_rows
    by_file = {r["file_name"]: r for r in grouped_rows}
    assert by_file["Nov18_1.pdf"]["flagged_question_count"] == 1, by_file["Nov18_1.pdf"]
    assert by_file["Nov18_2.pdf"]["flagged_question_count"] == 0, by_file["Nov18_2.pdf"]

    # --- Spark load, real local SparkSession, BQ write stubbed out ---
    spark_load_summary = "SKIPPED (pyspark not installed in this environment)"
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        SparkSession = None

    if SparkSession is not None:
        test_spark = SparkSession.builder.master("local[1]").appName("build_file_quality_review-self-test").getOrCreate()
        globals()["spark"] = test_spark
        try:
            written = {}

            class _FakeWriter:
                def __init__(self, df):
                    self._df = df
                    self._opts = {}

                def format(self, _fmt):
                    return self

                def option(self, key, value):
                    self._opts[key] = value
                    return self

                def mode(self, _mode):
                    return self

                def save(self):
                    written["df"] = self._df
                    written["opts"] = dict(self._opts)

            real_dataframe_class = type(test_spark.createDataFrame([(1,)], ["_probe"]))
            real_write_property = real_dataframe_class.write
            real_dataframe_class.write = property(lambda self: _FakeWriter(self))
            try:
                n_loaded = load_rows_into_bq_via_spark(
                    grouped_rows, BQ_FILE_QUALITY_SCHEMA, "fake-project", "lapd_survey", "file_quality_review",
                    staging_bucket="fake-staging-bucket",
                )
                assert n_loaded == len(grouped_rows), n_loaded
                assert written["opts"]["table"] == "fake-project.lapd_survey.file_quality_review", written["opts"]
                assert written["opts"]["temporaryGcsBucket"] == "fake-staging-bucket", written["opts"]
                assert written["opts"]["partitionField"] == "refreshed_date", written["opts"]
                assert written["opts"]["partitionType"] == "DAY", written["opts"]
                out_rows = written["df"].collect()
                assert len(out_rows) == len(grouped_rows), out_rows
                out_by_file = {r["file_name"]: r for r in out_rows}
                assert out_by_file["Nov18_1.pdf"]["issues"] == ["Q23: low model confidence"], out_by_file["Nov18_1.pdf"]["issues"]

                # empty-list no-op, no SparkSession call needed
                assert load_rows_into_bq_via_spark([], BQ_FILE_QUALITY_SCHEMA, "fake-project", "lapd_survey", "file_quality_review") == 0

                # no staging bucket configured at all -> temporaryGcsBucket simply omitted, not a crash
                written.clear()
                load_rows_into_bq_via_spark(grouped_rows[:1], BQ_FILE_QUALITY_SCHEMA, "fake-project", "lapd_survey", "file_quality_review", staging_bucket=None)
                assert "temporaryGcsBucket" not in written["opts"], written["opts"]

                spark_load_summary = f"{len(out_rows)} row(s) verified (table/partitionField/partitionType/temporaryGcsBucket + values, no-staging-bucket case verified)"
            finally:
                real_dataframe_class.write = real_write_property
        finally:
            del globals()["spark"]
            test_spark.stop()

    # --- missing-`spark`-global raises a clear RuntimeError ---
    assert "spark" not in globals()
    try:
        load_rows_into_bq_via_spark(grouped_rows, BQ_FILE_QUALITY_SCHEMA, "fake-project", "lapd_survey", "file_quality_review")
        raise AssertionError("expected RuntimeError when no `spark` global is set")
    except RuntimeError as e:
        assert "SparkSession" in str(e), e

    print("SELF-TEST PASSED")
    print("  _classify_review_reason(): verified against known phrasings + fallback")
    print("  build_question_number_lookup() / build_file_quality_row(): flagged + clean file cases verified")
    print(f"  build_file_quality_rows(): grouped {len(all_rows)} survey_responses row(s) into {len(grouped_rows)} file(s) correctly")
    print(f"  file_quality_review -> BigQuery via Spark: {spark_load_summary}")
    print("  missing-spark-global correctly raises RuntimeError")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bq-project", default=BQ_PROJECT, help="GCP project for both tables. Defaults to the caller's default project.")
    ap.add_argument("--bq-dataset", default=BQ_DATASET, help="BigQuery dataset containing both tables.")
    ap.add_argument("--survey-responses-table", default=SURVEY_RESPONSES_TABLE, help="Source table to read Q&A rows from.")
    ap.add_argument("--file-quality-table", default=FILE_QUALITY_TABLE, help="Destination table to (re)build.")
    ap.add_argument(
        "--folders",
        nargs="*",
        default=None,
        help="Only process these specific folder_name value(s) from survey_responses (default: every folder present).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only: query and build rows in memory, print the summary, but write nothing to BigQuery.",
    )
    ap.add_argument(
        "--no-replace-folder",
        action="store_true",
        help="Don't delete existing file_quality_review rows for a touched folder before loading - append only (can duplicate rows on a re-run).",
    )
    ap.add_argument(
        "--spark-staging-bucket",
        default=SPARK_BQ_STAGING_BUCKET,
        help="GCS bucket the Spark BigQuery connector stages this load through. This script has no other GCS bucket of its own - required unless your Spark cluster already has a default temporaryGcsBucket configured.",
    )
    ap.add_argument(
        "--self-test",
        action="store_true",
        help="Run the offline self-test (fake survey_responses rows, local SparkSession) and exit.",
    )
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

    if args.self_test:
        self_test()
        return

    run(
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        survey_responses_table=args.survey_responses_table,
        file_quality_table=args.file_quality_table,
        folders=args.folders,
        dry_run=args.dry_run,
        replace_existing_folder_rows=not args.no_replace_folder,
        spark_staging_bucket="syntasa-saas"
        # spark_staging_bucket=args.spark_staging_bucket,
        
    )


if __name__ == "__main__":
    main()