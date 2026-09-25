from google.cloud import storage, bigquery
import pandas as pd
import io


def err(msg, *args):
    print(msg % args if args else msg)


bucket_name = "tps_survey"
blob_name = (
    "TPS_Feedback/"
    "feedback_test.xlsx"
)

# ============================================================
# 0. Load the Excel feedback file from GCS
# ============================================================

try:
    storage_client = storage.Client()
except Exception as e:  # noqa: BLE001 - re-raised below, just adding a clear print first
    err(
        "[GCS] Could not create a GCS client — check you've run "
        "`gcloud auth application-default login` or set "
        "GOOGLE_APPLICATION_CREDENTIALS. Underlying error: %s",
        e,
    )
    raise

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)
print(f'blob:{blob}')

# Download the blob into a byte stream in memory
excel_bytes = blob.download_as_bytes()

# Load into a pandas DataFrame (requires openpyxl installed for .xlsx files)
df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='job_roMQ-mDUcadluqFA6ojdeiyWR0z')
print(df.head())

# ============================================================
# 1. Prepare the Excel user feedback file
# ============================================================

feedback_pdf = df.copy()

# Clean the columns used by the join and answer update
for col in ("folder_name", "file_name", "question_number", "correct_answer"):
    feedback_pdf[col] = feedback_pdf[col].apply(
        lambda value: None if pd.isna(value) else str(value).strip()
    )

# ============================================================
# 2. Load the existing BQ survey response table via the BigQuery
#    Python client — querying only the file_names we actually need.
# ============================================================

BQ_PROJECT = "gcp-sapchoda-dev"
BQ_DATASET = "ladph_tps"
BQ_TABLE = "survey_responses"


def load_all_survey_responses(
    project: str = BQ_PROJECT,
    dataset: str = BQ_DATASET,
    table: str = BQ_TABLE,
    client: "bigquery.Client" = None,
) -> pd.DataFrame:
    """Reads every row of survey_responses via the BigQuery Python client. Returns a pandas DataFrame."""
    client = client or bigquery.Client(project=project)
    full_table = f"{project}.{dataset}.{table}"
    return client.query(f"SELECT * FROM `{full_table}`").to_dataframe()


# Pull the full survey_responses table so every row carries over to the output table
survey_pdf = load_all_survey_responses()

# Make sure the join columns have compatible types
survey_pdf["folder_name"] = survey_pdf["folder_name"].apply(
    lambda value: None if pd.isna(value) else str(value).strip()
)
survey_pdf["file_name"] = survey_pdf["file_name"].apply(
    lambda value: None if pd.isna(value) else str(value).strip()
)
survey_pdf["question_number"] = survey_pdf["question_number"].apply(
    lambda value: None if pd.isna(value) else str(value)
)

# ============================================================
# 3. Prepare feedback columns for the join
# ============================================================

feedback_for_join = feedback_pdf[
    ["folder_name", "file_name", "question_number", "correct_answer"]
].rename(columns={
    "folder_name": "f_folder_name",
    "file_name": "f_file_name",
    "question_number": "f_question_number",
    "correct_answer": "f_correct_answer",
})

# ============================================================
# 4. Join Excel feedback to BQ responses
# ============================================================

joined_pdf = survey_pdf.merge(
    feedback_for_join,
    how="left",
    left_on=["folder_name", "file_name", "question_number"],
    right_on=["f_folder_name", "f_file_name", "f_question_number"],
    indicator=True,
)

# ============================================================
# 5. Compare and update survey_answer
# ============================================================

def _norm(value):
    return None if pd.isna(value) else str(value).strip().lower()


has_feedback = joined_pdf["_merge"] == "both"
answers_differ = has_feedback & (
    joined_pdf["survey_answer"].apply(_norm) != joined_pdf["f_correct_answer"].apply(_norm)
)

result_pdf = survey_pdf.copy()
result_pdf["correct_answer"] = joined_pdf["survey_answer"].where(
    ~answers_differ, joined_pdf["f_correct_answer"]
)
result_pdf["updated_by_user"] = answers_differ

# ============================================================
# 6. Review the changes
# ============================================================

# print(result_pdf[result_pdf["updated_by_user"]].to_string())

print("Rows updated by user:", int(result_pdf["updated_by_user"].sum()))
print("Total rows:", len(result_pdf))

print(result_pdf.to_string())

# ============================================================
# 7. Write result to a new BQ table: survey_responses + correct_answer
# ============================================================

OUTPUT_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.survey_responses_with_feedback"

bq_client = bigquery.Client(project=BQ_PROJECT)
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
load_job = bq_client.load_table_from_dataframe(
    result_pdf, OUTPUT_TABLE, job_config=job_config
)
load_job.result()
print(f"Wrote {len(result_pdf)} rows to {OUTPUT_TABLE}")
