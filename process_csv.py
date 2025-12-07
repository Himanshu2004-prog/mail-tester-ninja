import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from google.cloud import storage

# 🔧 Read config from environment variables
CLOUD_FUNCTION_URL = os.getenv(
    "CLOUD_FUNCTION_URL",
    "https://us-central1-mailtester-validator.cloudfunctions.net/find_email",
)
GCS_BUCKET = os.getenv("GCS_BUCKET", "mailtester-email-bulk")
INPUT_BLOB_NAME = os.getenv("INPUT_BLOB_NAME", "input.csv")
OUTPUT_BLOB_NAME = os.getenv("OUTPUT_BLOB_NAME", "output.csv")

NUM_THREADS = int(os.getenv("NUM_THREADS", "5"))
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "0.5"))  # seconds


def download_blob_to_local(bucket_name, blob_name, local_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)
    print(f"Downloaded gs://{bucket_name}/{blob_name} to {local_path}")


def upload_local_to_blob(local_path, bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to gs://{bucket_name}/{blob_name}")


def process_row(row_index, row):
    first_name = (row.get("first_name") or "").strip()
    last_name = (row.get("last_name") or "").strip()
    company_website = (row.get("company_website") or "").strip()

    if not first_name or not company_website:
        print(f"[Row {row_index}] Skipped: missing first_name or company_website")
        return {
            "first_name": first_name,
            "last_name": last_name,
            "company_website": company_website,
            "email_found": None,
            "status_code": None,
            "validation_result": None,
            "total_credits_used": None,
            "error": "Missing required fields",
        }

    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "company_website": company_website,
    }

    print(f"[Row {row_index}] Processing: {first_name} {last_name or ''} @ {company_website}")

    try:
        response = requests.post(CLOUD_FUNCTION_URL, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[Row {row_index}] Error: {e}")
        data = {"error": str(e)}

    time.sleep(DELAY_BETWEEN_REQUESTS)

    return {
        "first_name": first_name,
        "last_name": last_name,
        "company_website": company_website,
        "email_found": data.get("email_found"),
        "status_code": data.get("status_code"),
        "validation_result": data.get("validation_result"),
        "total_credits_used": data.get("total_credits_used"),
        "error": data.get("error"),
    }


def run_job():
    local_input = "/tmp/input.csv"
    local_output = "/tmp/output.csv"

    print("Starting Cloud Run Job...")
    print(f"Bucket: {GCS_BUCKET}")
    print(f"Input blob: {INPUT_BLOB_NAME}")
    print(f"Output blob: {OUTPUT_BLOB_NAME}")
    print(f"Cloud Function URL: {CLOUD_FUNCTION_URL}")
    print(f"Threads: {NUM_THREADS}")

    # 1) Download input.csv from GCS
    download_blob_to_local(GCS_BUCKET, INPUT_BLOB_NAME, local_input)

    # 2) Read rows
    with open(local_input, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    print(f"Found {total} rows")

    fieldnames = [
        "first_name",
        "last_name",
        "company_website",
        "email_found",
        "status_code",
        "validation_result",
        "total_credits_used",
        "error",
    ]

    # 3) Process in parallel and write to local_output as we go
    with open(local_output, "w", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()
        out_f.flush()

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            future_to_index = {
                executor.submit(process_row, idx, row): idx
                for idx, row in enumerate(rows, start=1)
            }

            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                try:
                    result = future.result()
                except Exception as e:
                    print(f"[Row {idx}] Unhandled error in thread: {e}")
                    result = {
                        "first_name": None,
                        "last_name": None,
                        "company_website": None,
                        "email_found": None,
                        "status_code": None,
                        "validation_result": None,
                        "total_credits_used": None,
                        "error": f"Unhandled thread error: {e}",
                    }

                writer.writerow(result)
                out_f.flush()
                print(f"[Row {idx}] Saved to local output")

    # 4) Upload output.csv back to GCS
    upload_local_to_blob(local_output, GCS_BUCKET, OUTPUT_BLOB_NAME)

    print("Job completed successfully.")


if __name__ == "__main__":
    run_job()
