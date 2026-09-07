"""
zeropark-upload-bridge — Pobiera CSV z publicznego URL i zapisuje na S3
(następca S3UploadLambda; identyczny kontrakt zdarzeń).
POZA VPC. Zależności: boto3 (wbudowany), rsa (requirements.txt),
reports_utils (zvendorowany subset dawnego common-utils-layer).

Interfejs:
  Input:  {"action": "upload", "csv_url": "https://...", "report_name": "MasterReportRow",
           "user_id": "U12345", "thread_ts": "1774631319.952000"}
  Output: {"status": "success", "s3_path": "s3://...", "row_count": 123, "columns": "...", ...}

Izolacja: pliki zapisywane pod raw_reports/{user_id}/{thread_ts}/{report_name}_{HHMMSS}.csv
"""
import urllib.request
import os
import boto3
from botocore.config import Config
from datetime import datetime, timezone

import logging
from reports_utils import get_secret, sanitize_thread_ts, generate_download_url

# Logging — structured output for CloudWatch
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


# ==============================================
# KONFIGURACJA
# ==============================================
S3_BUCKET = os.environ.get('S3_BUCKET', 'zeropark-reports-datalake')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

s3_client = boto3.client('s3', region_name=AWS_REGION,
                         config=Config(signature_version='s3v4'))


# ==============================================
# TODO: STREAMING_INTERIM — remove _StreamingCSVReader and the streaming
# block in download_and_upload() once DevOps grants s3:GetObject on bucket
# zeropark-prod-document (account 978438254669) to THIS function's execution
# role (zeropark-upload-bridge stack; the pending DevOps request raised for the
# old S3UploadFunctionRole must be re-targeted at the new role).
# Replace with a single s3_client.copy_object() call — server-side copy,
# zero bytes through Lambda, no internet routing.
# Tracking: DevOps request raised 2026-04-14.
# ==============================================
class _StreamingCSVReader:
    """
    Wraps an HTTP response for streaming upload to S3 via upload_fileobj.
    Counts rows and captures the header line as data passes through.
    Constant ~8 MB memory footprint regardless of file size.
    """

    CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB — matches S3 multipart minimum part size

    def __init__(self, response):
        self._response = response
        self._header = ''
        self._row_count = 0
        self._header_captured = False
        self._done = False

    def read(self, size=-1):
        if self._done:
            return b''

        chunk = self._response.read(self.CHUNK_SIZE if size == -1 else size)
        if not chunk:
            self._done = True
            return b''

        # Count newlines for row_count
        self._row_count += chunk.count(b'\n')

        # Capture header from first chunk
        if not self._header_captured:
            first_newline = chunk.find(b'\n')
            if first_newline != -1:
                self._header = chunk[:first_newline].decode('utf-8', errors='replace').strip()
                self._header_captured = True

        return chunk

    @property
    def row_count(self) -> int:
        """Total data rows (header line excluded)."""
        return max(self._row_count - 1, 0)

    @property
    def columns(self) -> str:
        """First line of the CSV (header row)."""
        return self._header


# ==============================================
# POBRANIE CSV + UPLOAD NA S3
# ==============================================
def download_and_upload(csv_url, report_name, user_id, thread_ts):
    if not user_id:
        logger.warning("[S3Upload] Brak user_id — wymagany do izolacji plików.")
        return {"status": "error", "message": "Brak user_id."}

    safe_thread = sanitize_thread_ts(thread_ts)

    # --- 1. Generowanie ścieżki S3 (izolacja per user/thread) ---
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%H%M%S")
    filename = f"{report_name}_{timestamp}.csv"
    s3_key = f"raw_reports/{user_id}/{safe_thread}/{filename}"

    # --- 2. Streaming: HTTP response → S3 (no full file in memory) ---
    # TODO: STREAMING_INTERIM — replace this entire block with s3_client.copy_object()
    # once DevOps grants cross-account s3:GetObject on zeropark-prod-document.
    logger.info(f"[S3Upload] Streaming CSV → s3://{S3_BUCKET}/{s3_key}")
    logger.info(f"[S3Upload] Source: {csv_url[:120]}...")
    try:
        req = urllib.request.Request(csv_url)
        resp = urllib.request.urlopen(req, timeout=120)
        reader = _StreamingCSVReader(resp)

        s3_client.upload_fileobj(
            reader,
            S3_BUCKET,
            s3_key,
            ExtraArgs={'ContentType': 'text/csv'}
        )

        row_count = reader.row_count
        first_line = reader.columns
        logger.info(f"[S3Upload] Stream complete — {row_count} wierszy, kolumny: {first_line[:200]}")

    except Exception as e:
        logger.error(f"[S3Upload] Błąd streamingu: {e}")
        return {"status": "error", "message": f"Błąd streamingu CSV→S3: {str(e)}"}

    if not first_line:
        logger.warning("[S3Upload] Pobrany plik CSV jest pusty lub nie zawiera nagłówka.")
        return {"status": "error", "message": "Pobrany plik CSV jest pusty."}

    # --- 3. Download URL (CloudFront signed or S3 presigned fallback) ---
    try:
        download_url = generate_download_url(s3_key)
        logger.info("[S3Upload] Download URL generated.")
    except Exception as e:
        logger.error(f"[S3Upload] Download URL error: {e}")
        download_url = ""

    logger.info(f"[S3Upload] OK — s3://{S3_BUCKET}/{s3_key}")
    return {
        "status": "success",
        "s3_path": f"s3://{S3_BUCKET}/{s3_key}",
        "download_url": download_url,
        "s3_bucket": S3_BUCKET,
        "s3_key": s3_key,
        "row_count": row_count,
        "columns": first_line,
        "report_name": report_name,
        "report_date": today
    }


# ==============================================
# HANDLER
# ==============================================
def lambda_handler(event, context):
    logger.info(f"=== S3Upload START === RequestId: {context.aws_request_id}")
    action = event.get("action")

    if action == "upload":
        csv_url = event.get("csv_url", "")
        report_name = event.get("report_name", "")
        user_id = event.get("user_id", "")
        thread_ts = event.get("thread_ts", "")

        if not csv_url:
            return {"status": "error", "message": "Brak csv_url."}
        if not report_name:
            return {"status": "error", "message": "Brak report_name."}
        if not user_id:
            return {"status": "error", "message": "Brak user_id."}

        return download_and_upload(csv_url, report_name, user_id, thread_ts)

    logger.error(f"[S3Upload] Nieznana akcja: {action}")
    return {"status": "error", "message": f"Nieznana akcja: {action}"}