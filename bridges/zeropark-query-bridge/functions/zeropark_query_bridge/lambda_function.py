"""
zeropark-query-bridge — Silnik SQL. Wykonuje zapytania na plikach CSV z S3
(następca DuckDBQueryLambda; identyczny kontrakt zdarzeń).
POZA VPC. Zależności: duckdb (Lambda Layer, właścicielem jest TEN stack),
rsa (requirements.txt), reports_utils (zvendorowany subset common-utils-layer).

Interfejs:
  action: "query"
    Input:  {"action": "query", "sql": "SELECT ... FROM read_csv_auto('s3://...') ..."}
    Output: {"status": "success", "data": "col1,col2\\nval1,val2\\n...", "row_count": 5, "columns": [...]}

  action: "query_export"
    Input:  {"action": "query_export", "sql": "SELECT ...", "export_name": "top_brands_comparison",
             "user_id": "U12345", "thread_ts": "1774631319.952000"}
    Output: {"status": "success", "s3_path": "s3://...", "download_url": "https://cf-domain/...",
             "row_count": 1234, "columns": [...], "preview": "col1,col2\\n..."}
"""
import json
import os
import shutil
import duckdb
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

MAX_RESULT_ROWS = int(os.environ.get('MAX_RESULT_ROWS', '50'))
MAX_EXPORT_ROWS = int(os.environ.get('MAX_EXPORT_ROWS', '50000'))
EXPORT_PREFIX = "raw_reports/query_results"

_LAYER_EXT_DIR = '/opt/.duckdb'
_TMP_EXT_DIR   = '/tmp/.duckdb'
_ext_ready     = False

s3_client = boto3.client('s3', region_name=AWS_REGION,
                         config=Config(signature_version='s3v4'))

# Walidacja SQL
ALLOWED_STATEMENTS = ["SELECT", "WITH", "SHOW", "DESCRIBE"]
BLOCKED_KEYWORDS = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
                     "TRUNCATE", "MERGE", "COPY", "EXPORT", "ATTACH", "INSTALL", "LOAD"]


# ==============================================
# WALIDACJA SQL
# ==============================================
def validate_sql(sql):
    sql_upper = sql.strip().upper()

    starts_ok = any(sql_upper.startswith(stmt) for stmt in ALLOWED_STATEMENTS)
    if not starts_ok:
        return False, "Zapytanie musi zaczynać się od SELECT, WITH, SHOW lub DESCRIBE."

    for kw in BLOCKED_KEYWORDS:
        if f" {kw} " in f" {sql_upper} ":
            return False, f"Zabroniona operacja: {kw}."

    return True, "OK"


# ==============================================
# INICJALIZACJA DUCKDB
# ==============================================

def _ensure_extensions() -> bool:
    """Copy pre-installed httpfs from layer to /tmp on cold start. No-op on warm invocations."""
    global _ext_ready
    if _ext_ready:
        return os.path.exists(_TMP_EXT_DIR)
    if os.path.exists(_LAYER_EXT_DIR):
        if not os.path.exists(_TMP_EXT_DIR):
            shutil.copytree(_LAYER_EXT_DIR, _TMP_EXT_DIR)
            logger.info("[DuckDB] Extensions copied from layer to /tmp (cold start)")
        _ext_ready = True
        return True
    logger.warning("[DuckDB] /opt/.duckdb not found — falling back to INSTALL httpfs")
    _ext_ready = True
    return False


def init_duckdb():
    """Creates in-memory DuckDB connection with httpfs loaded from pre-installed layer."""
    layer_present = _ensure_extensions()
    con = duckdb.connect(database=':memory:')
    con.execute("SET home_directory = '/tmp';")
    if layer_present:
        con.execute("SET autoinstall_known_extensions = false;")
        con.execute("LOAD httpfs;")
    else:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
    con.execute(f"""
        SET s3_region = '{AWS_REGION}';
        SET s3_url_style = 'path';
    """)
    return con


# Eager init — copy cost absorbed into cold start Init Duration, not first request
_ensure_extensions()


# ==============================================
# WYKONANIE ZAPYTANIA (inline, max 50 wierszy)
# ==============================================
def execute_query(sql):
    logger.info(f"[DuckDB] query SQL: {sql[:500]}")

    is_valid, msg = validate_sql(sql)
    if not is_valid:
        logger.error(f"[DuckDB] Walidacja FAILED: {msg}")
        return {"status": "error", "message": msg}

    con = None
    try:
        con = init_duckdb()

        limited_sql = f"SELECT * FROM ({sql}) _q LIMIT {MAX_RESULT_ROWS + 1}"
        result = con.execute(limited_sql)

        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        is_truncated = len(rows) > MAX_RESULT_ROWS
        if is_truncated:
            rows = rows[:MAX_RESULT_ROWS]

        csv_lines = [','.join(columns)]
        for row in rows:
            csv_lines.append(','.join(str(v) if v is not None else '' for v in row))

        csv_output = '\n'.join(csv_lines)
        truncation_note = f"\n(UWAGA: wynik obcięty do {MAX_RESULT_ROWS} wierszy)" if is_truncated else ""

        logger.info(f"[DuckDB] query OK — {len(rows)} wierszy, kolumny: {columns}")
        return {
            "status": "success",
            "data": csv_output + truncation_note,
            "row_count": len(rows),
            "columns": columns,
            "is_truncated": is_truncated
        }

    except duckdb.Error as e:
        logger.error(f"[DuckDB] DuckDB error: {e}")
        return {"status": "error", "message": f"DuckDB error: {str(e)}"}
    except Exception as e:
        logger.error(f"[DuckDB] Exception: {e}")
        return {"status": "error", "message": f"Błąd: {str(e)}"}
    finally:
        if con:
            con.close()


# ==============================================
# EKSPORT ZAPYTANIA NA S3 (pełny wynik + presigned URL)
# ==============================================
def execute_query_export(sql, export_name, user_id, thread_ts):
    logger.info(f"[DuckDB] query_export SQL: {sql[:500]}")
    logger.info(f"[DuckDB] export_name: {export_name}, user_id: {user_id}, MAX_EXPORT_ROWS: {MAX_EXPORT_ROWS}")

    is_valid, msg = validate_sql(sql)
    if not is_valid:
        logger.error(f"[DuckDB] Walidacja FAILED: {msg}")
        return {"status": "error", "message": msg}

    con = None
    try:
        con = init_duckdb()

        limited_sql = f"SELECT * FROM ({sql}) _q LIMIT {MAX_EXPORT_ROWS + 1}"
        result = con.execute(limited_sql)

        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        is_truncated = len(rows) > MAX_EXPORT_ROWS
        if is_truncated:
            rows = rows[:MAX_EXPORT_ROWS]

        # Budowanie CSV
        csv_lines = [','.join(columns)]
        for row in rows:
            csv_lines.append(','.join(str(v) if v is not None else '' for v in row))
        csv_output = '\n'.join(csv_lines)

        row_count = len(rows)
        logger.info(f"[DuckDB] query_export — {row_count} wierszy, {len(csv_output)} bajtów")

        # Generowanie ścieżki S3 (izolacja per user/thread)
        timestamp = datetime.now(timezone.utc).strftime("%H%M%S")
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in export_name)[:80]
        safe_thread = sanitize_thread_ts(thread_ts)
        filename = f"{safe_name}_{timestamp}.csv"

        if user_id and thread_ts:
            s3_key = f"raw_reports/{user_id}/{safe_thread}/{filename}"
        else:
            # Fallback dla wywołań testowych bez kontekstu
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            s3_key = f"{EXPORT_PREFIX}/{today}/{filename}"

        # Upload na S3
        logger.info(f"[DuckDB] Uploading → s3://{S3_BUCKET}/{s3_key}")
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=csv_output.encode('utf-8'),
            ContentType='text/csv'
        )

        # Download URL (CloudFront signed or S3 presigned fallback)
        try:
            download_url = generate_download_url(s3_key)
            logger.info("[DuckDB] Download URL generated.")
        except Exception as e:
            logger.error(f"[DuckDB] Download URL error: {e}")
            download_url = ""

        # Preview — pierwsze 5 wierszy danych
        preview_lines = csv_lines[:6]
        preview = '\n'.join(preview_lines)
        if row_count > 5:
            preview += f"\n... ({row_count} wierszy łącznie)"

        truncation_note = f" (UWAGA: wynik obcięty do {MAX_EXPORT_ROWS} wierszy)" if is_truncated else ""

        logger.info(f"[DuckDB] query_export OK — s3://{S3_BUCKET}/{s3_key}")
        return {
            "status": "success",
            "s3_path": f"s3://{S3_BUCKET}/{s3_key}",
            "s3_key": s3_key,
            "download_url": download_url,
            "row_count": row_count,
            "columns": columns,
            "is_truncated": is_truncated,
            "preview": preview + truncation_note,
            "export_name": export_name
        }

    except duckdb.Error as e:
        logger.error(f"[DuckDB] DuckDB error: {e}")
        return {"status": "error", "message": f"DuckDB error: {str(e)}"}
    except Exception as e:
        logger.error(f"[DuckDB] Exception: {e}")
        return {"status": "error", "message": f"Błąd: {str(e)}"}
    finally:
        if con:
            con.close()


# ==============================================
# HANDLER
# ==============================================
def lambda_handler(event, context):
    logger.info(f"=== DuckDB START === RequestId: {context.aws_request_id}")
    action = event.get("action")

    if action == "query":
        sql = event.get("sql", "")
        if not sql.strip():
            return {"status": "error", "message": "Puste zapytanie SQL."}
        return execute_query(sql)

    if action == "query_export":
        sql = event.get("sql", "")
        export_name = event.get("export_name", "query_result")
        user_id = event.get("user_id", "")
        thread_ts = event.get("thread_ts", "")
        if not sql.strip():
            return {"status": "error", "message": "Puste zapytanie SQL."}
        return execute_query_export(sql, export_name, user_id, thread_ts)

    logger.error(f"[DuckDB] Nieznana akcja: {action}")
    return {"status": "error", "message": f"Nieznana akcja: {action}"}