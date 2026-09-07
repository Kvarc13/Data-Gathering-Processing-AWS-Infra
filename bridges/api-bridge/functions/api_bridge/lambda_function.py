# =============================================================================
# File:     lambda_function.py
# Location: bi-automation\lambdas\zeropark\Automations_AWS\bi-data-pipeline\bridges\api-bridge\functions\api_bridge\lambda_function.py
# =============================================================================
"""
api-bridge - a stateless communication layer for external HTTP APIs.

This bridge knows NOTHING about any specific API. The caller (a
data-fetching engine) sends the full request spec; the bridge resolves the
API key from Secrets Manager, executes the request(s), converts the
response to CSV and lands it in S3. A new API = a new 'request' spec in a
pipeline's SOURCES + its key in Secrets Manager - no bridge change, ever.

Event contract - request mode:
  {
    "request": {
      "url":          "https://www.trillion.com/api.html",       # required
      "params":       {"username": "zeropark", "api_key": "{secret}",
                       "mode": "feedsreport", "reporttype": "daily",
                       "date": "{date}"},                        # optional
      "headers":      {"x-api-key": "{secret}"},                 # optional
      "secret":       "zeropark-bot/TRILLION_API_KEY",  # Secrets Manager id
      "secret_field": "api_key",     # JSON field; omit for bare-string secrets
      "format":       "csv",         # 'json' (default) | 'csv'
      "data_key":     "data",        # json: where the rows live
      "iterate":      "daily",       # one request per day of the window;
                                     # omit for one request per window
      "date_column":  "date",        # optional: stamp each row with its day
      "empty_header": ["SubId", "Revenue", "Visitors"],  # header when 0 rows
      "max_window_days": 7           # API hard window - clamped, never 400
    },
    "window": {"start": "2026-08-04", "end": "2026-08-10"},      # inclusive
    "bucket": "raw-data-imports-<account>",
    "key":    "MyDataset/2026-08-10/trillion.csv"
  }
Event contract - download mode (plain GET -> S3, streamed, any size):
  { "url": "https://.../report.csv", "bucket": ..., "key": ... }

Returns:
  {"key": "...", "rows": N}    # N = data rows; null for streamed downloads

Placeholders in params/headers values: {start} {end} {date} {secret} -
substituted by plain text replacement (tolerates other braces). {secret}
is the resolved key; the bridge never logs rendered URLs or headers.

Per-day tolerance: with 'iterate', a day that errors or answers with an
HTML page is logged and skipped (late/empty days are normal); without
'iterate', a failed request fails the job. Aggregation is NOT done here -
that belongs in the pipeline's COMBINE_SQL (DuckDB).
"""

import csv
import io
import json
import logging
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3 = boto3.client('s3', region_name='us-east-1')
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')

HTTP_TIMEOUT_SEC = 120


# ============================================================================
# Lambda entry point  ──  describes the whole job in one screen
# ============================================================================

def lambda_handler(event, context):
    job = parse_job(event)
    logger.info(f"{job['label']} -> s3://{job['bucket']}/{job['key']}")

    if job['mode'] == 'download':
        row_count = stream_url_to_s3(job['url'], job['bucket'], job['key'])
    else:
        row_count = run_request_job(job)

    logger.info(f"{row_count} row(s) -> s3://{job['bucket']}/{job['key']}")
    return {'key': job['key'], 'rows': row_count}


# ============================================================================
# Step 0: The job  ──  validate the caller's event
# ============================================================================

def parse_job(event: dict) -> dict:
    missing = [field for field in ('bucket', 'key') if not event.get(field)]
    if missing:
        raise ValueError(f'Missing event field(s): {", ".join(missing)}')

    if event.get('url'):
        return {**event, 'mode': 'download', 'label': event['url']}

    request = event.get('request')
    if not request or not request.get('url'):
        raise ValueError("Event needs either 'request' (with 'url') or "
                         "'url' (download mode)")
    return {**event, 'mode': 'request', 'label': request['url']}


# ============================================================================
# Step 1a: Download mode  ──  plain GET, multipart-streamed to S3
# ============================================================================

def stream_url_to_s3(url: str, bucket: str, key: str) -> None:
    """Constant memory whether the body is 5 KB or 5 GB. Row count unknown
    without parsing - returns None by design."""
    response = urllib.request.urlopen(urllib.request.Request(url),
                                      timeout=HTTP_TIMEOUT_SEC)
    s3.upload_fileobj(response, bucket, key, ExtraArgs={'ContentType': 'text/csv'})
    return None


# ============================================================================
# Step 1b: Request mode  ──  clamp, render, execute, convert, upload
# ============================================================================

def run_request_job(job: dict) -> int:
    request = job['request']
    span = clamp_window(job.get('window'), request.get('max_window_days'))
    secret = resolve_secret(request)

    rows = []
    for day_span in build_spans(request, span):
        rows.extend(fetch_rows(request, day_span, secret))

    csv_bytes = rows_to_csv(rows, request.get('empty_header', []))
    s3.put_object(Bucket=job['bucket'], Key=job['key'], Body=csv_bytes,
                  ContentType='text/csv')
    return len(rows)


def clamp_window(window: dict | None, max_window_days: int | None):
    """The span the API will accept: the requested window, clamped to the
    API's hard window (last N days, ending at latest yesterday) so
    out-of-window requests never 400. None = nothing to ask for."""
    if not window:
        return {'start': '', 'end': ''}   # dateless API
    start, end = window['start'], window['end']
    if max_window_days:
        today = datetime.now(timezone.utc).date()
        floor = (today - timedelta(days=max_window_days)).isoformat()
        yesterday = (today - timedelta(days=1)).isoformat()
        start, end = max(start, floor), min(end, yesterday)
    return {'start': start, 'end': end} if start <= end else None


def build_spans(request: dict, span: dict | None) -> list[dict]:
    """One substitution span per request: the whole window at once, or -
    with 'iterate': 'daily' - one span per day of the window."""
    if span is None:
        return []
    if request.get('iterate') == 'daily' and span['start']:
        return [{'start': day, 'end': day, 'date': day}
                for day in iter_dates(span['start'], span['end'])]
    return [{**span, 'date': span['start']}]


def iter_dates(start: str, end: str) -> list[str]:
    first = datetime.strptime(start, '%Y-%m-%d').date()
    last = datetime.strptime(end, '%Y-%m-%d').date()
    return [(first + timedelta(days=offset)).isoformat()
            for offset in range((last - first).days + 1)]


def fetch_rows(request: dict, span: dict, secret: str) -> list[dict]:
    """One HTTP request -> parsed rows. Iterated days tolerate failures
    (warn + skip, like late/empty days); a single request propagates them.
    Rendered URLs/headers may contain the secret - never logged."""
    substitutions = {**span, 'secret': secret}
    url = build_url(request['url'], render(request.get('params', {}), substitutions))
    headers = render(request.get('headers', {}), substitutions)
    try:
        http_request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(http_request, timeout=HTTP_TIMEOUT_SEC) as response:
            body = response.read().decode('utf-8-sig', errors='replace')
        rows = parse_rows(request, body, span)
        logger.info(f"{request['url']} [{span.get('date') or 'window'}]: "
                    f"{len(rows)} row(s)")
        return rows
    except Exception as error:
        if request.get('iterate'):
            logger.warning(f"{request['url']} [{span.get('date')}]: "
                           f"skipped - {error}")
            return []
        raise


def parse_rows(request: dict, body: str, span: dict) -> list[dict]:
    if request.get('format', 'json') == 'json':
        payload = json.loads(body)
        rows = payload if isinstance(payload, list) \
            else payload.get(request.get('data_key', 'data'), [])
    else:   # csv
        text = body.strip()
        if not text or text.startswith('<'):   # empty day or an HTML error page
            logger.warning(f"{request.get('url')} [{span.get('date')}]: no data")
            return []
        rows = list(csv.DictReader(io.StringIO(text)))

    if request.get('date_column'):
        for row in rows:
            row[request['date_column']] = span.get('date', '')
    return rows


def render(template: dict, substitutions: dict) -> dict:
    """{start}/{end}/{date}/{secret} in values, by plain text replacement
    (tolerates any other braces in values)."""
    rendered = {}
    for key, value in template.items():
        text = str(value)
        for placeholder, replacement in substitutions.items():
            text = text.replace('{' + placeholder + '}', str(replacement))
        rendered[key] = text
    return rendered


def build_url(url: str, params: dict) -> str:
    return f'{url}?{urllib.parse.urlencode(params)}' if params else url


def rows_to_csv(rows: list[dict], empty_header: list[str]) -> bytes:
    """Rows -> CSV bytes; the header is the ordered union of keys across all
    rows (iterated days may differ). With no rows, empty_header still gives
    downstream SQL its columns."""
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    if not fields:
        fields = empty_header
    if not fields:
        raise ValueError('API returned no rows and the request has no '
                         'empty_header - set it so downstream SQL has columns')
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fields, restval='', extrasaction='ignore')
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode()


# ============================================================================
# AWS Secrets Manager  (shared utility)
# ============================================================================

def resolve_secret(request: dict) -> str:
    """The value substituted for {secret}. Bare-string secrets as-is; JSON
    secrets need 'secret_field'. No 'secret' in the spec = open API."""
    if not request.get('secret'):
        return ''
    raw = secrets_client.get_secret_value(SecretId=request['secret'])['SecretString']
    field = request.get('secret_field')
    return json.loads(raw, strict=False)[field] if field else raw
