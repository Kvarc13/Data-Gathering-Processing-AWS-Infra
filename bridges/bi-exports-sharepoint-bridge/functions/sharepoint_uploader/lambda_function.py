# =============================================================================
# File:     lambda_function.py
# Location: bi-automation/lambdas/zeropark/Automations_AWS/bi-exports-sharepoint-bridge/functions/sharepoint_uploader/lambda_function.py
# =============================================================================
"""
SharePoint uploader - generic.

Triggered by S3 ObjectCreated events under the prefix `exports/sharepoint/`.
Reads the CSV body and metadata of the new object, then applies the data to
the SharePoint workbook tab specified in the object's metadata.

Contract (metadata fields set by the producer Lambda on s3:PutObject):
  target-file   - required. Server-relative SharePoint path to the .xlsx file.
  target-sheet  - required. Sheet to modify (created if missing). Cannot be
                  the reserved "update Log" tab.
  write-mode    - optional. 'replace' (default) | 'append' | 'upsert'.
  upsert-key    - required for upsert. Comma-separated key column name(s),
                  e.g. 'date' or 'date,feed_hash'.
  max-rows      - optional int. After writing, keep only the newest N data
                  rows (header stays). Meant for append retention.
  allow-empty   - optional 'true'. Lets replace write a CSV with 0 data rows
                  (without it, an empty replace is refused so a failed source
                  query cannot silently wipe a sheet).
  source-table  - informational, for logs and the update Log tab.
  source-system - informational, for logs and the update Log tab.

`write-mode` is optional and defaults to 'replace', so producers written
before write modes existed keep working unchanged.

Every successful job also appends an entry to the "update Log" tab of the
target workbook (newest first, capped at the last 1000 updates), so anyone
opening the file can see when, what and how much was last written.

CSV carries no types - every field arrives as text. Numeric values are
converted back to numbers on write (see coerce_value); without that, cells
land as TEXT and are invisible to SUM, charts and pivots. Dates stay text.

Producer snippet - paste into any data-fetching lambda, no shared code needed:

    import uuid, boto3

    def send_to_sharepoint(csv_bytes, target_file, target_sheet,
                           mode='replace', source_system='?', source_table='?',
                           **extra):
        # extra (all optional, values as strings):
        #   upsert_key='date,feed_hash', max_rows='50000', allow_empty='true'
        metadata = {'target-file': target_file, 'target-sheet': target_sheet,
                    'write-mode': mode, 'source-system': source_system,
                    'source-table': source_table}
        metadata.update({k.replace('_', '-'): str(v) for k, v in extra.items()})
        key = f'exports/sharepoint/{source_system}/{uuid.uuid4()}.csv'
        boto3.client('s3', region_name='us-east-1').put_object(
            Bucket='automated-files', Key=key, Body=csv_bytes,
            ContentType='text/csv', Metadata=metadata)
        return key

After a successful update, the S3 object is deleted (the lifecycle rule on
the bucket is a safety net for the case where deletion fails).

Delivery guarantees:
  Each job runs at most once. 'replace' is naturally idempotent, but 'append'
  and 'upsert' are not safe to blindly re-run - a duplicate S3 event delivery
  or a Lambda retry after a partial failure would duplicate or re-apply rows.
  The idempotency guard below claims each job in DynamoDB before working,
  marks it completed after, and releases the claim on failure so a retry can
  still run. Reserved concurrency of 1 (set in template.yaml) serializes all
  workbook writes. Jobs still failing after 2 retries (including write-guard
  refusals) go to the DLQ with the original event; the S3 object also stays
  in the bucket, so nothing is lost.

PEM format note:
  The SharePoint credentials secret stores the X.509 certificate and the
  encrypted private key concatenated in a single 'private_key' field of the
  JSON value. cryptography>=42 doesn't tolerate that format - it tries to
  parse the first PEM block (the certificate) as the key and fails with
  InvalidPadding. _extract_private_key_only() pulls out just the private-key
  block before handing it to office365's with_client_certificate(). If you
  ever rotate the SharePoint cert, keep the same concatenated format OR
  update both this code and the secret-population step together.
"""

import csv
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from openpyxl.reader.excel import load_workbook


# Site profiles - same registry as sharepoint-reader; producers pick one via
# the OPTIONAL 'target-site' metadata (default 'bi-team' keeps every existing
# job working unchanged). The certificate secret is tenant-level, shared.
SITES = {
    'bi-team': 'https://centralnic.sharepoint.com/sites/BITeamCMTech',
    'bicmt':   'https://centralnic.sharepoint.com/sites/BICMT',
}
IDEMPOTENCY_TTL_SECONDS = 7 * 24 * 3600

# Write guard: replace with 0 data rows is refused unless the producer sets
# allow-empty=true (a failed source query must not silently wipe a sheet).
# Shrinking is allowed - smaller result sets are a normal business outcome.

UPDATE_LOG_SHEET = 'update Log'
UPDATE_LOG_MAX_ENTRIES = 1000
UPDATE_LOG_HEADER = ['timestamp_utc', 'sheet', 'mode', 'rows_written', 'source', 's3_key']


# ---------- SharePoint credentials ----------

def fetch_sharepoint_credentials() -> dict:
    secret_name = os.environ['SHAREPOINT_SECRET_NAME']
    client = boto3.client('secretsmanager', region_name='us-east-1')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'], strict=False)


# ---------- SharePoint I/O ----------

def _extract_private_key_only(pem_blob: str) -> str:
    """
    The SharePoint secret stores certificate + encrypted private key concatenated
    in a single 'private_key' field. cryptography>=42 doesn't tolerate that -
    it tries to parse the first PEM block (the certificate) as the key and
    fails with InvalidPadding. Extract only the PRIVATE KEY block.
    """
    markers = [
        ('-----BEGIN ENCRYPTED PRIVATE KEY-----', '-----END ENCRYPTED PRIVATE KEY-----'),
        ('-----BEGIN PRIVATE KEY-----', '-----END PRIVATE KEY-----'),
        ('-----BEGIN RSA PRIVATE KEY-----', '-----END RSA PRIVATE KEY-----'),
    ]
    for begin, end in markers:
        start_idx = pem_blob.find(begin)
        if start_idx == -1:
            continue
        end_idx = pem_blob.find(end, start_idx)
        if end_idx == -1:
            continue
        return pem_blob[start_idx : end_idx + len(end)]
    raise ValueError('No private key block found in PEM blob')


def connect_to_sharepoint(credentials: dict, site_url: str) -> ClientContext:
    cert_settings = dict(credentials['SPCredentials'])  # copy, don't mutate the original
    cert_settings['private_key'] = _extract_private_key_only(cert_settings['private_key'])
    context = ClientContext(site_url).with_client_certificate('centralnic.com', **cert_settings)
    context.load(context.web)
    context.execute_query()
    return context


def download_workbook(context: ClientContext, relative_url: str) -> io.BytesIO:
    response = File.open_binary(context, relative_url)
    return io.BytesIO(response.content)


def upload_workbook(context: ClientContext, relative_url: str, workbook_bytes: io.BytesIO) -> None:
    directory, filename = os.path.split(relative_url)
    context.web.get_folder_by_server_relative_url(directory).upload_file(filename, workbook_bytes).execute_query()


# ---------- Job metadata ----------

def parse_job_metadata(metadata: dict) -> dict:
    """Validate the producer's S3 metadata and fill in defaults."""
    missing = [key for key in ('target-file', 'target-sheet') if not metadata.get(key)]
    if missing:
        raise ValueError(f'Missing required S3 metadata: {", ".join(missing)}')

    if metadata['target-sheet'] == UPDATE_LOG_SHEET:
        raise ValueError(f'target-sheet cannot be the reserved "{UPDATE_LOG_SHEET}" tab')

    site = metadata.get('target-site', 'bi-team')
    if site not in SITES:
        raise ValueError(f'Unknown target-site "{site}". Sites: {", ".join(sorted(SITES))}')

    mode = metadata.get('write-mode', 'replace')
    if mode not in MODES:
        raise ValueError(f'Unknown write-mode "{mode}". Supported: {", ".join(sorted(MODES))}')

    upsert_keys = None
    if mode == 'upsert':
        upsert_keys = [k.strip() for k in metadata.get('upsert-key', '').split(',') if k.strip()]
        if not upsert_keys:
            raise ValueError('write-mode "upsert" requires upsert-key metadata '
                             '(comma-separated key column names)')

    max_rows = None
    if metadata.get('max-rows'):
        try:
            max_rows = int(metadata['max-rows'])
        except ValueError:
            raise ValueError(f'max-rows must be an integer, got "{metadata["max-rows"]}"')
        if max_rows <= 0:
            raise ValueError('max-rows must be a positive integer')

    return {
        'target_site': site,
        'target_file': metadata['target-file'],
        'target_sheet': metadata['target-sheet'],
        'mode': mode,
        'upsert_keys': upsert_keys,
        'max_rows': max_rows,
        'allow_empty': metadata.get('allow-empty', '').lower() == 'true',
        'source_system': metadata.get('source-system', '?'),
        'source_table': metadata.get('source-table', '?'),
    }


# ---------- Write modes ----------
# A mode is a function (worksheet, rows, job) -> rows_written, where `rows`
# is the parsed CSV including its header row and `job` is the parsed
# metadata (most modes ignore it). Adding a mode = write the function and
# register it in MODES; the metadata contract picks it up automatically.

def mode_replace(worksheet, rows: list[list[str]], job: dict) -> int:
    """Clear the sheet, then write the full CSV, header included."""
    clear_sheet(worksheet)
    return write_rows_to_sheet(worksheet, rows, start_row=1)


def mode_append(worksheet, rows: list[list[str]], job: dict) -> int:
    """
    Write the CSV data rows after the last used row.

    The CSV header is written only when the sheet is empty; otherwise the
    sheet is assumed to already carry a header and the CSV header is skipped.

    Caveat: openpyxl's max_row counts rows that were ever touched (e.g.
    formatted but emptied), so appends land after those too. Keep target
    sheets free of stray formatting below the data.
    """
    if not rows:
        return 0
    if sheet_is_empty(worksheet):
        return write_rows_to_sheet(worksheet, rows, start_row=1)
    return write_rows_to_sheet(worksheet, rows[1:], start_row=worksheet.max_row + 1)


def mode_upsert(worksheet, rows: list[list[str]], job: dict) -> int:
    """
    Update rows whose key matches, append the rest. Returns updated + inserted.

    Rules:
      - The key is one or more columns named in the upsert-key metadata; they
        must exist in the CSV header.
      - On a non-empty sheet, the sheet header (row 1) must match the CSV
        header exactly - same names, same order. This prevents silent column
        misalignment; a mismatch fails the job loudly instead.
      - Duplicate keys in the CSV: the last row wins. Keys in the sheet are
        expected to be unique; if not, the last occurrence is the one updated.
      - Key comparison is text-based (cell values via str()), so keys should
        be text-stable columns (ids, hashes, ISO dates as text).
    """
    if not rows:
        return 0
    header, data = rows[0], rows[1:]

    missing = [k for k in job['upsert_keys'] if k not in header]
    if missing:
        raise ValueError(f'upsert-key column(s) not in CSV header: {", ".join(missing)}')

    if sheet_is_empty(worksheet):
        write_rows_to_sheet(worksheet, rows, start_row=1)
        return len(data)

    sheet_header = read_sheet_header(worksheet)
    if sheet_header != header:
        raise ValueError(f'Sheet header does not match CSV header. '
                         f'Sheet: {sheet_header} / CSV: {header}')

    key_positions = [header.index(k) for k in job['upsert_keys']]
    key_index = build_key_index(worksheet, key_positions)

    updated = inserted = 0
    next_row = worksheet.max_row + 1
    for row in data:
        row = row + [''] * (len(header) - len(row))  # pad ragged CSV rows
        key = tuple(row[pos] for pos in key_positions)
        if key in key_index:
            write_rows_to_sheet(worksheet, [row], start_row=key_index[key])
            updated += 1
        else:
            write_rows_to_sheet(worksheet, [row], start_row=next_row)
            key_index[key] = next_row
            next_row += 1
            inserted += 1
    return updated + inserted


MODES = {
    'replace': mode_replace,
    'append': mode_append,
    'upsert': mode_upsert,
}


# ---------- Write guards ----------

def enforce_write_guards(worksheet, rows: list[list[str]], job: dict) -> None:
    """
    Refuse a replace with 0 data rows (unless allow-empty=true): a failed
    source query would otherwise silently wipe the sheet. Shrinking is fine -
    smaller result sets are a normal business outcome. append and upsert
    cannot destroy data, so they are not guarded; an empty CSV there is a
    no-op that still shows up in the update Log with 0 rows.
    """
    if job['mode'] != 'replace':
        return
    new_data_rows = max(len(rows) - 1, 0)
    if new_data_rows == 0 and not job['allow_empty']:
        raise ValueError('Guard: replace with 0 data rows refused - '
                         'set allow-empty=true to override')


# ---------- Update log ----------

def append_update_log(workbook, job: dict, rows_written: int, s3_key: str) -> None:
    """Prepend one entry to the "update Log" tab (newest first, capped)."""
    worksheet = get_or_create_sheet(workbook, UPDATE_LOG_SHEET)
    if sheet_is_empty(worksheet):
        write_rows_to_sheet(worksheet, [UPDATE_LOG_HEADER], start_row=1)

    worksheet.insert_rows(2)
    entry = [
        datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        job['target_sheet'],
        job['mode'],
        rows_written,
        f"{job['source_system']}.{job['source_table']}",
        s3_key,
    ]
    write_rows_to_sheet(worksheet, [entry], start_row=2)

    entries = worksheet.max_row - 1
    if entries > UPDATE_LOG_MAX_ENTRIES:
        worksheet.delete_rows(UPDATE_LOG_MAX_ENTRIES + 2, entries - UPDATE_LOG_MAX_ENTRIES)


# ---------- Retention ----------

def trim_sheet_to_max_rows(worksheet, max_rows: int) -> int:
    """Keep the header plus the newest max_rows data rows (newest are at the
    bottom, so the oldest rows at the top are removed). Returns rows removed."""
    excess = (worksheet.max_row - 1) - max_rows
    if excess <= 0:
        return 0
    worksheet.delete_rows(2, excess)
    return excess


# ---------- Workbook editing ----------

def get_or_create_sheet(workbook, sheet_name: str):
    if sheet_name in workbook.sheetnames:
        return workbook[sheet_name]
    return workbook.create_sheet(title=sheet_name)


def clear_sheet(worksheet) -> None:
    if worksheet.max_row > 0:
        worksheet.delete_rows(1, worksheet.max_row)


def sheet_is_empty(worksheet) -> bool:
    return worksheet.max_row == 1 and all(cell.value is None for cell in worksheet[1])


def read_sheet_header(worksheet) -> list[str]:
    """Row 1 as text, trailing empty cells stripped."""
    values = ['' if cell.value is None else str(cell.value) for cell in worksheet[1]]
    while values and values[-1] == '':
        values.pop()
    return values


def build_key_index(worksheet, key_positions: list[int]) -> dict:
    """Map key tuple -> row number for every data row (rows 2..max_row)."""
    index = {}
    for row_number in range(2, worksheet.max_row + 1):
        key = tuple(
            '' if (value := worksheet.cell(row=row_number, column=pos + 1).value) is None else str(value)
            for pos in key_positions
        )
        index[key] = row_number  # duplicate keys: last occurrence wins
    return index


# CSV is typeless - csv.reader hands back str for every field, and openpyxl
# stores exactly what it is given. Without coercion every cell lands as TEXT
# (left-aligned, ignored by SUM/charts/pivots). Only these three shapes are
# converted; everything else stays text on purpose.
_NUMBER_RE = re.compile(r'^-?(0|[1-9]\d*)(\.\d+)?$')   # no leading zeros, no 1e5, no +5


def coerce_value(value):
    """Text -> int / float where it round-trips exactly, else unchanged.

    Deliberately conservative: leading zeros ('007'), exponent forms ('1e5'),
    'NaN'/'inf' and anything over 15 significant digits (Excel's float
    precision limit) stay text, so ids and hashes survive intact. DATES are
    deliberately NOT converted - they stay text, as before, which keeps the
    upsert key comparison text-to-text."""
    if not isinstance(value, str):
        return value
    text = value.strip()
    if text == '':
        return None
    if _NUMBER_RE.match(text) and len(text.lstrip('-').replace('.', '')) <= 15:
        return float(text) if '.' in text else int(text)
    return value


def write_rows_to_sheet(worksheet, rows: list[list], start_row: int) -> int:
    """Returns the number of rows written. Values are coerced out of CSV text
    into real numbers so Excel treats them as such (see coerce_value)."""
    written = 0
    for offset, row in enumerate(rows):
        for col_idx, value in enumerate(row, start=1):
            worksheet.cell(row=start_row + offset, column=col_idx,
                           value=coerce_value(value))
        written += 1
    return written


def parse_csv(csv_bytes: bytes) -> list[list[str]]:
    # utf-8-sig transparently strips a BOM if the producer left one in.
    return list(csv.reader(io.StringIO(csv_bytes.decode('utf-8-sig'))))


def apply_job_to_workbook(workbook_bytes: io.BytesIO, job: dict, csv_bytes: bytes,
                          s3_key: str) -> tuple[io.BytesIO, int, int]:
    """Guard, apply the write mode, trim retention, log the update - one save.
    Returns (new workbook bytes, rows written, rows trimmed)."""
    rows = parse_csv(csv_bytes)
    workbook = load_workbook(workbook_bytes)
    worksheet = get_or_create_sheet(workbook, job['target_sheet'])

    enforce_write_guards(worksheet, rows, job)
    rows_written = MODES[job['mode']](worksheet, rows, job)
    rows_trimmed = trim_sheet_to_max_rows(worksheet, job['max_rows']) if job['max_rows'] else 0
    append_update_log(workbook, job, rows_written, s3_key)

    output = io.BytesIO()
    workbook.save(output)
    output.seek(0)
    return output, rows_written, rows_trimmed


# ---------- Idempotency guard ----------

_idempotency_table = None


def _get_idempotency_table():
    global _idempotency_table
    if _idempotency_table is None:
        _idempotency_table = boto3.resource('dynamodb', region_name='us-east-1') \
            .Table(os.environ['IDEMPOTENCY_TABLE'])
    return _idempotency_table


def idempotency_acquire(job_id: str) -> bool:
    """Claim the job. Returns False when it was already claimed or completed."""
    table = _get_idempotency_table()
    try:
        table.put_item(
            Item={
                'job_id': job_id,
                'job_status': 'IN_PROGRESS',
                'expires_at': int(time.time()) + IDEMPOTENCY_TTL_SECONDS,
            },
            ConditionExpression='attribute_not_exists(job_id)',
        )
        return True
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return False


def idempotency_complete(job_id: str) -> None:
    _get_idempotency_table().update_item(
        Key={'job_id': job_id},
        UpdateExpression='SET job_status = :done',
        ExpressionAttributeValues={':done': 'COMPLETED'},
    )


def idempotency_release(job_id: str) -> None:
    """Drop the claim after a failure so the Lambda retry can run the job."""
    _get_idempotency_table().delete_item(Key={'job_id': job_id})


# ---------- S3 I/O ----------

def fetch_s3_object_and_metadata(bucket: str, key: str) -> tuple[bytes, dict]:
    s3 = boto3.client('s3', region_name='us-east-1')
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response['Body'].read()
    # S3 returns metadata keys lowercased
    metadata = response.get('Metadata', {})
    return body, metadata


def delete_s3_object(bucket: str, key: str) -> None:
    boto3.client('s3', region_name='us-east-1').delete_object(Bucket=bucket, Key=key)


# ---------- Lambda entry point ----------

def lambda_handler(event, context):
    results = [process_record(record) for record in event.get('Records', [])]
    return {'statusCode': 200, 'processed': results}


def process_record(record: dict) -> dict:
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
    job_id = f"{key}#{record['s3']['object'].get('eTag', '')}"
    print(f'Processing s3://{bucket}/{key}')

    if not idempotency_acquire(job_id):
        print(f'Job {job_id} already processed or in progress - skipping.')
        return {'key': key, 'skipped': 'duplicate delivery'}

    try:
        result = run_job(bucket, key)
    except Exception:
        idempotency_release(job_id)
        raise

    idempotency_complete(job_id)
    return result


def run_job(bucket: str, key: str) -> dict:
    csv_bytes, metadata = fetch_s3_object_and_metadata(bucket, key)
    job = parse_job_metadata(metadata)
    print(f'Source: {job["source_system"]}.{job["source_table"]} -> '
          f'[{job["target_site"]}] {job["target_file"]} '
          f'/ sheet "{job["target_sheet"]}" [{job["mode"]}]')

    sp_credentials = fetch_sharepoint_credentials()
    sp_context = connect_to_sharepoint(sp_credentials, SITES[job['target_site']])

    existing_workbook = download_workbook(sp_context, job['target_file'])
    updated_workbook, rows_written, rows_trimmed = apply_job_to_workbook(
        existing_workbook, job, csv_bytes, key,
    )
    upload_workbook(sp_context, job['target_file'], updated_workbook)
    print(f'Wrote {rows_written} rows to "{job["target_sheet"]}" [{job["mode"]}]'
          + (f', trimmed {rows_trimmed} old rows' if rows_trimmed else '') + '.')

    delete_s3_object(bucket, key)
    print(f'Deleted s3://{bucket}/{key}.')

    return {
        'bucket': bucket,
        'key': key,
        'file': job['target_file'],
        'sheet': job['target_sheet'],
        'mode': job['mode'],
        'rows_written': rows_written,
        'rows_trimmed': rows_trimmed,
    }
