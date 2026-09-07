"""
reports_utils — vendored subset of the legacy common-utils-layer
(reports_bot_utils.py), exactly the three helpers this function imports:

  get_secret(name)                       — Secrets Manager, container-lifetime cache
  generate_download_url(s3_key, expires) — CloudFront signed URL / S3 presigned fallback
  sanitize_thread_ts(thread_ts)          — safe S3 key segment

Function bodies are UNCHANGED from the layer source — sanitize_thread_ts
shapes S3 paths and the CloudFront signer must keep signing exactly as
production does, so nothing here is "improved". The unused layer helpers
(post_to_slack, invoke_lambda) are deliberately not carried over.

TWIN NOTE: an identical copy lives in the other zeropark bridge function
(upload <-> query) — same rule as the SharePoint bridge's shared code: both
copies live in this repo area, keep them in sync.

Deps: rsa>=4.9 (requirements.txt; the deprecated rsa-signer-layer stays dead).
"""
import os
from datetime import datetime, timezone, timedelta

import boto3
import rsa
from botocore.config import Config
from botocore.signers import CloudFrontSigner

import logging
logger = logging.getLogger(__name__)


# ==============================================
# SECRETS MANAGER
# ==============================================
_sm_client = boto3.client('secretsmanager')
_secrets_cache: dict[str, str] = {}


def get_secret(name: str) -> str:
    """Retrieve secret from AWS Secrets Manager. Cached for container lifetime."""
    if name not in _secrets_cache:
        _secrets_cache[name] = _sm_client.get_secret_value(
            SecretId=name)['SecretString']
    return _secrets_cache[name]


# ==============================================
# CLOUDFRONT SIGNED URL
# ==============================================
# S3 client for presigned URL fallback (used when CloudFront is not configured)
_s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))

# CloudFront private key cached for container lifetime
_cf_private_key = None


def _get_cf_signer(key_pair_id: str) -> CloudFrontSigner:
    """Create CloudFront URL signer. Private key cached for container lifetime."""
    global _cf_private_key
    if _cf_private_key is None:
        pem = get_secret('zeropark-bot/CLOUDFRONT_PRIVATE_KEY')
        _cf_private_key = rsa.PrivateKey.load_pkcs1(pem.encode())

    def rsa_signer(message: bytes) -> bytes:
        return rsa.sign(message, _cf_private_key, 'SHA-1')

    return CloudFrontSigner(key_pair_id, rsa_signer)


def generate_download_url(s3_key: str, expires_in: int | None = None) -> str:
    """Generate download URL. Uses CloudFront signed URL if configured, S3 presigned as fallback.

    Reads configuration from environment variables at call time:
      CLOUDFRONT_DOMAIN      — CloudFront distribution domain (empty = use S3 fallback)
      CLOUDFRONT_KEY_PAIR_ID — CloudFront key pair ID
      PRESIGNED_URL_EXPIRY   — URL validity in seconds (default: 86400 = 24h)
      S3_BUCKET              — S3 bucket name (used for S3 presigned fallback)
    """
    cf_domain = os.environ.get('CLOUDFRONT_DOMAIN', '')
    key_pair_id = os.environ.get('CLOUDFRONT_KEY_PAIR_ID', '')
    expiry = expires_in if expires_in is not None else int(
        os.environ.get('PRESIGNED_URL_EXPIRY', '86400')
    )

    if cf_domain and key_pair_id:
        url = f"https://{cf_domain}/{s3_key}"
        expire_date = datetime.now(timezone.utc) + timedelta(seconds=expiry)
        return _get_cf_signer(key_pair_id).generate_presigned_url(
            url, date_less_than=expire_date
        )
    else:
        s3_bucket = os.environ.get('S3_BUCKET', 'zeropark-reports-datalake')
        return _s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': s3_bucket, 'Key': s3_key},
            ExpiresIn=expiry
        )


# ==============================================
# S3 PATH HELPERS
# ==============================================
def sanitize_thread_ts(thread_ts: str) -> str:
    """Sanitize thread_ts for use in S3 keys: '1774631319.952000' → '1774631319_952000'."""
    return thread_ts.replace('.', '_') if thread_ts else 'no_thread'
