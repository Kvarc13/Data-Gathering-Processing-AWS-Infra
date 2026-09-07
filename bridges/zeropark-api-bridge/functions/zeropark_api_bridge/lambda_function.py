"""
zeropark-api-bridge — Gateway do Zeropark Reports API (następca
ZeroparkGatewayLambda; identyczny kontrakt zdarzeń + akcja cancel_report).
Jedyna Lambda w VPC. Zero dependencies poza stdlib.

Interfejs:
  action: "generate_report"
    Input:  {"action": "generate_report", "params": {...}}
    Output: {"status": "success", "csv_url": "https://...", "report_name": "..."}
         or {"status": "pending", "status_url": "https://...", "report_name": "..."}

  action: "check_report_status"
    Input:  {"action": "check_report_status", "status_url": "https://..."}
    Output: {"status": "success", "csv_url": "https://..."}
         or {"status": "pending"}
         or {"status": "error", "message": "..."}

  action: "cancel_report"
    Input:  {"action": "cancel_report", "status_url": "https://..."}
    Output: {"status": "cancelled", "http_status": 200, "api_response": "..."}
         or {"status": "error", "message": "..."}
    (GET <status_url>/cancel — przerywa budowę raportu po stronie reports-api.)
"""
import json
import urllib.request
import urllib.error
import os
import re
import time

import logging

# Logging — structured output for CloudWatch
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


# ==============================================
# KONFIGURACJA
# ==============================================
# Adres API — VPC endpoint, ALB lub tunel Pinggy
ZEROPARK_API_URL = os.environ.get('ZEROPARK_API_URL', 'https://reports-api.zeropark.codewise.com')
# Opcjonalny Host header dla VPC Endpoint/ALB
ZEROPARK_API_HOST = os.environ.get('ZEROPARK_API_HOST', '')
# Oryginalny publiczny hostname (do podmiany URL-i statusowych)
ORIGINAL_API_HOST = "reports-api.zeropark.codewise.com"

POLL_INTERVAL_SEC = int(os.environ.get('POLL_INTERVAL_SEC', '5'))
POLL_MAX_ATTEMPTS = int(os.environ.get('POLL_MAX_ATTEMPTS', '15'))


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Blokuje automatyczne podążanie za redirectami (302).
    urllib domyślnie podąża za 302, co uniemożliwia przechwycenie Location headera."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _replace_host_in_url(url):
    """Podmienia oryginalny hostname na wewnętrzny adres VPC (jeśli skonfigurowany)."""
    return re.sub(
        r'https?://' + re.escape(ORIGINAL_API_HOST),
        ZEROPARK_API_URL,
        url
    )


def _build_request(url, data=None):
    """Buduje Request z opcjonalnym Host headerem."""
    headers = {}
    if data is not None:
        headers['Content-Type'] = 'application/json'
    if ZEROPARK_API_HOST:
        headers['Host'] = ZEROPARK_API_HOST

    req = urllib.request.Request(url, headers=headers)
    if data is not None:
        req.data = json.dumps(data).encode('utf-8')
    return req


# ==============================================
# GENEROWANIE RAPORTU
# ==============================================
def generate_report(params):
    report_name = params.get('report_name')
    date_range = params.get('date_range')
    dimensions = params.get('dimensions', [])
    metrics = params.get('metrics', [])
    filters = params.get('filters', [])
    date_from = params.get('date_from')
    date_to = params.get('date_to')
    time_aggregation = params.get('time_aggregation', 'none')

    logger.info(f"[Gateway] Generuję raport: {report_name}, date_range={date_range}, "
          f"dims={dimensions}, metrics={metrics}, filters={filters}")

    # --- 1. POST /report ---
    payload = {
        "reportName": report_name,
        "timeOffset": params.get('time_offset', 'Z'),
        "timeAggregation": time_aggregation,
        "columns": dimensions + metrics,
        "filters": filters
    }

    if date_range == "CUSTOM":
        if not date_from or not date_to:
            return {"status": "error", "message": "Dla zakresu CUSTOM wymagane są date_from i date_to."}
        payload["dateFrom"] = date_from
        payload["dateTo"] = date_to
    else:
        payload["dateRange"] = date_range

    try:
        req = _build_request(f"{ZEROPARK_API_URL}/report", data=payload)
        logger.info(f"[Gateway] POST {ZEROPARK_API_URL}/report — payload: {json.dumps(payload)[:500]}")
        response = urllib.request.urlopen(req, timeout=30)
        resp_data = json.loads(response.read().decode('utf-8'))
        logger.info(f"[Gateway] POST response: {json.dumps(resp_data)[:300]}")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode('utf-8', errors='replace')[:500]
        except Exception:
            pass
        logger.error(f"[Gateway] POST FAILED: HTTP {e.code} {e.reason} — {body}")
        return {"status": "error", "message": f"HTTP {e.code} przy POST /report: {e.reason}. Body: {body}"}
    except Exception as e:
        logger.error(f"[Gateway] POST EXCEPTION: {e}")
        return {"status": "error", "message": f"Błąd połączenia z API: {str(e)}"}

    # --- 2. Wyciągnij URL statusu ---
    status_url_original = resp_data.get("message", "")
    if not status_url_original:
        return {"status": "error", "message": "API nie zwróciło adresu statusu raportu."}

    status_url = _replace_host_in_url(status_url_original)
    logger.info(f"[Gateway] Polling URL: {status_url}")

    # --- 3. Polling ---
    opener = urllib.request.build_opener(NoRedirectHandler)

    for attempt in range(1, POLL_MAX_ATTEMPTS + 1):
        time.sleep(POLL_INTERVAL_SEC)
        try:
            poll_req = _build_request(status_url)
            poll_resp = opener.open(poll_req, timeout=15)

            content = poll_resp.read(500).decode('utf-8', errors='replace')
            if "not ready" in content.lower():
                if attempt % 6 == 0:  # co 30s loguj status
                    logger.warning(f"[Gateway] Attempt {attempt}/{POLL_MAX_ATTEMPTS}: not ready yet")
                continue

            # HTTP 200 z treścią CSV — raport serwowany inline
            # Zwracamy URL statusu jako csv_url (sam serwuje CSV)
            logger.info(f"[Gateway] Raport gotowy (inline CSV) po {attempt} próbach.")
            # Zwróć oryginalny (publiczny) URL
            return {
                "status": "success",
                "csv_url": status_url_original,
                "report_name": report_name
            }

        except urllib.error.HTTPError as e:
            if e.code == 302:
                download_url = e.headers.get("Location", "")
                if download_url:
                    logger.info(f"[Gateway] Raport gotowy (302 redirect) po {attempt} próbach: {download_url[:120]}")
                    return {
                        "status": "success",
                        "csv_url": download_url,
                        "report_name": report_name
                    }
                return {"status": "error", "message": "HTTP 302 bez nagłówka Location."}
            elif e.code == 404:
                if attempt % 6 == 0:
                    logger.error(f"[Gateway] Attempt {attempt}/{POLL_MAX_ATTEMPTS}: 404 (nie istnieje jeszcze)")
                continue
            else:
                logger.error(f"[Gateway] Polling HTTP error: {e.code} {e.reason}")
                return {"status": "error", "message": f"HTTP {e.code} przy pollowaniu: {e.reason}"}
        except Exception as e:
            logger.error(f"[Gateway] Polling exception: {e}")
            return {"status": "error", "message": f"Błąd pollowania: {str(e)}"}

    logger.warning(f"[Gateway] TIMEOUT po {POLL_MAX_ATTEMPTS} próbach — przekazuję do ReportChecker.")
    return {
        "status": "pending",
        "status_url": status_url_original,
        "report_name": report_name,
        "message": "Raport nie był gotowy po 2.5 min — przekazano do asynchronicznego checkera."
    }


# ==============================================
# JEDNORAZOWE SPRAWDZENIE STATUSU (dla ReportCheckerLambda)
# ==============================================
def check_report_status(status_url):
    """Jednorazowe sprawdzenie czy raport jest gotowy. Bez polling loop."""
    internal_url = _replace_host_in_url(status_url)
    logger.info(f"[Gateway] check_report_status: {internal_url[:120]}")

    opener = urllib.request.build_opener(NoRedirectHandler)
    try:
        poll_req = _build_request(internal_url)
        poll_resp = opener.open(poll_req, timeout=15)
        content = poll_resp.read(500).decode('utf-8', errors='replace')

        if "not ready" in content.lower():
            logger.warning("[Gateway] check_report_status: not ready yet")
            return {"status": "pending"}

        logger.info("[Gateway] check_report_status: ready (inline CSV)")
        return {"status": "success", "csv_url": status_url}

    except urllib.error.HTTPError as e:
        if e.code == 302:
            download_url = e.headers.get("Location", "")
            if download_url:
                logger.info(f"[Gateway] check_report_status: ready (302) → {download_url[:80]}")
                return {"status": "success", "csv_url": download_url}
            return {"status": "error", "message": "HTTP 302 bez nagłówka Location."}
        elif e.code == 404:
            logger.error("[Gateway] check_report_status: 404 (nie istnieje jeszcze)")
            return {"status": "pending"}
        else:
            logger.error(f"[Gateway] check_report_status HTTP error: {e.code} {e.reason}")
            return {"status": "error", "message": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        logger.error(f"[Gateway] check_report_status exception: {e}")
        return {"status": "error", "message": f"Błąd sprawdzania: {str(e)}"}


# ==============================================
# ANULOWANIE BUDOWY RAPORTU
# ==============================================
def cancel_report(status_url):
    """GET <status_url>/cancel — przerywa budowę raportu. Bez retry.

    Defensywnie: każdy 2xx traktujemy jako anulowanie (format odpowiedzi API
    nie jest udokumentowany); 4xx/5xx wraca jako error z treścią — anulowanie
    raportu już ukończonego może tak wyglądać i to nie jest awaria.
    """
    internal_url = _replace_host_in_url(status_url) + "/cancel"
    logger.info(f"[Gateway] cancel_report: {internal_url[:120]}")

    opener = urllib.request.build_opener(NoRedirectHandler)
    try:
        req = _build_request(internal_url)
        resp = opener.open(req, timeout=15)
        content = resp.read(500).decode('utf-8', errors='replace')
        logger.info(f"[Gateway] cancel_report: HTTP {resp.status} — {content[:120]}")
        return {"status": "cancelled", "http_status": resp.status,
                "api_response": content[:200]}
    except urllib.error.HTTPError as e:
        logger.error(f"[Gateway] cancel_report HTTP error: {e.code} {e.reason}")
        return {"status": "error", "message": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        logger.error(f"[Gateway] cancel_report exception: {e}")
        return {"status": "error", "message": f"Błąd anulowania: {str(e)}"}


# ==============================================
# HANDLER
# ==============================================
def lambda_handler(event, context):
    logger.info(f"=== ZeroparkGateway START === RequestId: {context.aws_request_id}")
    action = event.get("action")

    if action == "generate_report":
        params = event.get("params", {})
        return generate_report(params)

    if action == "check_report_status":
        status_url = event.get("status_url", "")
        if not status_url:
            return {"status": "error", "message": "Brak status_url."}
        return check_report_status(status_url)

    if action == "cancel_report":
        status_url = event.get("status_url", "")
        if not status_url:
            return {"status": "error", "message": "Brak status_url."}
        return cancel_report(status_url)

    logger.error(f"[Gateway] Nieznana akcja: {action}")
    return {"status": "error", "message": f"Nieznana akcja: {action}"}