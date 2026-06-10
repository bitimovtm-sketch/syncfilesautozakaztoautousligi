"""
Bitrix24 deal files sync: Portal 1 -> Portal 2.

Trigger: HTTP POST/GET to /sync with deal_id (from Bitrix24 robot).
Logic: read deal from P1, find matching deal on P2 by VIN, push file fields.

Concurrency model:
- One background worker thread drains a FIFO queue of deal_ids.
- Pending deal_ids are deduplicated (a burst of robot calls for the same
  deal collapses to one sync).
- A global throttle limits ALL outgoing Bitrix24 calls (REST + file
  downloads) to ~2/sec to stay under the platform rate limit.
- QUERY_LIMIT_EXCEEDED / HTTP 503 trigger exponential backoff retry.
"""
import os
import base64
import datetime as dt
import logging
import queue
import re
import threading
import time
from urllib.parse import unquote

import requests
from flask import Flask, request, jsonify

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger(__name__)

# ---------- Configuration ----------
PORTAL_1_WEBHOOK = os.environ['PORTAL_1_WEBHOOK'].rstrip('/')
PORTAL_2_WEBHOOK = os.environ['PORTAL_2_WEBHOOK'].rstrip('/')

ENTITY_TYPE_ID = 2  # deals

# VIN field codes
VIN_P1 = 'UF_CRM_1673397434675'
VIN_P2 = 'UF_CRM_1770366284'

# Portal 1 file field  ->  Portal 2 file field
FILE_FIELDS = {
    'UF_CRM_1756163221881': 'UF_CRM_1781046413',  # ПТД
    'UF_CRM_1771305756180': 'UF_CRM_1781046439',  # ЭПТС
    'UF_CRM_1771305801806': 'UF_CRM_1781046452',  # СБКТС
    'UF_CRM_1772147136':    'UF_CRM_1781046463',  # Коносамент
    'UF_CRM_1688341306463': 'UF_CRM_1781046476',  # Чек об оплате
}


def uf_camel(uf):
    """UF_CRM_1234567890 -> ufCrm_1234567890 (camelCase form for crm.item.* API)."""
    return 'ufCrm_' + uf[7:] if uf.startswith('UF_CRM_') else uf


VIN_P1_C = uf_camel(VIN_P1)
VIN_P2_C = uf_camel(VIN_P2)
FILE_FIELDS_C = {uf_camel(k): uf_camel(v) for k, v in FILE_FIELDS.items()}


# ---------- Rate limiting ----------
# Bitrix24 cloud allows ~2 REST calls/sec on average.
# A single global lock + timestamp gives us a strict cap.
_MIN_INTERVAL = 0.5  # seconds between any two outbound Bitrix24 calls
_rate_lock = threading.Lock()
_last_call_at = 0.0


def _throttle():
    """Block until at least _MIN_INTERVAL has passed since the last call."""
    global _last_call_at
    with _rate_lock:
        wait = _MIN_INTERVAL - (time.monotonic() - _last_call_at)
        if wait > 0:
            time.sleep(wait)
        _last_call_at = time.monotonic()


# ---------- Bitrix24 helpers ----------
def bx(webhook, method, payload):
    """POST to a Bitrix24 REST method with throttling and retry on rate limit."""
    backoffs = [2, 4, 8]  # extra wait on retries
    last_err = None
    for attempt in range(len(backoffs) + 1):
        _throttle()
        try:
            r = requests.post(f'{webhook}/{method}.json', json=payload, timeout=55)
            if r.status_code == 503:
                last_err = f'HTTP 503 from {method}'
            else:
                r.raise_for_status()
                data = r.json()
                err = data.get('error')
                if err in ('QUERY_LIMIT_EXCEEDED', 'OPERATION_TIME_LIMIT'):
                    last_err = f'{method}: {err}'
                elif err:
                    raise RuntimeError(f'{method}: {data.get("error_description") or data}')
                else:
                    return data.get('result')
        except requests.RequestException as e:
            last_err = f'{method}: {e}'
        if attempt < len(backoffs):
            wait = backoffs[attempt]
            log.warning('rate-limited or transient error (%s); retry in %ss', last_err, wait)
            time.sleep(wait)
    raise RuntimeError(f'gave up after retries: {last_err}')


def get_deal_p1(deal_id):
    """Fetch deal from portal 1 (returns dict)."""
    return bx(PORTAL_1_WEBHOOK, 'crm.item.get', {
        'entityTypeId': ENTITY_TYPE_ID,
        'id': int(deal_id),
    })['item']


def find_deal_p2(vin):
    """Find first deal on portal 2 by VIN, or None."""
    res = bx(PORTAL_2_WEBHOOK, 'crm.item.list', {
        'entityTypeId': ENTITY_TYPE_ID,
        'filter': {VIN_P2_C: vin},
        'select': ['id'],
    })
    items = res.get('items') or []
    return items[0]['id'] if items else None


def extract_filename(resp, fallback='file.bin'):
    """Pull filename out of Content-Disposition header."""
    cd = resp.headers.get('Content-Disposition', '')
    m = re.search(r"filename\*=UTF-8''([^;]+)", cd, re.IGNORECASE)
    if m:
        return unquote(m.group(1))
    m = re.search(r'filename="([^"]+)"', cd)
    if m:
        return m.group(1)
    m = re.search(r'filename=([^;]+)', cd)
    if m:
        return m.group(1).strip()
    return fallback


def _normalize_file_url(url):
    """
    Workaround for Bitrix24 webhook-context urlMachine: sometimes it comes back
    as ".../crm.controller.item.getFile/?token=..." (trailing slash, no extension)
    and 404s. The working form is ".../crm.controller.item.getFile.json?token=...".
    Add .json before the query string if it's missing.
    """
    if not url:
        return url
    path, sep, query = url.partition('?')
    path_stripped = path.rstrip('/')
    if not path_stripped.endswith('.json'):
        path_stripped += '.json'
    return path_stripped + sep + query


def download(url):
    """Download file by its urlMachine. Returns (filename, base64_str)."""
    _throttle()  # file downloads also hit the portal — keep them in the same budget
    url = _normalize_file_url(url)
    r = requests.get(url, timeout=55, allow_redirects=True)
    r.raise_for_status()
    return extract_filename(r), base64.b64encode(r.content).decode('ascii')


def build_update_payload(deal_p1):
    """
    For each file field with content on portal 1, download files and prep base64 payload.
    Returns {p2_field_camel: [[name, b64], ...]} — empty fields on P1 are omitted,
    so portal 2 keeps whatever it already has there.
    """
    fields = {}
    for p1c, p2c in FILE_FIELDS_C.items():
        files = deal_p1.get(p1c) or []
        if not files:
            continue
        prepared = []
        for f in files:
            url = f.get('urlMachine') or f.get('url')
            log.info('Downloading file id=%s from portal 1', f.get('id'))
            name, b64 = download(url)
            prepared.append([name, b64])
        fields[p2c] = prepared
    return fields


def do_sync(deal_id):
    """Main routine; logs and swallows all exceptions."""
    try:
        log.info('=== sync start P1 deal=%s ===', deal_id)
        deal = get_deal_p1(deal_id)
        vin = deal.get(VIN_P1_C)
        if not vin:
            log.warning('deal %s has no VIN, skip', deal_id)
            _note(f'deal {deal_id} no VIN', skipped_no_vin=1)
            return
        log.info('VIN=%r', vin)
        p2_id = find_deal_p2(vin)
        if not p2_id:
            log.info('no deal on portal 2 with VIN=%r, skip', vin)
            _note(f'deal {deal_id}: no P2 deal for VIN={vin}', skipped_no_p2_deal=1)
            return
        log.info('matched portal 2 deal id=%s', p2_id)
        fields = build_update_payload(deal)
        if not fields:
            log.info('all file fields on portal 1 are empty, nothing to push')
            _note(f'deal {deal_id}: all P1 file fields empty', completed=1)
            return
        log.info('updating portal 2, fields: %s', list(fields.keys()))
        bx(PORTAL_2_WEBHOOK, 'crm.item.update', {
            'entityTypeId': ENTITY_TYPE_ID,
            'id': p2_id,
            'fields': fields,
        })
        log.info('=== sync OK: P1 deal %s -> P2 deal %s ===', deal_id, p2_id)
        _note(f'OK: P1 {deal_id} -> P2 {p2_id}', completed=1)
    except Exception as e:
        log.exception('sync failed for deal %s: %s', deal_id, e)
        _note(f'FAIL deal {deal_id}: {e}', failed=1)


# ---------- Task queue ----------
# Single worker drains the queue serially. Throttling above already
# enforces the rate limit, but a single worker also prevents bursts
# from interleaving and makes logs readable.
_task_queue: "queue.Queue[str]" = queue.Queue()
_pending: set = set()
_pending_lock = threading.Lock()

# Stats for /status
_stats_lock = threading.Lock()
_stats = {
    'completed': 0,
    'failed': 0,
    'skipped_no_vin': 0,
    'skipped_no_p2_deal': 0,
    'last_event_at': None,
    'last_event_msg': None,
}


def _note(msg, **inc):
    """Update stats counters and last-event marker."""
    with _stats_lock:
        for k, v in inc.items():
            _stats[k] = _stats.get(k, 0) + v
        _stats['last_event_at'] = dt.datetime.utcnow().isoformat() + 'Z'
        _stats['last_event_msg'] = msg


def _worker():
    log.info('queue worker started')
    while True:
        deal_id = _task_queue.get()
        try:
            with _pending_lock:
                _pending.discard(deal_id)
            do_sync(deal_id)
        except Exception:
            log.exception('worker crashed processing deal %s', deal_id)
            _note(f'worker crash on deal {deal_id}', failed=1)
        finally:
            _task_queue.task_done()


def enqueue(deal_id):
    """Add deal to queue unless it's already pending. Returns True if added."""
    with _pending_lock:
        if deal_id in _pending:
            return False
        _pending.add(deal_id)
    _task_queue.put(deal_id)
    return True


# Spin up the worker once, on import.
_worker_thread = threading.Thread(target=_worker, daemon=True, name='sync_worker')
_worker_thread.start()


# ---------- HTTP ----------
app = Flask(__name__)


def _extract_deal_id():
    """
    Pull deal_id from query, form, or JSON. Handles common Bitrix24 trigger shapes:
    - explicit ?deal_id=123 / form `deal_id=123` (robot with URL template)
    - JSON body {"deal_id": 123}
    - outgoing webhook ONCRMDEAL{ADD,UPDATE,DELETE}: data[FIELDS][ID]=123
    - workflow robot: document_id[2]=DEAL_12345
    """
    # 1. Direct deal_id / id in query or form
    raw = request.values.get('deal_id') or request.values.get('id')

    # 2. JSON body
    if not raw:
        j = request.get_json(silent=True) or {}
        raw = j.get('deal_id') or j.get('id')

    # 3. Outgoing webhook ONCRMDEALUPDATE etc.: data[FIELDS][ID]=12345
    if not raw:
        raw = request.values.get('data[FIELDS][ID]')

    # 4. Robot workflow: document_id[2]=DEAL_12345
    if not raw:
        for k, v in request.form.items():
            if k.startswith('document_id') and isinstance(v, str) and v.startswith('DEAL_'):
                raw = v[5:]
                break

    if isinstance(raw, str) and raw.startswith('DEAL_'):
        raw = raw[5:]
    return str(raw).strip() if raw else None


@app.route('/sync', methods=['POST', 'GET'])
def sync_route():
    # Log shape of every inbound request so we can see how Bitrix24 is hitting us.
    log.info(
        'incoming /sync: method=%s args=%s form_keys=%s has_json=%s',
        request.method,
        dict(request.args),
        list(request.form.keys()),
        request.get_json(silent=True) is not None,
    )
    deal_id = _extract_deal_id()
    if not deal_id or not deal_id.isdigit():
        log.warning(
            'no deal_id extracted. args=%s form=%s json=%s',
            dict(request.args), dict(request.form), request.get_json(silent=True),
        )
        return jsonify({'error': 'missing or invalid deal_id'}), 400
    added = enqueue(deal_id)
    if not added:
        log.info('deal %s already pending, deduplicated', deal_id)
    return jsonify({'ok': True, 'deal_id': deal_id, 'queued': added, 'queue_size': _task_queue.qsize()}), 200


@app.route('/status', methods=['GET'])
def status():
    with _pending_lock:
        pending_list = sorted(_pending)
    with _stats_lock:
        s = dict(_stats)
    return jsonify({
        'worker_alive': _worker_thread.is_alive(),
        'queue_size': _task_queue.qsize(),
        'pending_deals': pending_list,
        **s,
    }), 200


@app.route('/', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
