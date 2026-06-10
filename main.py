"""
Bitrix24 deal files sync: Portal 1 -> Portal 2.

Portal 1 (source, read + file download): OAuth local application.
  Reason: CRM "File"-type fields can only be downloaded via
  crm.controller.item.getFile, which rejects webhook auth (401).
Portal 2 (destination, search + write): incoming webhook (works fine).

Trigger: HTTP POST/GET to /sync with deal_id (robot / outgoing webhook on P1).

Concurrency: FIFO queue + single worker + global ~2 req/sec throttle + retry.
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
# Portal 1: OAuth local application
P1_DOMAIN = os.environ.get('P1_DOMAIN', 'autozakaz.bitrix24.ru')
P1_CLIENT_ID = os.environ['P1_CLIENT_ID']          # local app client_id (app.xxxxx)
P1_CLIENT_SECRET = os.environ['P1_CLIENT_SECRET']  # local app client_secret
# Initial refresh token: filled in after first install via /install endpoint
P1_REFRESH_TOKEN = os.environ.get('P1_REFRESH_TOKEN', '')

# Portal 2: incoming webhook (write-only operations work fine via webhook)
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
_MIN_INTERVAL = 0.5  # seconds between any two outbound Bitrix24 calls
_rate_lock = threading.Lock()
_last_call_at = 0.0


def _throttle():
    global _last_call_at
    with _rate_lock:
        wait = _MIN_INTERVAL - (time.monotonic() - _last_call_at)
        if wait > 0:
            time.sleep(wait)
        _last_call_at = time.monotonic()


# ---------- OAuth token manager (Portal 1) ----------
_token_lock = threading.Lock()
_token = {
    'access_token': None,
    'refresh_token': P1_REFRESH_TOKEN or None,
    'expires_at': 0.0,  # monotonic deadline
}


def _refresh_p1_token():
    """Exchange refresh_token for a fresh access_token. Must hold _token_lock."""
    rt = _token['refresh_token']
    if not rt:
        raise RuntimeError(
            'No P1 refresh token. Install the local app (it will hit /install), '
            'or set P1_REFRESH_TOKEN env var.'
        )
    _throttle()
    r = requests.get(
        'https://oauth.bitrix.info/oauth/token/',
        params={
            'grant_type': 'refresh_token',
            'client_id': P1_CLIENT_ID,
            'client_secret': P1_CLIENT_SECRET,
            'refresh_token': rt,
        },
        timeout=30,
    )
    data = r.json()
    if 'access_token' not in data:
        raise RuntimeError(f'OAuth refresh failed: {data}')
    _token['access_token'] = data['access_token']
    _token['refresh_token'] = data.get('refresh_token') or rt
    # expires_in is usually 3600; renew 5 min early
    _token['expires_at'] = time.monotonic() + int(data.get('expires_in', 3600)) - 300
    log.info('P1 access token refreshed, valid ~%s min', int(data.get('expires_in', 3600)) / 60)
    log.info('P1 NEW refresh_token (save to Railway env P1_REFRESH_TOKEN to survive restarts): %s',
             _token['refresh_token'])


def get_p1_token():
    """Return a valid access token for portal 1, refreshing if needed."""
    with _token_lock:
        if not _token['access_token'] or time.monotonic() >= _token['expires_at']:
            _refresh_p1_token()
        return _token['access_token']


def set_p1_tokens(access_token, refresh_token, expires_in=3600):
    """Store tokens received from app installation (/install)."""
    with _token_lock:
        _token['access_token'] = access_token
        _token['refresh_token'] = refresh_token
        _token['expires_at'] = time.monotonic() + int(expires_in) - 300


# ---------- Bitrix24 REST helpers ----------
def _bx_request(post_url, payload):
    """Single POST with retry on transient errors. Returns parsed result or raises."""
    backoffs = [2, 4, 8]
    last_err = None
    for attempt in range(len(backoffs) + 1):
        _throttle()
        retriable = False
        try:
            r = requests.post(post_url, json=payload, timeout=55)
            if r.status_code in (429, 503):
                last_err = f'HTTP {r.status_code}'
                retriable = True
            else:
                data = r.json()
                err = data.get('error')
                if err in ('QUERY_LIMIT_EXCEEDED', 'OPERATION_TIME_LIMIT'):
                    last_err = err
                    retriable = True
                elif err:
                    raise RuntimeError(f'{data.get("error_description") or data}')
                else:
                    return data.get('result')
        except (requests.RequestException, ValueError) as e:
            last_err = str(e)
            retriable = True
        if not retriable or attempt >= len(backoffs):
            break
        wait = backoffs[attempt]
        log.warning('transient error (%s); retry in %ss', last_err, wait)
        time.sleep(wait)
    raise RuntimeError(last_err or 'unknown error')


def bx_p1(method, payload):
    """Call portal 1 REST method with OAuth. Retries once on expired_token."""
    token = get_p1_token()
    url = f'https://{P1_DOMAIN}/rest/{method}.json'
    try:
        return _bx_request(url, {**payload, 'auth': token})
    except RuntimeError as e:
        if 'expired_token' in str(e).lower() or 'invalid_token' in str(e).lower():
            log.info('P1 token rejected, forcing refresh and retrying once')
            with _token_lock:
                _token['access_token'] = None
            token = get_p1_token()
            return _bx_request(url, {**payload, 'auth': token})
        raise RuntimeError(f'{method}: {e}')


def bx_p2(method, payload):
    """Call portal 2 REST method via incoming webhook."""
    try:
        return _bx_request(f'{PORTAL_2_WEBHOOK}/{method}.json', payload)
    except RuntimeError as e:
        raise RuntimeError(f'{method}: {e}')


def get_deal_p1(deal_id):
    return bx_p1('crm.item.get', {
        'entityTypeId': ENTITY_TYPE_ID,
        'id': int(deal_id),
    })['item']


def find_deal_p2(vin):
    res = bx_p2('crm.item.list', {
        'entityTypeId': ENTITY_TYPE_ID,
        'filter': {VIN_P2_C: vin},
        'select': ['id'],
    })
    items = res.get('items') or []
    return items[0]['id'] if items else None


# ---------- File download (Portal 1, OAuth context) ----------
def extract_filename(resp, fallback='file.bin'):
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


def download_file(file_meta):
    """
    Download a file from a P1 CRM file field.
    In OAuth context urlMachine comes as:
      https://{domain}/rest/crm.controller.item.getFile.json?auth=...&token=...
    The embedded auth may belong to a stale token, so we swap in our current one.
    """
    url = file_meta.get('urlMachine') or file_meta.get('url')
    if not url:
        raise RuntimeError(f'file {file_meta.get("id")}: no download URL in response')

    # Replace/insert auth= with our current access token
    token = get_p1_token()
    if 'auth=' in url:
        url = re.sub(r'auth=[^&]*', f'auth={token}', url)
    else:
        sep = '&' if '?' in url else '?'
        url = f'{url}{sep}auth={token}'

    _throttle()
    r = requests.get(url, timeout=55, allow_redirects=True)
    if not r.ok:
        log.warning('download HTTP %s; body[:300]=%r', r.status_code, r.text[:300] if r.text else '')
        r.raise_for_status()
    return extract_filename(r), base64.b64encode(r.content).decode('ascii')


def build_update_payload(deal_p1):
    """
    {p2_field_camel: [[name, b64], ...]} for each non-empty P1 file field.
    Empty P1 fields are omitted -> P2 keeps its current content there.
    """
    fields = {}
    for p1c, p2c in FILE_FIELDS_C.items():
        files = deal_p1.get(p1c) or []
        if not files:
            continue
        prepared = []
        for f in files:
            log.info('Downloading file id=%s for field %s', f.get('id'), p1c)
            name, b64 = download_file(f)
            prepared.append([name, b64])
        fields[p2c] = prepared
    return fields


# ---------- Stats ----------
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
    with _stats_lock:
        for k, v in inc.items():
            _stats[k] = _stats.get(k, 0) + v
        _stats['last_event_at'] = dt.datetime.utcnow().isoformat() + 'Z'
        _stats['last_event_msg'] = msg


def do_sync(deal_id):
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
        bx_p2('crm.item.update', {
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
_task_queue: "queue.Queue[str]" = queue.Queue()
_pending: set = set()
_pending_lock = threading.Lock()


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
    with _pending_lock:
        if deal_id in _pending:
            return False
        _pending.add(deal_id)
    _task_queue.put(deal_id)
    return True


_worker_thread = threading.Thread(target=_worker, daemon=True, name='sync_worker')
_worker_thread.start()


# ---------- HTTP ----------
app = Flask(__name__)


def _extract_deal_id():
    raw = request.values.get('deal_id') or request.values.get('id')
    if not raw:
        j = request.get_json(silent=True) or {}
        raw = j.get('deal_id') or j.get('id')
    if not raw:
        raw = request.values.get('data[FIELDS][ID]')
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
    log.info(
        'incoming /sync: method=%s args=%s form_keys=%s has_json=%s',
        request.method, dict(request.args), list(request.form.keys()),
        request.get_json(silent=True) is not None,
    )
    deal_id = _extract_deal_id()
    if not deal_id or not deal_id.isdigit():
        log.warning('no deal_id extracted. args=%s form=%s json=%s',
                    dict(request.args), dict(request.form), request.get_json(silent=True))
        return jsonify({'error': 'missing or invalid deal_id'}), 400
    added = enqueue(deal_id)
    if not added:
        log.info('deal %s already pending, deduplicated', deal_id)
    return jsonify({'ok': True, 'deal_id': deal_id, 'queued': added,
                    'queue_size': _task_queue.qsize()}), 200


@app.route('/install', methods=['POST', 'GET'])
def install():
    """
    Installation handler for the P1 local application.
    Bitrix24 POSTs AUTH_ID (access token) and REFRESH_ID (refresh token) here
    when the app is installed or re-installed.
    """
    auth_id = request.values.get('AUTH_ID')
    refresh_id = request.values.get('REFRESH_ID')
    expires_in = request.values.get('AUTH_EXPIRES', 3600)
    log.info('/install hit: has AUTH_ID=%s has REFRESH_ID=%s form_keys=%s',
             bool(auth_id), bool(refresh_id), list(request.form.keys()))
    if auth_id and refresh_id:
        set_p1_tokens(auth_id, refresh_id, expires_in)
        log.info('P1 tokens stored from /install.')
        log.info('SAVE THIS refresh_token to Railway env P1_REFRESH_TOKEN: %s', refresh_id)
        # Minimal page + BX24.installFinish so Bitrix marks installation complete
        return (
            '<!DOCTYPE html><html><head>'
            '<script src="//api.bitrix24.com/api/v1/"></script>'
            '<script>BX24.init(function(){ BX24.installFinish(); });</script>'
            '</head><body>App installed. Tokens captured — check Railway logs '
            'and save P1_REFRESH_TOKEN.</body></html>',
            200,
            {'Content-Type': 'text/html'},
        )
    return jsonify({'ok': True, 'note': 'no tokens in request'}), 200


@app.route('/status', methods=['GET'])
def status():
    with _pending_lock:
        pending_list = sorted(_pending)
    with _stats_lock:
        s = dict(_stats)
    with _token_lock:
        has_access = bool(_token['access_token'])
        has_refresh = bool(_token['refresh_token'])
    return jsonify({
        'worker_alive': _worker_thread.is_alive(),
        'queue_size': _task_queue.qsize(),
        'pending_deals': pending_list,
        'p1_oauth': {'has_access_token': has_access, 'has_refresh_token': has_refresh},
        **s,
    }), 200


@app.route('/', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
