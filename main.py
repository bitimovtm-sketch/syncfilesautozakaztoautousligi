"""
Bitrix24 deal files sync: Portal 1 -> Portal 2.

Trigger: HTTP POST/GET to /sync with deal_id (from Bitrix24 robot).
Logic: read deal from P1, find matching deal on P2 by VIN, push file fields.
"""
import os
import base64
import logging
import re
import threading
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


# ---------- Bitrix24 helpers ----------
def bx(webhook, method, payload):
    """POST to a Bitrix24 REST method; raise on error."""
    r = requests.post(f'{webhook}/{method}.json', json=payload, timeout=55)
    r.raise_for_status()
    data = r.json()
    if 'error' in data:
        raise RuntimeError(f'{method}: {data.get("error_description") or data}')
    return data.get('result')


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


def download(url):
    """Download file by its urlMachine. Returns (filename, base64_str)."""
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
            return
        log.info('VIN=%r', vin)
        p2_id = find_deal_p2(vin)
        if not p2_id:
            log.info('no deal on portal 2 with VIN=%r, skip', vin)
            return
        log.info('matched portal 2 deal id=%s', p2_id)
        fields = build_update_payload(deal)
        if not fields:
            log.info('all file fields on portal 1 are empty, nothing to push')
            return
        log.info('updating portal 2, fields: %s', list(fields.keys()))
        bx(PORTAL_2_WEBHOOK, 'crm.item.update', {
            'entityTypeId': ENTITY_TYPE_ID,
            'id': p2_id,
            'fields': fields,
        })
        log.info('=== sync OK: P1 deal %s -> P2 deal %s ===', deal_id, p2_id)
    except Exception as e:
        log.exception('sync failed for deal %s: %s', deal_id, e)


# ---------- HTTP ----------
app = Flask(__name__)


def _extract_deal_id():
    """Pull deal_id from query, form, or JSON. Handles 'DEAL_123' robot format."""
    raw = request.values.get('deal_id') or request.values.get('id')
    if not raw:
        j = request.get_json(silent=True) or {}
        raw = j.get('deal_id') or j.get('id')
    if not raw:
        # Bitrix24 robot might pass document_id[2]=DEAL_12345
        for k, v in request.form.items():
            if k.startswith('document_id') and isinstance(v, str) and v.startswith('DEAL_'):
                raw = v[5:]
                break
    if isinstance(raw, str) and raw.startswith('DEAL_'):
        raw = raw[5:]
    return str(raw).strip() if raw else None


@app.route('/sync', methods=['POST', 'GET'])
def sync_route():
    deal_id = _extract_deal_id()
    if not deal_id or not deal_id.isdigit():
        log.warning('no deal_id: args=%s form=%s', dict(request.args), dict(request.form))
        return jsonify({'error': 'missing or invalid deal_id'}), 400
    # Respond to the robot immediately; sync runs in background.
    threading.Thread(target=do_sync, args=(deal_id,), daemon=True).start()
    return jsonify({'ok': True, 'deal_id': deal_id}), 200


@app.route('/', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
