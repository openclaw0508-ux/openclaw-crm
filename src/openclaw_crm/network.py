from __future__ import annotations

from datetime import datetime

from openclaw_crm.sheets import read_sheet, append_sheet, update_sheet
from openclaw_crm.config import get_spreadsheet_id
from openclaw_crm.pipeline import _parse_rows, create_deal

SIGNALS_RANGE = "'Network Signals'!A:F"
PIPELINE_RANGE = "Pipeline!A:U"

SIGNAL_HEADERS = [
    "Timestamp", "Source Client", "Channel",
    "Signal Text", "Mentioned Company", "Status",
]


def add_signal(signal: dict) -> dict:
    sid = get_spreadsheet_id()
    row = [
        signal.get("timestamp", datetime.now().isoformat()),
        signal.get("source_client", ""),
        signal.get("channel", ""),
        signal.get("signal_text", ""),
        signal.get("mentioned_company", ""),
        "new",
    ]
    result = append_sheet(sid, SIGNALS_RANGE, [row])
    return {"ok": result.success, "status": "new"}


def get_pending_signals() -> list[dict]:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, SIGNALS_RANGE)
    rows = _parse_rows(r)
    return [r for r in rows if r.get("Status", "").lower() == "new"]


def _get_all_signals() -> tuple[list[list[str]], list[str]]:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, SIGNALS_RANGE)
    if not r.success or not r.data:
        return [], []
    rows = r.data.get("values", [])
    if len(rows) < 2:
        return [], rows[0] if rows else SIGNAL_HEADERS
    return rows[1:], rows[0]


def promote_signal(signal_row: int, deal_overrides: dict | None = None) -> dict:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, SIGNALS_RANGE)
    if not r.success or not r.data:
        return {"ok": False, "error": "Cannot read signals"}
    rows = r.data.get("values", [])
    if signal_row < 2 or signal_row > len(rows):
        return {"ok": False, "error": f"Signal row {signal_row} out of range"}
    headers = rows[0]
    signal = dict(zip(headers, rows[signal_row - 1]))
    if signal.get("Status", "").lower() == "promoted":
        return {"ok": False, "error": f"Signal row {signal_row} already promoted"}
    deal = {
        "client": signal.get("Mentioned Company", ""),
        "source": "network",
        "stage": "lead",
        "referred_by": signal.get("Source Client", ""),
        "network_parent": signal.get("Source Client", ""),
        "network_notes": signal.get("Signal Text", ""),
        "signal_date": signal.get("Timestamp", "")[:10],
    }
    if deal_overrides:
        deal.update(deal_overrides)
    result = create_deal(deal)
    if not result.get("ok"):
        return {"ok": False, "error": "Deal creation failed", "deal": result}
    signal_data = rows[signal_row - 1]
    status_idx = headers.index("Status") if "Status" in headers else 5
    signal_data += [""] * (len(headers) - len(signal_data))
    signal_data[status_idx] = "promoted"
    update_sheet(sid, f"'Network Signals'!A{signal_row}:F{signal_row}", [signal_data])
    return {"ok": True, "signal_row": signal_row, "deal": result}


def dismiss_signal(signal_row: int) -> dict:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, SIGNALS_RANGE)
    if not r.success or not r.data:
        return {"ok": False, "error": "Cannot read signals"}
    rows = r.data.get("values", [])
    if signal_row < 2 or signal_row > len(rows):
        return {"ok": False, "error": f"Signal row {signal_row} out of range"}
    headers = rows[0]
    signal_data = rows[signal_row - 1]
    status_idx = headers.index("Status") if "Status" in headers else 5
    signal_data += [""] * (len(headers) - len(signal_data))
    signal_data[status_idx] = "dismissed"
    result = update_sheet(sid, f"'Network Signals'!A{signal_row}:F{signal_row}", [signal_data])
    return {"ok": result.success}


def get_network_tree(root: str | None = None) -> dict:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, PIPELINE_RANGE)
    all_deals = _parse_rows(r)
    tree: dict[str, list[dict]] = {}
    for d in all_deals:
        parent = d.get("Network Parent", "") or d.get("Referred By", "")
        if parent:
            tree.setdefault(parent, []).append({
                "client": d.get("Client", ""),
                "stage": d.get("Stage", ""),
                "budget": d.get("Budget", ""),
            })
    if root:
        return {root: tree.get(root, [])}
    return tree


def get_network_value(client: str) -> dict:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, PIPELINE_RANGE)
    all_deals = _parse_rows(r)

    def _budget(d: dict) -> float:
        try:
            return float(d.get("Budget", "0").replace(",", "").replace("$", ""))
        except ValueError:
            return 0

    direct = sum(_budget(d) for d in all_deals if d.get("Client", "").lower() == client.lower())
    network = sum(
        _budget(d) for d in all_deals
        if (d.get("Network Parent", "") or d.get("Referred By", "")).lower()
        == client.lower()
    )
    return {"client": client, "direct_value": direct, "network_value": network, "total": direct + network}


def check_competitor_guard(company: str, source_client: str) -> bool:
    sid = get_spreadsheet_id()
    existing_clients: set[str] = set()
    r = read_sheet(sid, PIPELINE_RANGE)
    for d in _parse_rows(r):
        if d.get("Stage", "").lower() in ("won", "negotiation", "proposal"):
            existing_clients.add(d.get("Client", "").lower())
    clients_r = read_sheet(sid, "Clients!A:I")
    for c in _parse_rows(clients_r):
        if c.get("Status", "").lower() in ("active", "paused"):
            existing_clients.add(c.get("Client", "").lower())
    return company.lower() not in existing_clients
