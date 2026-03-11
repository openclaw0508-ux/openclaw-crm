from __future__ import annotations

from datetime import date, datetime

from openclaw_crm.sheets import read_sheet, append_sheet, update_sheet
from openclaw_crm.config import get_spreadsheet_id

STAGE_PROBABILITY = {
    "lead": 0.10, "qualifying": 0.25, "proposal": 0.50,
    "negotiation": 0.75, "won": 1.0, "lost": 0.0,
}

HEADERS = [
    "Client", "Contact", "Source", "Stage", "Budget", "Rate Type",
    "Service", "First Contact", "Last Contact", "Next Action",
    "Due Date", "Notes", "Slack Channel", "Proposal Link",
    "Owner", "Upwork URL", "Probability",
    "Referred By", "Network Parent", "Network Notes", "Signal Date",
]

PIPELINE_RANGE = "Pipeline!A:U"
REVENUE_RANGE = "'Revenue Log'!A:F"


def _parse_rows(result) -> list[dict]:
    if not result.success or not result.data:
        return []
    rows = result.data.get("values", [])
    if len(rows) < 2:
        return []
    headers = rows[0]
    return [
        dict(zip(headers, row + [""] * (len(headers) - len(row))))
        for row in rows[1:]
    ]


def _days_since(date_str: str) -> int:
    if not date_str:
        return 999
    try:
        d = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        return (date.today() - d).days
    except ValueError:
        return 999


def get_pipeline(active_only: bool = True) -> list[dict]:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, PIPELINE_RANGE)
    deals = _parse_rows(r)
    if active_only:
        deals = [d for d in deals if d.get("Stage", "").lower() not in ("won", "lost")]
    return deals


def create_deal(deal: dict) -> dict:
    sid = get_spreadsheet_id()
    existing = _parse_rows(read_sheet(sid, PIPELINE_RANGE))
    row_num = len(existing) + 2
    prob_formula = (
        f'=IFS(D{row_num}="lead",0.1,D{row_num}="qualifying",0.25,'
        f'D{row_num}="proposal",0.5,D{row_num}="negotiation",0.75,'
        f'D{row_num}="won",1,D{row_num}="lost",0,TRUE,0)'
    )
    source = deal.get("source", "upwork").lower()
    if deal.get("referred_by"):
        source = "network"
    stage = deal.get("stage", "lead").lower()
    row = [
        deal.get("client", ""),
        deal.get("contact", ""),
        source,
        stage,
        deal.get("budget", ""),
        deal.get("rate_type", "fixed"),
        deal.get("service", ""),
        deal.get("first_contact", date.today().isoformat()),
        deal.get("last_contact", date.today().isoformat()),
        deal.get("next_action", "Review lead"),
        deal.get("due_date", ""),
        deal.get("notes", ""),
        deal.get("slack_channel", ""),
        deal.get("proposal_link", ""),
        deal.get("owner", ""),
        deal.get("upwork_url", ""),
        prob_formula,
        deal.get("referred_by", ""),
        deal.get("network_parent", deal.get("referred_by", "")),
        deal.get("network_notes", ""),
        deal.get("signal_date", date.today().isoformat() if deal.get("referred_by") else ""),
    ]
    result = append_sheet(sid, PIPELINE_RANGE, [row])
    return {"ok": result.success, "row": row_num, "client": deal.get("client", "")}


def update_deal(row: int, updates: dict) -> dict:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, PIPELINE_RANGE)
    rows = r.data.get("values", []) if r.success and r.data else []
    if row < 2 or row > len(rows):
        return {"ok": False, "error": f"Row {row} out of range"}
    headers = rows[0]
    current = rows[row - 1]
    current += [""] * (len(headers) - len(current))
    for key, val in updates.items():
        if key in headers:
            current[headers.index(key)] = val
    result = update_sheet(sid, f"Pipeline!A{row}:U{row}", [current])
    return {"ok": result.success}


def move_stage(client: str, new_stage: str) -> dict:
    new_stage = new_stage.lower()
    sid = get_spreadsheet_id()
    r = read_sheet(sid, PIPELINE_RANGE)
    rows = r.data.get("values", []) if r.success and r.data else []
    if len(rows) < 2:
        return {"ok": False, "error": "No pipeline data"}
    headers = rows[0]
    client_idx = headers.index("Client") if "Client" in headers else 0
    stage_idx = headers.index("Stage") if "Stage" in headers else 3
    contact_idx = headers.index("Last Contact") if "Last Contact" in headers else 8
    for i, row in enumerate(rows[1:], start=2):
        if len(row) > client_idx and row[client_idx].lower() == client.lower():
            row += [""] * (len(headers) - len(row))
            row[stage_idx] = new_stage
            row[contact_idx] = date.today().isoformat()
            update_sheet(sid, f"Pipeline!A{i}:U{i}", [row])
            return {"ok": True, "client": client, "stage": new_stage, "row": i}
    return {"ok": False, "error": f"Client '{client}' not found"}


def get_pipeline_summary() -> dict:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, PIPELINE_RANGE)
    all_deals = _parse_rows(r)
    active = [d for d in all_deals if d.get("Stage", "").lower() not in ("won", "lost")]
    by_stage: dict[str, list] = {}
    total_weighted = 0.0
    network_count = 0
    for d in active:
        stage = d.get("Stage", "unknown").lower()
        by_stage.setdefault(stage, []).append(d)
        try:
            budget = float(d.get("Budget", "0").replace(",", "").replace("$", ""))
        except ValueError:
            budget = 0
        total_weighted += budget * STAGE_PROBABILITY.get(stage, 0)
        if d.get("Referred By"):
            network_count += 1
    stale_count = sum(1 for d in active if _days_since(d.get("Last Contact", "")) >= 7)
    top_referrer = ""
    if network_count:
        refs: dict[str, int] = {}
        for d in active:
            ref = d.get("Referred By", "")
            if ref:
                refs[ref] = refs.get(ref, 0) + 1
        if refs:
            top_referrer = max(refs, key=refs.get)
    return {
        "total_deals": len(active),
        "won_deals": len([d for d in all_deals if d.get("Stage", "").lower() == "won"]),
        "by_stage": {k: len(v) for k, v in by_stage.items()},
        "total_weighted_value": round(total_weighted, 2),
        "stale_count": stale_count,
        "network_count": network_count,
        "top_referrer": top_referrer,
        "deals": active,
    }


def get_stale_deals(thresholds: list[int] | None = None) -> dict:
    if thresholds is None:
        thresholds = [7, 14, 21]
    thresholds = sorted(thresholds, reverse=True)
    deals = get_pipeline(active_only=True)
    buckets: dict[int, list] = {t: [] for t in thresholds}
    for deal in deals:
        days = _days_since(deal.get("Last Contact", ""))
        for t in thresholds:
            if days >= t:
                deal["_days_stale"] = days
                buckets[t].append(deal)
                break
    return buckets


def get_overdue_invoices() -> list[dict]:
    sid = get_spreadsheet_id()
    r = read_sheet(sid, REVENUE_RANGE)
    rows = _parse_rows(r)
    overdue = []
    for row in rows:
        if row.get("Status", "").lower() == "sent":
            days = _days_since(row.get("Date", ""))
            if days > 30:
                row["_days_overdue"] = days
                overdue.append(row)
    return overdue
