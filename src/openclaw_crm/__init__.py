from __future__ import annotations

from datetime import date

from openclaw_crm import pipeline, network


class CRMManager:
    def pipeline_summary(self) -> str:
        s = pipeline.get_pipeline_summary()
        stages = " | ".join(f"{k}: {v}" for k, v in s["by_stage"].items())
        lines = [
            f"*Pipeline* — {s['total_deals']} active, {s['won_deals']} won",
            f"Weighted value: ${s['total_weighted_value']:,.0f}",
            f"Stages: {stages}",
        ]
        if s["network_count"]:
            lines.append(f"Network deals: {s['network_count']} (top referrer: {s['top_referrer']})")
        if s["stale_count"]:
            lines.append(f":warning: {s['stale_count']} stale deals (no contact 7+ days)")
        return "\n".join(lines)

    def stale_deals(self) -> str:
        buckets = pipeline.get_stale_deals()
        if not any(buckets.values()):
            return ":white_check_mark: No stale deals"
        lines = [":hourglass: *Stale Deals*"]
        emoji_map = {21: ":red_circle:", 14: ":large_orange_circle:", 7: ":yellow_circle:"}
        for threshold, deals in sorted(buckets.items(), reverse=True):
            emoji = emoji_map.get(threshold, ":white_circle:")
            for d in deals:
                client = d.get('Client', '?')
                days_stale = d['_days_stale']
                stage = d.get('Stage', '?')
                lines.append(f"{emoji} *{client}* — {days_stale}d stale (stage: {stage})")
        return "\n".join(lines)

    def overdue_invoices(self) -> str:
        overdue = pipeline.get_overdue_invoices()
        if not overdue:
            return ":white_check_mark: No overdue invoices"
        lines = [":money_with_wings: *Overdue Invoices*"]
        for inv in overdue:
            lines.append(f":red_circle: *{inv.get('Client', '?')}* — {inv['_days_overdue']}d overdue")
        return "\n".join(lines)

    def add_deal(self, **kwargs) -> str:
        result = pipeline.create_deal(kwargs)
        if result.get("ok"):
            return f":white_check_mark: Deal created for *{result['client']}* (row {result['row']})"
        return f":x: Failed to create deal: {result}"

    def move_deal(self, client: str, stage: str) -> str:
        result = pipeline.move_stage(client, stage)
        if result.get("ok"):
            return f":white_check_mark: *{client}* moved to *{stage}* (row {result['row']})"
        return f":x: {result.get('error', 'Unknown error')}"

    def network_tree(self, root: str | None = None) -> str:
        tree = network.get_network_tree(root)
        if not tree:
            return "No network referrals found."
        lines = [":spider_web: *Network Tree*"]
        for parent, children in tree.items():
            lines.append(f"\n*{parent}*")
            for c in children:
                lines.append(f"  → {c['client']} ({c['stage']}) — {c['budget']}")
        return "\n".join(lines)

    def pending_signals(self) -> str:
        signals = network.get_pending_signals()
        if not signals:
            return ":white_check_mark: No pending signals"
        lines = [f":satellite: *{len(signals)} Pending Signals*"]
        for s in signals:
            source = s.get('Source Client', '?')
            company = s.get('Mentioned Company', '?')
            text = s.get('Signal Text', '')[:80]
            lines.append(f"• {source} → {company}: {text}")
        return "\n".join(lines)

    def promote_signal(self, row: int, **overrides) -> str:
        result = network.promote_signal(row, overrides or None)
        if result.get("ok"):
            return f":white_check_mark: Signal row {row} promoted to deal"
        return f":x: {result.get('error', 'Unknown error')}"

    def dismiss_signal(self, row: int) -> str:
        result = network.dismiss_signal(row)
        if result.get("ok"):
            return f":white_check_mark: Signal row {row} dismissed"
        return ":x: Failed to dismiss signal"

    def record_signal(self, **kwargs) -> str:
        result = network.add_signal(kwargs)
        if result.get("ok"):
            return f":white_check_mark: Signal recorded"
        return f":x: Failed to record signal"
