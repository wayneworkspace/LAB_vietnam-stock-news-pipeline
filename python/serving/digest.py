"""Serving: build the morning digest from Gold (read-only). Facts only, no recommendations.
The digest text itself is Vietnamese on purpose: it is the product that gets posted to Discord."""
import os
from datetime import datetime, timedelta

import duckdb

from config import GOLD_DB, VN_TZ

SENT = {-2: "rất tiêu cực", -1: "hơi tiêu cực", 0: "trung lập", 1: "hơi tích cực", 2: "rất tích cực"}
LIMIT = 1900   # Discord allows 2000 characters per message
FOOTER = ("\n_Giá lấy từ CafeF, chưa đối chiếu nguồn khác. Cảm xúc do AI chấm, chỉ dựa trên tiêu đề._"
          "\n_Tham khảo, không phải khuyến nghị._")


def _num(x, nd=2):
    return f"{x:.{nd}f}".replace(".", ",")


def build_digest() -> str:
    if not os.path.exists(GOLD_DB):
        raise RuntimeError("No Gold data yet, run /build-gold first")
    now = datetime.now(VN_TZ).replace(tzinfo=None)
    con = duckdb.connect(GOLD_DB, read_only=True)
    try:
        try:
            tickers = [r[0] for r in con.execute("SELECT ma FROM dim_ticker WHERE active ORDER BY ma").fetchall()]
        except duckdb.CatalogException:
            raise RuntimeError("Gold is incomplete, run /build-gold first")
        lines = [f"**Bản tin sáng {now:%d/%m/%Y}**"]
        for ma in tickers:
            p = con.execute("SELECT date_key, close, pct_change FROM fact_price_daily WHERE ma = ? "
                            "ORDER BY date_key DESC LIMIT 1", [ma]).fetchone()
            if p:
                chg = f"{p[2] * 100:+.2f}".replace(".", ",") + "%" if p[2] is not None else "n/a"
                lines.append(f"\n**{ma}**: đóng cửa {_num(p[1], 1)} nghìn đồng ({chg} so với phiên trước, "
                             f"phiên {p[0]:%d/%m})")
            else:
                lines.append(f"\n**{ma}**: chưa có dữ liệu giá")
            n24 = con.execute("SELECT count(*) FROM fact_news WHERE ma = ? AND published_at >= ?",
                              [ma, now - timedelta(hours=24)]).fetchone()[0]
            lines.append(f"Số tin trong 24 giờ qua: {n24}. Ba tin gần nhất:")
            for ts, ntype, tk, sent, text in con.execute(
                    "SELECT published_at, news_type, time_known, sentiment, coalesce(summary, title) "
                    "FROM fact_news WHERE ma = ? ORDER BY published_at DESC NULLS LAST LIMIT 3", [ma]).fetchall():
                when = f"{ts:%d/%m %H:%M}" if tk else f"{ts:%d/%m}"
                kind = "công bố" if ntype == "cong_bo" else "báo"
                label = SENT.get(sent, "chưa chấm")
                lines.append(f"• {when} [{kind}] ({label}) {str(text)[:150]}")
        body = "\n".join(lines)
        if len(body) > LIMIT:
            body = body[:LIMIT].rsplit("\n", 1)[0] + "\n…"
        return body + FOOTER
    finally:
        con.close()
