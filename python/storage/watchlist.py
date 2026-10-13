"""Watchlist table operations (SQLite)."""
from storage import bronze

MAX_ACTIVE_TICKERS = 10


def active_tickers(con) -> list:
    """[(ma, san)] for the tickers being tracked; an empty exchange defaults to HOSE."""
    return con.execute("SELECT ma, coalesce(san, 'HOSE') FROM watchlist WHERE active = 1 "
                       "ORDER BY ma").fetchall()


def upsert_watchlist(ma, ten=None, san=None, active=True, ghi_chu=None):
    ma = ma.strip().upper()
    bronze.init_bronze()
    con = bronze.connect()
    try:
        if active:
            n = con.execute("SELECT count(*) FROM watchlist WHERE active = 1 AND ma <> ?",
                            [ma]).fetchone()[0]
            if n >= MAX_ACTIVE_TICKERS:
                raise ValueError(f"Watchlist is full: at most {MAX_ACTIVE_TICKERS} active tickers")
        con.execute(
            """INSERT INTO watchlist (ma, ten, san, active, ghi_chu) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT (ma) DO UPDATE SET ten = excluded.ten, san = excluded.san,
                 active = excluded.active, ghi_chu = excluded.ghi_chu""",
            [ma, ten, san, 1 if active else 0, ghi_chu])
        con.commit()
        return [[r[0], r[1], r[2], bool(r[3]), r[4]] for r in con.execute(
            "SELECT ma, ten, san, active, ghi_chu FROM watchlist ORDER BY ma").fetchall()]
    finally:
        con.close()
