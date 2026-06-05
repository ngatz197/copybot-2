#!/usr/bin/env python3
import asyncio
import logging
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# ==================== OPTIONAL DEPENDENCIES ====================
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logging.warning("psycopg2 not installed — seen_trades will fall back to local file.")

# ==================== DATA CLASSES ====================
@dataclass
class Position:
    market_id:     str
    question:      str
    outcome:       str
    token_id:      str
    entry_price:   float
    size_usd:      float
    shares:        float
    source_wallet: str
    source_name:   str
    status:        str   = "open"
    exit_price:    float = 0.0
    pnl:           float = 0.0
    order_id:      str   = ""
    current_price: float = 0.0
    signal_source: str   = "rest"   # "ws" | "rest"
    # Last-known share count of the *source* wallet for this position.
    # Updated on every REST poll so we can detect partial sells.
    source_shares: float = 0.0
    # Accumulated sub-threshold sell fractions.  When this crosses
    # PARTIAL_SELL_THRESHOLD the combined reduction is acted upon and reset.
    pending_reduction: float = 0.0

@dataclass
class PendingLimitBuy:
    pos_key:       str
    token_id:      str
    market_id:     str
    question:      str
    outcome:       str
    source_wallet: str
    source_name:   str
    limit_price:   float
    size_usd:      float
    order_id:      str
    signal_source: str      = "rest"   # "ws" | "rest"
    placed_at:     datetime = field(default_factory=datetime.now)

# ==================== SEEN TRADES STORE ====================
class SeenTradesStore:
    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath = filepath
        self.db_url   = db_url
        self._seen: set = set()
        self._conn   = None

        if db_url and PSYCOPG2_AVAILABLE:
            self._init_postgres()
        else:
            self._load_file()

        logging.info(f"SeenTradesStore ready | backend={self.backend} | {len(self._seen)} historic keys loaded")

    def _init_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS seen_trades (
                        pos_key    TEXT PRIMARY KEY,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_state (
                        key   TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """)
            self._seen   = self._load_postgres()
            self.backend = "postgres"
            logging.info(f"Postgres connected — {len(self._seen)} seen keys loaded")
        except Exception as e:
            logging.error(f"Postgres init failed: {e} — falling back to local file")
            self._conn = None
            self._load_file()

    def _load_postgres(self):
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pos_key FROM seen_trades")
                return {row[0] for row in cur.fetchall()}
        except Exception as e:
            logging.warning(f"Postgres load failed: {e}")
            return set()

    def _save_postgres(self, pos_key: str):
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING",
                    (pos_key,)
                )
        except Exception as e:
            logging.warning(f"Postgres save failed for {pos_key}: {e}")
            self._reconnect_postgres()

    def _save_postgres_many(self, keys):
        if not keys: return
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING",
                    [(k,) for k in keys]
                )
        except Exception as e:
            logging.warning(f"Postgres bulk save failed: {e}")
            self._reconnect_postgres()

    def _reconnect_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            # Reload from DB and merge so any keys written by other processes
            # during the outage are picked up, and in-memory-only keys are retained.
            refreshed = self._load_postgres()
            self._seen.update(refreshed)
            logging.info(f"Postgres reconnected — merged {len(refreshed)} keys from DB")
        except Exception as e:
            logging.error(f"Postgres reconnect failed: {e}")
            self._conn = None

    def _load_file(self):
        try:
            with open(self.filepath, "r") as f:
                data = json.load(f)
                self._seen = set(data) if isinstance(data, list) else set()
        except FileNotFoundError:
            self._seen = set()
        except Exception as e:
            logging.warning(f"Could not read seen trades file: {e}")
            self._seen = set()
        self.backend = "local-file"

    def _save_file(self):
        def _sync_write():
            try:
                with open(self.filepath, "w") as f:
                    json.dump(sorted(self._seen), f)
            except Exception as e:
                logging.warning(f"Could not save seen trades file: {e}")

        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, _sync_write)
        except RuntimeError:
            _sync_write()

    def is_seen(self, pos_key: str) -> bool:
        return pos_key in self._seen

    def mark_seen(self, pos_key: str):
        if pos_key in self._seen: return
        if self._conn:
            self._save_postgres(pos_key)  # persist first; ON CONFLICT makes this idempotent
        self._seen.add(pos_key)
        if not self._conn:
            self._save_file()

    def unmark_seen(self, pos_key: str):
        self._seen.discard(pos_key)
        if self._conn:
            try:
                with self._conn.cursor() as cur:
                    cur.execute("DELETE FROM seen_trades WHERE pos_key = %s", (pos_key,))
            except Exception as e:
                logging.warning(f"Postgres unmark failed: {e}")
        else:
            self._save_file()

    def snapshot_existing(self, pos_keys):
        new_keys = [k for k in pos_keys if k not in self._seen]
        if not new_keys: return
        if self._conn:
            self._save_postgres_many(new_keys)
        else:
            # Stage in-memory first for file backend, then persist
            for k in new_keys:
                self._seen.add(k)
            self._save_file()
            logging.info(f"Snapshot: marked {len(new_keys)} pre-existing trades as seen")
            return
        # Postgres path: only add to _seen after the write attempt
        # _save_postgres_many logs on failure but doesn't raise; we still
        # mark in-memory so the current session won't re-process them.
        # On a fresh restart the DB is the source of truth, so a failed
        # write would at worst cause a one-time duplicate signal — which
        # the ON CONFLICT guard on the DB side makes harmless.
        for k in new_keys:
            self._seen.add(k)
        logging.info(f"Snapshot: marked {len(new_keys)} pre-existing trades as seen")

    @property
    def is_empty(self) -> bool:
        return len(self._seen) == 0


# ==================== BANKROLL PERSISTENCE ====================
def save_bankroll(conn, value: float):
    """Persist compounding_bankroll to Postgres so it survives restarts."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bot_state (key, value) VALUES ('compounding_bankroll', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (str(value),))
        conn.commit()
    except Exception as e:
        logging.warning(f"Failed to save bankroll: {e}")


def load_bankroll(conn) -> Optional[float]:
    """Load persisted compounding_bankroll from Postgres. Returns None if not found."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM bot_state WHERE key = 'compounding_bankroll'")
            row = cur.fetchone()
            return float(row[0]) if row else None
    except Exception as e:
        logging.warning(f"Failed to load bankroll: {e}")
        return None