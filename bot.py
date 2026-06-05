import sys
import time
import asyncio
import logging
import threading
import psycopg2
import config as cfg
from engine import CopyTrader, run_health_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def keep_neon_alive():
    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = psycopg2.connect(cfg.DATABASE_URL, sslmode="require")
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            logging.info("🟢 Neon keep-alive ping sent")
        except Exception as e:
            logging.warning(f"Neon keep-alive failed: {e}")
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
        time.sleep(180)

def handle_task_exception(task: asyncio.Task):
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.critical(f"💥 Background task worker loop failed: {task.get_name()} -> {e}", exc_info=True)




async def main():
    logging.info("⚡ Booting PolyGun-Optimized Polymarket Pipeline Infrastructure Interface...")

    # 1. Threaded Health / UI Layer Container Boot
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # 2. Database Session Maintenance Container Boot
    neon_thread = threading.Thread(target=keep_neon_alive, daemon=True)
    neon_thread.start()

    # 3. Instantiate Core Engine
    bot = CopyTrader()
    cfg._bot_ref = bot

    # 4. Market-channel WS listener — detects whale buy/sell signals by
    #    watching trade events on tokens held by tracked wallets.
    #    Tokens are subscribed progressively as the REST poller discovers them.
    ws_market_task = asyncio.create_task(
        bot._ws_listener.run(), name="MarketChannelWS"
    )
    ws_market_task.add_done_callback(handle_task_exception)

    # 5. User-channel WS listener — monitors OUR OWN wallet's order fills so
    #    pending buys are promoted to open positions faster than the REST cycle.
    #    Requires PRIVATE_KEY + CLOB credentials; silently stays idle otherwise.
    if bot._user_listener is not None:
        ws_user_task = asyncio.create_task(
            bot._user_listener.run(), name="UserChannelWS"
        )
        ws_user_task.add_done_callback(handle_task_exception)

    # 6. Passive Reconciliation Engine Poller
    while True:
        try:
            await bot.scan_and_copy()
        except (OSError, asyncio.TimeoutError) as transient_err:
            logging.warning(f"Transient polling error in validation handler: {transient_err}")
        except Exception:
            logging.critical("Fatal breakdown in validation engine polling layer.", exc_info=True)
            raise

        await asyncio.sleep(cfg.POLL_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Operations gracefully suspended via hardware interrupt signal.")
