#!/usr/bin/env python3
import os
import time
import logging
import asyncio
import requests
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import html
import config as cfg

from models import Position, PendingLimitBuy, SeenTradesStore, save_bankroll, load_bankroll
from exchange import RobustBalanceManager, PolymarketExecutor, PolymarketWSListener, PolymarketUserChannelListener

# ==================== ENVIRONMENT / CONSTANTS ====================
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "50"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "8080"))
MAX_RETRIES           = 3
RETRY_DELAY           = 5
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.08"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "90"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
DATABASE_URL          = os.getenv("DATABASE_URL", "")
PARTIAL_SELL_THRESHOLD  = float(os.getenv("PARTIAL_SELL_THRESHOLD", "0.20"))
SELL_LIMIT_MAX_DISCOUNT = float(os.getenv("SELL_LIMIT_MAX_DISCOUNT", "0.05"))

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# ==================== SIZING HELPERS ====================
def _price_based_size(price: float) -> float:
    if price < 0.30:
        pct = 0.01   # 5% for low-probability outcomes
    elif price <= 0.70:
        pct = 0.05   # 20% for mid-range outcomes
    else:
        pct = 0.07   # 25% for high-probability outcomes
    return cfg.compounding_bankroll * pct


def _calc_size(config: dict, price: float, source_value: float = 0.0) -> float:
    if config.get("risk_type") == "fixed":
        return cfg.compounding_bankroll * config.get("fixed_risk", 0.025)

    tiered = _price_based_size(price)

    # If the source wallet spent under $1 and this wallet has copy_sub_dollar
    # enabled, mirror their exact spend regardless of bankroll size.
    # Only fall through to tiered sizing when source trade is $1 or above.
    if config.get("copy_sub_dollar", False) and 0 < source_value < 1.0:
        return source_value

    return tiered


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = cfg.DRY_RUN):
        self.dry_run          = dry_run
        self.balance          = RobustBalanceManager(dry_run=self.dry_run)
        
        try:
            logging.info("Initializing bankroll allocation from live wallet balance...")
            initial_balance = self.balance.fetch_with_retry(retries=5, delay=5)

            # Guard: if fetch_with_retry is ever accidentally made async, calling
            # it without await returns a coroutine object instead of a float,
            # producing the cryptic "unsupported format string passed to coroutine"
            # crash.  Detect and recover here rather than dying at the f-string.
            if asyncio.iscoroutine(initial_balance):
                logging.warning(
                    "fetch_with_retry returned a coroutine — it must be a plain sync "
                    "method. Awaiting as one-time fallback; fix the method signature."
                )
                initial_balance = asyncio.get_event_loop().run_until_complete(initial_balance)

            initial_balance = float(initial_balance)  # hard cast — fails loudly if non-numeric
            cfg.compounding_bankroll      = initial_balance
            cfg.peak_bankroll             = initial_balance
            # Seed dry-run virtual balance from the real on-chain balance so
            # deductions and drawdown checks start from the correct baseline.
            self.balance.cached_balance   = initial_balance
            self.balance.peak_balance     = initial_balance
            logging.info(f"Dry-run virtual balance seeded at ${initial_balance:.2f}")
        except SystemExit:
            raise  # don't swallow our own intentional exits
        except Exception as e:
            logging.error(f"Critical initialization failure: {e}")
            raise SystemExit("Exiting bot: Unable to ascertain initial balance configuration.")

        self.positions:       Dict[str, Position]        = {}
        self.pending:         Dict[str, PendingLimitBuy] = {}
        self.closed_positions: list                      = []
        self.executor         = PolymarketExecutor(dry_run)
        self.seen             = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)

        # Restore compounding_bankroll from Postgres if available so restarts
        # don't reset sizing history.  Fall back to real wallet balance on first run.
        saved_bankroll = load_bankroll(self.seen._conn) if self.seen._conn else None
        if saved_bankroll is not None:
            cfg.compounding_bankroll = saved_bankroll
            cfg.peak_bankroll        = max(saved_bankroll, initial_balance)
            logging.info(f"Compounding bankroll restored from DB: ${saved_bankroll:.2f}")
        else:
            cfg.compounding_bankroll = initial_balance
            cfg.peak_bankroll        = initial_balance
            logging.info(f"Compounding bankroll seeded from wallet: ${initial_balance:.2f}")

        # Guard: if bankroll is zero (e.g. DB returned 0, or first-run edge case),
        # fall back to the live wallet balance so sizing never produces $0 orders.
        if cfg.compounding_bankroll <= 0:
            cfg.compounding_bankroll = initial_balance
            logging.warning(
                f"[INIT] compounding_bankroll was 0 or negative — reset to live balance ${initial_balance:.2f}"
            )

        self._first_scan_done:   Set[str]       = set()
        self._lookback_tokens:   Dict[str, set] = {}  # token_ids traded within lookback window per wallet

        # Record bot start time for lookback filtering (if not already set by main())
        if cfg.BOT_START_TIME is None:
            cfg.BOT_START_TIME = datetime.utcnow()
            logging.info(f"[LOOKBACK] BOT_START_TIME set to {cfg.BOT_START_TIME} UTC | window={cfg.LOOKBACK_HOURS}h")

        # Lock that serialises the "check-seen → place-order → mark-seen" critical
        # section so a simultaneous WS signal and REST poll can never both slip
        # through for the same pos_key (fix #14).
        self._pending_lock: asyncio.Lock = asyncio.Lock()

        self._ws_price_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._ws_tracked:     Set[str]      = set()
        self._ws_listener:    Optional[PolymarketWSListener] = None
        # Tracks dedup keys for WS sell signals that have already been acted on,
        # mapped to the monotonic timestamp at which they were recorded.
        # Using a dict (key → timestamp) instead of a plain set lets us evict
        # only entries that are genuinely stale (> 10 min old) rather than
        # clearing the entire set at once, which would drop still-relevant guards
        # and allow the REST poller to re-fire a sell already handled by WS.
        self._ws_sell_executed: Dict[str, float] = {}

        if WEBSOCKETS_AVAILABLE:
            self._ws_listener = PolymarketWSListener(
                token_ids                = self._ws_tracked,
                wallet_addrs             = set(cfg.WALLETS.keys()),
                ws_price_queue           = self._ws_price_queue,
                on_trade_callback        = self._on_ws_event,
                on_order_placed_callback = self._on_ws_order_placed,
            )
            # User channel listener: monitors OUR OWN wallet's fills only.
            # Source-wallet detection (whale buy/sell signals) is handled
            # entirely by _ws_listener on the public /ws/market channel.
            # The user channel is an authenticated private endpoint and rejects
            # subscriptions to third-party wallets — that was the reconnect loop.
            self._user_listener = PolymarketUserChannelListener(
                on_fill_callback = self._on_own_fill,
            )
            logging.info("PolymarketWSListener initialised — market channel (whale detection + new-token order_placed) + user channel (own fills)")
        else:
            logging.warning("WebSocket listener inactive — install websockets to enable")
            self._user_listener = None

        logging.info(f"CopyTrader V2 started | mode={'DRY RUN' if dry_run else 'LIVE'}")

    def _update_compounding(self, realised_pnl: float):
        """
        Update the sizing base after every realised trade.

        Wins:   grow by profit × COMPOUNDING_RATE (conservative reinvestment)
        Losses: shrink by the full loss (losses are never dampened by the rate)

        Persists to Postgres after every update so the value survives restarts.
        Also mirrors into RobustBalanceManager for dry-run so sizing stays
        consistent with virtual balance deductions.
        """
        if realised_pnl >= 0:
            delta = realised_pnl * cfg.COMPOUNDING_RATE
        else:
            delta = realised_pnl  # full loss absorbed immediately

        cfg.compounding_bankroll = max(cfg.compounding_bankroll + delta, 0.0)

        if cfg.compounding_bankroll > cfg.peak_bankroll:
            cfg.peak_bankroll = cfg.compounding_bankroll

        if self.seen._conn:
            save_bankroll(self.seen._conn, cfg.compounding_bankroll)

        logging.info(
            f"[COMPOUND] pnl={realised_pnl:+.4f} | rate={cfg.COMPOUNDING_RATE:.0%} | "
            f"delta={delta:+.4f} | sizing_base=${cfg.compounding_bankroll:.2f}"
        )

    async def _on_ws_order_placed(self, ev: dict):
        """
        Fires when a tracked wallet posts a NEW resting order on a token we
        have not seen before.

        At this point the order is placed but not yet filled — the whale has
        declared intent.  We use this window to:

        1. Self-subscribe the token on the market channel so the fill event
           arrives in real time (the listener already does this before calling
           here, but we record it in _ws_tracked to survive reconnects).

        2. Fetch the orderbook and place our own limit buy at best_ask,
           capped at the whale's order price + premium.  This gets us in at
           or near the same price the whale is targeting, before the fill
           moves the market.

        If outcome is missing we cannot build a valid pos_key — we skip the
        order placement but still keep the token subscribed so the fill event
        (which should carry outcome) triggers _on_ws_event normally.
        """
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return

        token_id   = ev.get("token_id", "")
        maker_addr = ev.get("maker_addr", "").lower()
        price      = float(ev.get("price", 0))
        outcome    = ev.get("outcome", "").upper()

        if not token_id or not maker_addr or price <= 0:
            return

        tracked_wallets = {addr.lower(): addr for addr in cfg.WALLETS}
        if maker_addr not in tracked_wallets:
            return

        matched_addr = tracked_wallets[maker_addr]
        config       = cfg.WALLETS.get(matched_addr) or cfg.WALLETS.get(maker_addr)
        if not config:
            return

        # Always ensure the token is tracked for the subsequent fill event.
        if token_id not in self._ws_tracked:
            if self._ws_listener:
                asyncio.create_task(self._ws_listener.subscribe_token(token_id))

        if not outcome:
            # Can't place an order without outcome — wait for the fill event
            # which should carry it.  Token is already subscribed above.
            logging.info(
                f"[WS ORDER_PLACED] {config['name']} placed order on "
                f"{token_id[:12]}… — outcome unknown, token subscribed, "
                f"deferring entry to fill event."
            )
            return

        is_broken, _ = self.balance.check_drawdown()
        if is_broken is None or is_broken:
            return

        pos_key = f"{maker_addr}_{token_id}_{outcome}"

        # Fetch orderbook before acquiring lock — slow HTTP call outside lock.
        loop = asyncio.get_running_loop()
        best_ask, _ = await loop.run_in_executor(
            None, self.get_orderbook_prices, token_id
        )

        async with self._pending_lock:
            if self.seen.is_seen(pos_key) or pos_key in self.pending:
                return

            if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                logging.warning(
                    f"[WS ORDER_PLACED] Position limit reached — "
                    f"skipping {config['name']} pre-signal."
                )
                return

            premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
            # Cap at whale's order price + premium — never pay more than we
            # would have on a fill signal.
            price_cap    = price * (1.0 + premium)
            actual_price = min(best_ask, price_cap) if best_ask > 0 else price_cap

            if actual_price <= 0 or actual_price >= 1.0:
                return

            source_value = 0.0  # unknown at order-placement time
            my_size = _calc_size(config, actual_price, source_value)

            current_bal = self.balance.get_balance()
            if current_bal is not None and my_size > current_bal:
                return

            logging.info(
                f"⚡ [WS PRE-FILL BUY] {config['name']} | {outcome} "
                f"token {token_id[:12]}… @ {actual_price:.4f} "
                f"(${my_size:.2f}) — placed ahead of fill [signal_source=ws]"
            )

            ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
            if not ok:
                logging.warning(
                    f"[WS ORDER_PLACED] Pre-fill order placement failed for "
                    f"{config['name']} — fill event will retry via _on_ws_event."
                )
                return

            if self.dry_run:
                self.balance.apply_dry_run_buy(my_size)

            self.seen.mark_seen(pos_key)

            self.pending[pos_key] = PendingLimitBuy(
                pos_key       = pos_key,
                token_id      = token_id,
                market_id     = "pending-ws",
                question      = f"WS pre-fill — {token_id[:16]}…",
                outcome       = outcome,
                source_wallet = matched_addr,
                source_name   = config["name"],
                limit_price   = actual_price,
                size_usd      = my_size,
                order_id      = order_id,
                signal_source = "ws",
            )

    async def _on_own_fill(self, ev: dict):
        """
        Callback for fill confirmations on our OWN orders from the user channel.

        Primary use: accelerate pending-order fill detection so we promote a
        PendingLimitBuy to an open Position faster than the REST poll cycle.
        The REST poller (process_pending_fills) remains the authoritative source;
        this is a speed-up, not a replacement.
        """
        order_id = ev.get("order_id", "")
        token_id = ev.get("token_id", "")
        side     = ev.get("side", "")
        price    = float(ev.get("price", 0))

        if not order_id:
            return

        # Find the matching pending order by order_id.
        matched_key = next(
            (k for k, p in self.pending.items() if p.order_id == order_id),
            None,
        )
        if matched_key is None:
            # Could be a sell fill or an order we don't track — ignore quietly.
            logging.debug(
                f"[USER-WS OWN FILL] {side} fill for order {order_id[:12]}… "
                f"not found in pending — already promoted or is a sell."
            )
            return

        p = self.pending[matched_key]
        logging.info(
            f"✨ [USER-WS OWN FILL] {p.source_name} | {p.outcome} | "
            f"order={order_id[:12]}… | price={price:.4f} — promoting to open position"
        )

        # Promote immediately — don't wait for the next REST poll cycle.
        self.positions[matched_key] = Position(
            market_id     = p.market_id,
            question      = p.question,
            outcome       = p.outcome,
            token_id      = p.token_id,
            entry_price   = price if price > 0 else p.limit_price,
            size_usd      = p.size_usd,
            shares        = round(p.size_usd / (price if price > 0 else p.limit_price), 4),
            source_wallet = p.source_wallet,
            source_name   = p.source_name,
            order_id      = p.order_id,
            current_price = price if price > 0 else p.limit_price,
            signal_source = p.signal_source,
            source_shares = 0.0,
        )
        del self.pending[matched_key]

    async def _on_ws_event(self, ev: dict):
        """
        Market-channel WS trade callback.  All whale-detection signals arrive
        here from PolymarketWSListener (/ws/market).  Resolves source wallet
        direction from maker_side / taker_side fields then mirrors the action.

        - Source BUY  → open / add to position.
        - Source SELL → close / reduce position.

        Own-order fill confirmations are handled separately by _on_own_fill
        (user channel) and do NOT pass through this method.
        """
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return

        tracked_wallets = {addr.lower(): addr for addr in cfg.WALLETS}

        # All whale-detection events come from the market channel.
        # Events carry maker_addr / taker_addr; scan both to find a match.
        maker, taker  = ev.get("maker_addr", ""), ev.get("taker_addr", "")
        matched_lower = next(
            (w for w in tracked_wallets if w in (maker, taker)), None
        )

        if not matched_lower:
            return

        matched_addr = tracked_wallets[matched_lower]
        config       = cfg.WALLETS.get(matched_addr) or cfg.WALLETS.get(matched_lower)
        if not config:
            return

        copy_mode = config.get("copy_mode", "new_only")
        if copy_mode not in ("new_only", "all"):
            logging.warning(f"[WS] {config['name']} copy_mode='{copy_mode}' not supported — skipping.")
            return

        token_id   = ev["token_id"]
        outcome    = ev.get("outcome", "").upper()
        maker_side = ev.get("maker_side", "")
        taker_side = ev.get("taker_side", "")

        if not outcome:
            # Missing outcome — skip rather than defaulting to YES and
            # mislabelling the position.
            logging.debug(
                f"[WS] Missing outcome for {token_id[:12]}… — skipping to avoid "
                f"mislabelling position."
            )
            return

        if matched_lower == maker and maker_side:
            source_trade_side = maker_side
        elif matched_lower == taker and taker_side:
            source_trade_side = taker_side
        else:
            # maker_side / taker_side absent — cannot determine direction safely.
            # Skip and let REST poll handle it from authoritative share counts.
            logging.warning(
                f"[WS] maker_side/taker_side absent for {config['name']} "
                f"token {token_id[:12]}\u2026 — cannot resolve direction; "
                f"deferring to REST poll."
            )
            return

        pos_key = f"{matched_lower}_{token_id}_{outcome}"

        if source_trade_side == "SELL":
            # ── Sell path ──────────────────────────────────────────────────────
            open_pos_key = next(
                (k for k, p in self.positions.items()
                 if p.source_wallet == matched_addr
                 and p.token_id == token_id
                 and p.status == "open"),
                None,
            )
            if open_pos_key is None:
                logging.debug(
                    f"[WS SELL] {config['name']} sold token {token_id[:12]}… "
                    f"but we hold no matching position — ignoring."
                )
                return
            await self._on_ws_sell_event(ev, open_pos_key, self.positions[open_pos_key])
        else:
            # ── Buy path ───────────────────────────────────────────────────────
            await self._on_ws_buy_event(ev, matched_lower, matched_addr, config, token_id, outcome, pos_key)


    async def _on_ws_buy_event(
        self,
        ev:           dict,
        matched_lower: str,
        matched_addr:  str,
        config:        dict,
        token_id:      str,
        outcome:       str,   # market outcome: YES / NO / OVER / UNDER etc.
        pos_key:       str,
    ):
        is_broken, _ = self.balance.check_drawdown()
        if is_broken is None:
            logging.warning("[WS BUY] Balance unknown — skipping signal until balance is confirmed.")
            return
        if is_broken:
            return

        # Fetch orderbook BEFORE acquiring the lock — slow HTTP call.
        loop = asyncio.get_running_loop()
        best_ask, _ = await loop.run_in_executor(
            None, self.get_orderbook_prices, token_id
        )

        async with self._pending_lock:
            if self.seen.is_seen(pos_key) or pos_key in self.pending:
                return

            if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                logging.warning(f"[WS BUY] Position limit reached — skipping {config['name']} signal.")
                return

            signal_price = float(ev.get("price", 0.0))
            premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
            if signal_price <= 0:
                # Source price required for both steps — nothing to anchor on; skip.
                logging.warning(
                    f"[WS BUY] No signal price for {token_id[:12]}… "
                    f"— skipping {config['name']}."
                )
                return
            if best_ask > 0:
                # Step 1 — orderbook available: post at best_ask, capped so we never
                # overpay above what the source wallet paid + premium.
                actual_price = min(best_ask, signal_price * (1.0 + premium))
            else:
                # Step 2 — orderbook unavailable: use source price + premium as proxy.
                actual_price = signal_price * (1.0 + premium)

            if actual_price <= 0 or actual_price >= 1.0:
                logging.error(f"[WS BUY] Invalid price {actual_price} for {token_id[:12]} — aborting.")
                return

            # Guard: skip if market has moved more than 50% above the source fill.
            if actual_price > signal_price * 1.50:
                logging.warning(
                    f"[WS BUY] Market moved too far from source price "
                    f"(source={signal_price:.4f}, market={actual_price:.4f}) — skipping {config['name']}."
                )
                return

            source_value = float(ev.get("size", 0.0)) * actual_price
            my_size = _calc_size(config, actual_price, source_value)

            current_bal = self.balance.get_balance()
            if current_bal is not None and my_size > current_bal:
                logging.warning(f"[WS BUY] Order size ${my_size:.2f} exceeds balance ${current_bal:.2f} — skipping {config['name']}.")
                return

            logging.info(
                f"⚡ [WS INSTANT BUY] {config['name']} | {outcome} "
                f"token {token_id[:12]}… @ {actual_price:.4f} "
                f"(${my_size:.2f}) [signal_source=ws]"
            )

            ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
            if not ok:
                logging.warning(f"[WS BUY] Order placement failed for {config['name']} — REST poll will retry.")
                return

            if self.dry_run:
                self.balance.apply_dry_run_buy(my_size)

            self.seen.mark_seen(pos_key)

            self.pending[pos_key] = PendingLimitBuy(
                pos_key       = pos_key,
                token_id      = token_id,
                market_id     = "pending-ws",
                question      = f"WS signal — {token_id[:16]}…",
                outcome       = outcome,
                source_wallet = matched_addr,
                source_name   = config["name"],
                limit_price   = actual_price,
                size_usd      = my_size,
                order_id      = order_id,
                signal_source = "ws",
            )

        if self._ws_listener and token_id not in self._ws_tracked:
            asyncio.create_task(self._ws_listener.subscribe_token(token_id))

    async def _on_ws_sell_event(self, ev: dict, pos_key: str, position: "Position"):
        """
        Mirror a sell detected via WS.  We use the source wallet's share count
        already stored on the position to compute the sell fraction.
        """
        ws_sold_shares = float(ev.get("size", 0.0))
        if ws_sold_shares <= 0:
            return

        if position.source_shares <= 0:
            logging.info("[WS SELL] source_shares not yet initialized — deferring to REST")
            return

        source_total  = position.source_shares if position.source_shares > 0 else ws_sold_shares
        sell_fraction = min(ws_sold_shares / source_total, 1.0)

        if sell_fraction < PARTIAL_SELL_THRESHOLD:
            # Accumulate sub-threshold reductions; fire when total crosses threshold.
            position.pending_reduction += sell_fraction
            logging.info(
                f"[WS SELL] {position.source_name} sold {sell_fraction:.1%} "
                f"(accumulated={position.pending_reduction:.1%}) — below threshold, accumulating."
            )
            if position.pending_reduction < PARTIAL_SELL_THRESHOLD:
                return
            # Accumulated total now crosses the threshold — fire and reset.
            sell_fraction              = position.pending_reduction
            position.pending_reduction = 0.0

        our_shares_to_sell = round(position.shares * sell_fraction, 4)
        if our_shares_to_sell <= 0:
            return

        dedup_key = f"{pos_key}_{sell_fraction:.6f}"
        if dedup_key in self._ws_sell_executed:
            return
        now_mono = time.monotonic()
        self._ws_sell_executed[dedup_key] = now_mono
        # Evict only entries older than 10 minutes so we never drop a guard that
        # is still within a plausible REST-poll window (fix #7).
        if len(self._ws_sell_executed) > 2000:
            cutoff = now_mono - 600  # 10 minutes
            self._ws_sell_executed = {
                k: ts for k, ts in self._ws_sell_executed.items() if ts >= cutoff
            }

        signal_price = float(ev.get("price", 0.0))
        logging.info(
            f"⚡ [WS INSTANT SELL] {position.source_name} | {position.outcome} | "
            f"fraction={sell_fraction:.1%} | our_shares={our_shares_to_sell:.4f} | "
            f"ref_price={signal_price:.4f}"
        )

        await self._execute_sell(
            pos_key         = pos_key,
            position        = position,
            shares_to_sell  = our_shares_to_sell,
            reference_price = signal_price,
            trigger         = "[WS SELL]",
        )

    # ------------------------------------------------------------------ #
    #  Sell helpers                                                        #
    # ------------------------------------------------------------------ #

    async def _execute_sell(
        self,
        pos_key:       str,
        position:      "Position",
        shares_to_sell: float,
        reference_price: float,
        trigger:       str,
    ):
        """
        Sell *shares_to_sell* of *position*, update internal state, and
        record PnL.  Works for both full and partial exits.

        *trigger* is a short label for log lines, e.g. "[WS SELL]" or
        "[REST EXIT]".

        Returns True if the sell order was accepted, False otherwise.
        """
        loop = asyncio.get_running_loop()

        # Fetch a fresh orderbook price for PnL accounting (non-blocking).
        # Use best_ask when available; fall back to reference_price (last known
        # position price) rather than a potentially fabricated mid.
        exit_ask, _ = await loop.run_in_executor(
            None, self.get_orderbook_prices, position.token_id
        )
        exit_price = exit_ask if exit_ask > 0 else reference_price

        ok, _ = await loop.run_in_executor(
            None,
            lambda: self.executor.place_sell(
                position.token_id, shares_to_sell, reference_price=reference_price
            ),
        )
        if not ok:
            logging.warning(f"{trigger} Sell order failed for {pos_key} — will retry on next poll.")
            return False

        realised_pnl = (exit_price - position.entry_price) * shares_to_sell
        is_full_exit = abs(shares_to_sell - position.shares) < 1e-6

        if is_full_exit:
            position.status     = "closed"
            position.exit_price = exit_price
            position.pnl        = realised_pnl

            if self.dry_run:
                self.balance.apply_dry_run_sell(shares_to_sell * exit_price, realised_pnl)
            else:
                self._update_compounding(realised_pnl)

            self.closed_positions.append(position)
            if len(self.closed_positions) > 500:
                self.closed_positions = self.closed_positions[-500:]
            self.positions.pop(pos_key, None)
            logging.info(
                f"📉 {trigger} FULL EXIT {position.source_name} | "
                f"{position.outcome} | exit={exit_price:.4f} | "
                f"pnl={realised_pnl:+.4f} | signal={position.signal_source}"
            )
        else:
            # Partial sell — shrink our position proportionally.
            position.shares   -= shares_to_sell
            position.size_usd  = position.shares * position.entry_price
            position.pnl      += realised_pnl

            if self.dry_run:
                self.balance.apply_dry_run_sell(shares_to_sell * exit_price, realised_pnl)
            else:
                self._update_compounding(realised_pnl)

            logging.info(
                f"✂️  {trigger} PARTIAL EXIT {position.source_name} | "
                f"{position.outcome} | sold={shares_to_sell:.4f} shares | "
                f"remaining={position.shares:.4f} | exit={exit_price:.4f} | "
                f"pnl={realised_pnl:+.4f}"
            )

        return True

    def _get_positions_sync(self, wallet_addr: str) -> Optional[list]:
        url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50"
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, timeout=12)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 404:
                    return []
                else:
                    logging.warning(f"[REST] HTTP {resp.status_code} for {wallet_addr[:10]}")
            except Exception as e:
                logging.warning(f"[REST] Attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return None

    def _get_recent_trades_sync(self, wallet_addr: str) -> set:
        """
        Fetch trade activity for wallet_addr from the last LOOKBACK_HOURS.
        Returns a set of token_ids that were bought within the lookback window.
        Uses data-api.polymarket.com/activity endpoint.
        """
        if not cfg.BOT_START_TIME or cfg.LOOKBACK_HOURS <= 0:
            return set()

        cutoff = cfg.BOT_START_TIME - timedelta(hours=cfg.LOOKBACK_HOURS)
        cutoff_ts = int(cutoff.timestamp())

        url = f"https://data-api.polymarket.com/activity?user={wallet_addr}&limit=100"
        try:
            resp = requests.get(url, timeout=12)
            if resp.status_code != 200:
                logging.warning(f"[LOOKBACK] Activity fetch failed ({resp.status_code}) for {wallet_addr[:10]}")
                return set()

            activities = resp.json()
            recent_tokens = set()

            for activity in activities:
                # Activity timestamp may be in seconds or milliseconds
                ts_raw = activity.get("timestamp", activity.get("createdAt", 0))
                if not ts_raw:
                    continue

                ts = int(ts_raw)
                # Normalise milliseconds -> seconds
                if ts > 1e12:
                    ts = ts // 1000

                # Only include trades within the lookback window
                if ts < cutoff_ts:
                    continue

                # Only care about BUY side trades
                trade_type = (activity.get("type") or activity.get("side") or "").upper()
                if trade_type not in ("BUY", "TRADE", ""):
                    continue

                token_id = activity.get("asset") or activity.get("tokenId") or activity.get("token_id")
                if token_id:
                    recent_tokens.add(token_id)
                else:
                    logging.debug(
                        f"[LOOKBACK] Activity record missing token_id — keys present: {list(activity.keys())}"
                    )

            logging.info(
                f"[LOOKBACK] {wallet_addr[:10]}... -- {len(recent_tokens)} token(s) traded "
                f"in last {cfg.LOOKBACK_HOURS}h (since {cutoff.strftime('%H:%M UTC')})"
            )
            return recent_tokens

        except Exception as e:
            logging.warning(f"[LOOKBACK] Activity fetch error for {wallet_addr[:10]}: {e}")
            return set()

    async def _fetch_all_wallets(self) -> Dict[str, Optional[list]]:
        loop         = asyncio.get_running_loop()
        wallet_addrs = list(cfg.WALLETS.keys())

        # Fetch positions and recent trade activity concurrently
        pos_tasks = [
            loop.run_in_executor(None, self._get_positions_sync, addr)
            for addr in wallet_addrs
        ]
        trade_tasks = [
            loop.run_in_executor(None, self._get_recent_trades_sync, addr)
            for addr in wallet_addrs
        ]

        pos_results   = await asyncio.gather(*pos_tasks,   return_exceptions=True)
        trade_results = await asyncio.gather(*trade_tasks, return_exceptions=True)

        out = {}
        for addr, result in zip(wallet_addrs, pos_results):
            if isinstance(result, Exception):
                logging.warning(f"[REST] Exception for {addr[:10]}: {result}")
                out[addr] = None
            else:
                out[addr] = result

        # Store recent-trade token sets per wallet for lookback filtering
        self._lookback_tokens: Dict[str, set] = {}
        for addr, result in zip(wallet_addrs, trade_results):
            if isinstance(result, Exception) or not isinstance(result, set):
                self._lookback_tokens[addr] = set()
            else:
                self._lookback_tokens[addr] = result

        return out

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(
                    f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8
                )
                if r.status_code == 200:
                    data     = r.json()
                    bids     = data.get("bids", [])
                    asks     = data.get("asks", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    best_ask = float(asks[0]["price"]) if asks else 0.0
                    mid      = (
                        (best_bid + best_ask) / 2
                        if best_bid and best_ask
                        else (best_bid or best_ask or 0.0)
                    )
                    return best_ask, mid
            except Exception as e:
                logging.warning(f"Orderbook request error: {e}")
                time.sleep(1)
        return 0.0, 0.50

    def get_market_question(self, market_id: str) -> str:
        if not market_id or market_id in ("unknown", "pending-ws"):
            return "Polymarket Asset"
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(
                    f"https://clob.polymarket.com/markets/{market_id}", timeout=8
                )
                if r.status_code == 200:
                    return r.json().get("question", "Polymarket Asset")
            except Exception:
                time.sleep(1)
        return "Polymarket Asset"

    async def _reconcile_ws_pending(self, raw_by_wallet: Dict[str, Optional[list]]):
        loop = asyncio.get_running_loop()
        # Snapshot the dict before iterating: the `await` inside the loop yields
        # control to the event loop, where _on_ws_buy_event (or expiry cleanup)
        # could add/remove entries and raise RuntimeError: dictionary changed size
        # during iteration (fix #9).
        pending_snapshot = list(self.pending.items())
        for pos_key, pending in pending_snapshot:
            if pending.market_id != "pending-ws":
                continue
            wallet_raw = raw_by_wallet.get(pending.source_wallet) or []
            for rest_pos in wallet_raw:
                if rest_pos.get("asset") == pending.token_id:
                    market_id = rest_pos.get("conditionId", "unknown")
                    # Offload blocking HTTP call (#10)
                    question  = await loop.run_in_executor(
                        None, self.get_market_question, market_id
                    )
                    pending.market_id = market_id
                    pending.question  = question
                    logging.info(
                        f"[WS→REST] Reconciled pending '{question[:40]}' "
                        f"for {pending.source_name}"
                    )
                    break

    def clean_expired_limit_orders(self):
        now = datetime.now()
        for k, p in list(self.pending.items()):
            if (now - p.placed_at).total_seconds() < LIMIT_EXPIRY_SECONDS:
                continue

            logging.info(
                f"[EXPIRY] Limit order expired for {p.source_name} "
                f"[signal_source={p.signal_source}] — cancelling and repricing…"
            )

            if not self.executor.cancel_order(p.order_id):
                logging.warning(
                    f"[EXPIRY] Cancel failed for {p.source_name} order {p.order_id} — "
                    f"retaining in pending to retry next cycle."
                )
                continue

            if self.dry_run:
                self.balance.apply_dry_run_cancel(p.size_usd)

            # Fetch a fresh orderbook price and repost at the live best_ask.
            # If the orderbook call fails we drop the order rather than posting
            # at a potentially stale price.
            try:
                best_ask, _ = self.get_orderbook_prices(p.token_id)
                if best_ask <= 0 or best_ask >= 1.0:
                    logging.warning(
                        f"[EXPIRY] No valid best_ask for {p.source_name} "
                        f"— dropping order."
                    )
                    del self.pending[k]
                    continue
                new_price = best_ask

                ok, new_order_id, _ = self.executor.place_limit_buy(
                    p.token_id, p.size_usd, new_price
                )
                if ok:
                    if self.dry_run:
                        self.balance.apply_dry_run_buy(p.size_usd)
                    p.order_id   = new_order_id
                    p.limit_price = new_price
                    p.placed_at  = datetime.now()
                    logging.info(
                        f"[EXPIRY] Repriced {p.source_name} order → {new_price:.4f} "
                        f"(was {p.limit_price:.4f})"
                    )
                else:
                    logging.warning(
                        f"[EXPIRY] Reprice order placement failed for {p.source_name} "
                        f"— dropping order."
                    )
                    del self.pending[k]
            except Exception as e:
                logging.warning(f"[EXPIRY] Reprice failed for {p.source_name}: {e} — dropping order.")
                del self.pending[k]

    def process_pending_fills(self):
        for k, p in list(self.pending.items()):
            if self.executor.is_order_filled(p.order_id):
                logging.info(
                    f"✨ [FILL] {p.source_name} | {p.outcome} | "
                    f"signal_source={p.signal_source}"
                )
                self.positions[k] = Position(
                    market_id     = p.market_id,
                    question      = p.question,
                    outcome       = p.outcome,
                    token_id      = p.token_id,
                    entry_price   = p.limit_price,
                    size_usd      = p.size_usd,
                    shares        = round(p.size_usd / p.limit_price, 4),
                    source_wallet = p.source_wallet,
                    source_name   = p.source_name,
                    order_id      = p.order_id,
                    current_price = p.limit_price,
                    signal_source = p.signal_source,
                    source_shares = 0.0,   # populated on the next REST poll
                )
                del self.pending[k]

    async def _drain_ws_price_queue(self):
        drained = 0
        while not self._ws_price_queue.empty():
            try:
                ev = self._ws_price_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            token_id = ev.get("token_id", "")
            price    = ev.get("price", 0.0)
            if token_id and price:
                for pos in self.positions.values():
                    if pos.token_id == token_id:
                        pos.current_price = price
            drained += 1
        if drained:
            logging.debug(f"[WS] Drained {drained} price update(s)")

    async def scan_and_copy(self):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return

        is_broken, dd_pct = self.balance.check_drawdown()
        if is_broken is None:
            # Balance unknown — refuse to poll rather than silently proceeding (#5)
            logging.warning("[REST] Balance unknown — skipping poll cycle until balance is confirmed.")
            return
        if is_broken:
            logging.critical(f"🛑 DRAWDOWN TRIGGERED ({dd_pct*100:.1f}%) — pausing 48 h.")
            cfg.bot_paused_until = datetime.now() + timedelta(hours=48)
            return

        current_bal = self.balance.get_balance()
        if current_bal is None:
            return

        self.clean_expired_limit_orders()
        self.process_pending_fills()

        await self._drain_ws_price_queue()

        logging.info(
            f"Poll | Balance: ${current_bal:.2f} | "
            f"Positions: {len(self.positions)} | "
            f"Pending: {len(self.pending)} | "
            f"WS tokens: {len(self._ws_tracked)}"
        )

        all_wallet_data = await self._fetch_all_wallets()

        await self._reconcile_ws_pending(all_wallet_data)  # now async (#10)

        loop = asyncio.get_running_loop()

        for wallet_addr, config in cfg.WALLETS.items():
            copy_mode = config.get("copy_mode", "new_only")
            if copy_mode not in ("new_only", "all"):
                logging.warning(f"[REST] {config['name']} copy_mode='{copy_mode}' not supported — skipping.")
                continue

            raw = all_wallet_data.get(wallet_addr)
            if raw is None:
                logging.warning(f"[REST] Failed to fetch positions for {config['name']}.")
                continue

            source_token_ids = {
                pos.get("asset") for pos in raw
                if pos.get("asset") and float(pos.get("size", pos.get("shares", 0))) > 0
            }

            logging.info(
                f"[REST] {config['name']} — {len(raw)} position(s), "
                f"{len(source_token_ids)} active tokens"
            )

            if wallet_addr not in self._first_scan_done:
                # Lookback tokens: positions entered within LOOKBACK_HOURS before bot start.
                # Everything else (older positions) is marked seen and skipped.
                lookback_tokens = getattr(self, "_lookback_tokens", {}).get(wallet_addr, set())
                position_assets = {
                    pos.get("asset") for pos in raw
                    if pos.get("asset") and float(pos.get("size", pos.get("shares", 0))) > 0
                }
                matched = lookback_tokens & position_assets
                unmatched = lookback_tokens - position_assets
                logging.info(
                    f"[LOOKBACK MATCH] {config['name']} | activity_tokens={len(lookback_tokens)} | "
                    f"position_assets={len(position_assets)} | matched={len(matched)} | "
                    f"unmatched_from_activity={len(unmatched)}"
                )
                if unmatched:
                    logging.debug(f"[LOOKBACK MATCH] Unmatched token IDs (activity but not in positions): {list(unmatched)[:5]}")

                pre_existing = []
                lookback_eligible = []
                for pos in raw:
                    asset = pos.get("asset")
                    size  = float(pos.get("size", pos.get("shares", 0)))
                    if not asset or size <= 0:
                        continue
                    raw_side = (pos.get("outcome") or pos.get("side") or "YES").upper()
                    pos_key  = f"{wallet_addr.lower()}_{asset}_{raw_side}"

                    if cfg.LOOKBACK_HOURS > 0 and asset in lookback_tokens:
                        # Trade happened within lookback window — copy it
                        lookback_eligible.append(pos_key)
                        logging.info(
                            f"[LOOKBACK] {config['name']} | {raw_side} | {asset[:12]}... "
                            f"— within {cfg.LOOKBACK_HOURS}h window, will attempt to copy"
                        )
                    else:
                        # Trade is older than lookback window — mark seen, skip
                        pre_existing.append(pos_key)

                self.seen.snapshot_existing(pre_existing)

                if lookback_eligible:
                    # Unmark any previously-seen keys so the REST buy loop below
                    # can copy them. Without this, tokens from prior runs that are
                    # stored in the DB seen_trades table will be silently skipped
                    # even though they fall within the lookback window.
                    unmarked = 0
                    for pk in lookback_eligible:
                        if self.seen.is_seen(pk):
                            self.seen.unmark_seen(pk)
                            unmarked += 1
                    if unmarked:
                        logging.info(
                            f"[LOOKBACK] {config['name']} — unmarked {unmarked} previously-seen "
                            f"key(s) so lookback copy can proceed"
                        )
                    logging.info(
                        f"[LOOKBACK] {config['name']} — {len(lookback_eligible)} position(s) "
                        f"eligible for lookback copy, {len(pre_existing)} older position(s) skipped"
                    )
                else:
                    logging.info(
                        f"[LOOKBACK] {config['name']} — no positions within lookback window "
                        f"({cfg.LOOKBACK_HOURS}h). All {len(pre_existing)} existing position(s) skipped."
                    )

                # Token subscriptions are NOT done here.
                # The bot only subscribes tokens it has actually placed an order
                # on — handled in _on_ws_buy_event, _on_ws_order_placed, and the
                # REST buy path.  Bulk-subscribing all source wallet tokens floods
                # the WS stream with events for positions the bot never copied.
                # New tokens are discovered via wallet-address order_placed events
                # on the market channel, which fires before the fill and
                # self-subscribes the token in time to catch it.
                self._first_scan_done.add(wallet_addr)

            for pos in raw:
                token_id  = pos.get("asset")
                shares    = float(pos.get("size", pos.get("shares", 0)))
                side      = (pos.get("outcome") or pos.get("side") or "YES").upper()
                market_id = pos.get("conditionId", "unknown")

                if not token_id or shares <= 0:
                    continue

                pos_key = f"{wallet_addr.lower()}_{token_id}_{side}"

                # Both calls below are blocking HTTP — do them BEFORE acquiring
                # the lock so we never hold _pending_lock across I/O (fix #8).
                best_ask, _ = await loop.run_in_executor(
                    None, self.get_orderbook_prices, token_id
                )

                source_price = float(pos.get("avgPrice", pos.get("price", 0.0)))
                premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                if source_price <= 0:
                    # Source price required for both steps — nothing to anchor on; skip.
                    logging.warning(
                        f"[REST] No source price for {token_id[:12]}… "
                        f"— skipping {config['name']}."
                    )
                    continue
                if best_ask > 0:
                    # Step 1 — orderbook available: post at best_ask, capped so we never
                    # overpay above what the source wallet paid + premium.
                    actual_price = min(best_ask, source_price * (1.0 + premium))
                else:
                    # Step 2 — orderbook unavailable: use source price + premium as proxy.
                    actual_price = source_price * (1.0 + premium)

                if actual_price <= 0 or actual_price >= 1.0:
                    logging.error(f"[REST] Invalid price {actual_price} — skipping.")
                    continue

                # Guard: skip if market has moved more than 50% above the source fill.
                if actual_price > source_price * 1.50:
                    logging.warning(
                        f"[REST] Market moved too far from source price "
                        f"(source={source_price:.4f}, market={actual_price:.4f}) — skipping."
                    )
                    continue

                source_value = float(pos.get("initialValue", pos.get("value", 0.0)))
                my_size = _calc_size(config, actual_price, source_value)
                logging.info(
                    f"[REST SIZING] {config['name']} | {side} | {token_id[:12]}… | "
                    f"bankroll=${cfg.compounding_bankroll:.2f} | price={actual_price:.4f} | "
                    f"source_price={source_price:.4f} | size=${my_size:.2f}"
                )

                current_bal = self.balance.get_balance()
                if current_bal is not None and my_size > current_bal:
                    logging.warning(f"[REST] Order size ${my_size:.2f} exceeds balance ${current_bal:.2f} — skipping.")
                    continue

                # Fetch market question outside the lock (blocking HTTP, fix #8).
                question_str = await loop.run_in_executor(
                    None, self.get_market_question, market_id
                )

                # Acquire lock only for the lightweight guard + state-mutation
                # section.  All blocking I/O has already completed above (fix #8).
                async with self._pending_lock:
                    if self.seen.is_seen(pos_key) or pos_key in self.pending:
                        # WS already handled this signal — REST is correctly suppressed.
                        logging.info(
                            f"[REST SKIP] {config['name']} | {side} | {token_id[:12]}… "
                            f"already seen or pending — skipping"
                        )
                        continue

                    if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                        logging.warning(f"[REST] Position limit reached — skipping REST fallback.")
                        continue

                    # WS did not catch this signal — REST is acting as fallback.
                    # If the market-channel WS is connected this may indicate a
                    # gap (token not yet subscribed, dropped event, or missing
                    # outcome/side fields in the market channel event).
                    ws_active = (
                        self._ws_listener is not None
                        and getattr(self._ws_listener, "_running", False)
                    )
                    if ws_active:
                        logging.warning(
                            f"[REST FALLBACK] {config['name']} | {side} | "
                            f"'{question_str[:40]}' @ {actual_price:.4f} — "
                            f"WS user channel active but missed this signal. "
                            f"token={token_id[:12]}… [check WS event fields]"
                        )
                    else:
                        logging.info(
                            f"🔁 [REST FALLBACK] {config['name']} | {side} | "
                            f"'{question_str[:40]}' @ {actual_price:.4f} "
                            f"[signal_source=rest | WS inactive]"
                        )

                    ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
                    if ok:
                        if self.dry_run:
                            self.balance.apply_dry_run_buy(my_size)

                        self.seen.mark_seen(pos_key)

                        if self._ws_listener and token_id not in self._ws_tracked:
                            asyncio.create_task(self._ws_listener.subscribe_token(token_id))

                        self.pending[pos_key] = PendingLimitBuy(
                            pos_key       = pos_key,
                            token_id      = token_id,
                            market_id     = market_id,
                            question      = question_str,
                            outcome       = side,
                            source_wallet = wallet_addr,
                            source_name   = config["name"],
                            limit_price   = actual_price,
                            size_usd      = my_size,
                            order_id      = order_id,
                            signal_source = "rest",
                        )

            # ── Build a shares map from the REST response ──────────────────────
            # Maps token_id → current source-wallet share count (0 if closed).
            source_shares_map: Dict[str, float] = {
                pos.get("asset"): float(pos.get("size", pos.get("shares", 0)))
                for pos in raw
                if pos.get("asset")
            }

            cur_price_map = {
                pos.get("asset"): float(pos.get("curPrice", 0))
                for pos in raw
                if pos.get("asset") and float(pos.get("curPrice", 0)) > 0
            }
            for _pos in self.positions.values():
                if _pos.source_wallet == wallet_addr and _pos.token_id in cur_price_map:
                    rest_price = cur_price_map[_pos.token_id]
                    if rest_price > 0:
                        _pos.current_price = rest_price

            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.status != "open":
                    continue

                current_source_shares = source_shares_map.get(position.token_id, 0.0)

                # ── Update source_shares baseline on first poll after fill ──
                if position.source_shares <= 0 and current_source_shares > 0:
                    position.source_shares = current_source_shares
                    logging.debug(
                        f"[REST] source_shares initialised for {pos_key}: "
                        f"{current_source_shares:.4f}"
                    )

                # ── Full exit: source closed the position entirely ───────────
                if position.token_id not in source_token_ids:
                    logging.info(
                        f"📉 [REST EXIT] {position.source_name} fully closed — "
                        f"mirroring full sell [signal={position.signal_source}]"
                    )
                    await self._execute_sell(
                        pos_key         = pos_key,
                        position        = position,
                        shares_to_sell  = position.shares,
                        reference_price = position.current_price or position.entry_price,
                        trigger         = "[REST EXIT]",
                    )
                    continue

                # ── Partial sell: source reduced shares ──────────────────────
                prev_shares = position.source_shares
                if prev_shares > 0 and current_source_shares < prev_shares:
                    reduction = prev_shares - current_source_shares
                    fraction  = reduction / prev_shares

                    # Accumulate sub-threshold reductions so a series of small
                    # cuts that together exceed the threshold still triggers a sell.
                    position.pending_reduction += fraction
                    effective_fraction = position.pending_reduction

                    if effective_fraction >= PARTIAL_SELL_THRESHOLD:
                        dedup_key = f"{pos_key}_{effective_fraction:.6f}"
                        if dedup_key in self._ws_sell_executed:
                            logging.debug(
                                f"[REST] Partial sell for {pos_key} already handled "
                                f"by WS ({effective_fraction:.1%}) — skipping REST duplicate."
                            )
                            position.source_shares     = current_source_shares
                            position.pending_reduction = 0.0
                            continue

                        our_shares_to_sell = round(position.shares * effective_fraction, 4)
                        logging.info(
                            f"✂️  [REST PARTIAL] {position.source_name} reduced "
                            f"{effective_fraction:.1%} of position (accumulated) — selling "
                            f"{our_shares_to_sell:.4f} of our "
                            f"{position.shares:.4f} shares"
                        )
                        sold_ok = await self._execute_sell(
                            pos_key         = pos_key,
                            position        = position,
                            shares_to_sell  = our_shares_to_sell,
                            reference_price = position.current_price or position.entry_price,
                            trigger         = "[REST PARTIAL]",
                        )
                        if sold_ok:
                            position.pending_reduction = 0.0
                    else:
                        logging.info(
                            f"[REST PARTIAL] {position.source_name} reduced {fraction:.1%} "
                            f"(accumulated={effective_fraction:.1%}) — below threshold, accumulating."
                        )

                    # Update baseline regardless of whether we acted.
                    position.source_shares = current_source_shares

# ==================== WEB DASHBOARD ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>CopyTrader Dashboard</title>
    <meta http-equiv="refresh" content="15">
    <style>
        *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0d0d0f; color: #e2e8f0; min-height: 100vh; padding: 24px 16px; }}
        .page {{ max-width: 1100px; margin: 0 auto; }}
        .header {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 28px; flex-wrap: wrap; gap: 8px; }}
        .header-title {{ font-size: 1.25rem; font-weight: 700; color: #f8fafc; letter-spacing: -0.3px; }}
        .header-title span {{ color: #6ee7b7; }}
        .badge {{ font-size: 0.72rem; font-weight: 600; padding: 3px 10px; border-radius: 999px; letter-spacing: 0.4px; text-transform: uppercase; }}
        .badge-live   {{ background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }}
        .badge-dry    {{ background: #1e1b4b; color: #a5b4fc; border: 1px solid #312e81; }}
        .badge-paused {{ background: #450a0a; color: #fca5a5; border: 1px solid #7f1d1d; }}
        .badge-ws     {{ background: #083344; color: #67e8f9; border: 1px solid #155e75; }}
        .badge-src-ws       {{ background: #083344; color: #67e8f9; font-size: 0.62rem; padding: 1px 6px; border-radius: 999px; }}
        .badge-src-rest     {{ background: #1e1b4b; color: #a5b4fc; font-size: 0.62rem; padding: 1px 6px; border-radius: 999px; }}
        .timestamp    {{ font-size: 0.75rem; color: #64748b; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }}
        .stat-card {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }}
        .stat-label {{ font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.6px; color: #64748b; margin-bottom: 6px; }}
        .stat-value {{ font-size: 1.6rem; font-weight: 700; color: #f1f5f9; line-height: 1; }}
        .stat-sub {{ font-size: 0.75rem; color: #475569; margin-top: 5px; }}
        .pos {{ color: #34d399; }} .neg {{ color: #f87171; }} .neu {{ color: #94a3b8; }}
        .section {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; margin-bottom: 20px; overflow: hidden; }}
        .section-header {{ display: flex; align-items: center; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid #1e2230; }}
        .section-title {{ font-size: 0.85rem; font-weight: 700; color: #cbd5e1; text-transform: uppercase; letter-spacing: 0.5px; }}
        .count-pill {{ font-size: 0.72rem; font-weight: 700; background: #1e2230; color: #94a3b8; border-radius: 999px; padding: 2px 10px; }}
        .tbl-wrap {{ overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
        thead th {{ padding: 10px 16px; text-align: left; font-size: 0.70rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: #475569; background: #13151a; white-space: nowrap; }}
        tbody tr {{ border-top: 1px solid #1a1d26; transition: background 0.15s; }}
        tbody tr:hover {{ background: #1c1f28; }}
        tbody td {{ padding: 12px 16px; color: #cbd5e1; vertical-align: middle; }}
        .market-name {{ font-weight: 500; color: #e2e8f0; max-width: 280px; }}
        .outcome-pill {{ display: inline-block; font-size: 0.68rem; font-weight: 700; padding: 2px 8px; border-radius: 999px; text-transform: uppercase; }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; }}
        .outcome-no  {{ background: #450a0a; color: #fca5a5; }}
        .source-tag  {{ font-size: 0.70rem; font-weight: 600; color: #818cf8; background: #1e1b4b; padding: 2px 8px; border-radius: 999px; }}
        .price-mono  {{ font-family: 'Courier New', monospace; font-size: 0.80rem; }}
        .pnl-cell    {{ font-weight: 700; font-size: 0.83rem; white-space: nowrap; }}
        .empty {{ padding: 32px 20px; text-align: center; color: #334155; font-size: 0.85rem; }}
        .empty-icon  {{ font-size: 1.8rem; margin-bottom: 8px; }}
    </style>
</head>
<body>
<div class="page">
    <div class="header">
        <div>
            <div class="header-title">🤖 Poly<span>CopyTrader</span></div>
            <div class="timestamp">Updated {last_updated} &nbsp;·&nbsp; Auto-refresh 15s</div>
        </div>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
            <span class="badge {mode_badge}">{mode_label}</span>
            <span class="badge {status_badge}">{status_label}</span>
            <span class="badge badge-ws">⚡ WS {ws_token_count} tokens</span>
        </div>
    </div>
    <div class="stats">
        <div class="stat-card">
            <div class="stat-label">Total Balance</div>
            <div class="stat-value">${balance:.2f}</div>
            <div class="stat-sub">pUSD &nbsp;·&nbsp; Peak ${peak:.2f}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Compounding Bankroll</div>
            <div class="stat-value {comp_cls}">${comp_bankroll:.2f}</div>
            <div class="stat-sub">Sizing base &nbsp;·&nbsp; Rate {comp_rate:.0f}%</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Total PnL</div>
            <div class="stat-value {total_pnl_cls}">{total_pnl_sign}${total_pnl_abs}</div>
            <div class="stat-sub">Realised + Unrealised</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Unrealised</div>
            <div class="stat-value {unreal_cls}">{unreal_sign}${unreal_abs}</div>
            <div class="stat-sub">{open_count} open position(s)</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Realised</div>
            <div class="stat-value {real_cls}">{real_sign}${real_abs}</div>
            <div class="stat-sub">{closed_count} closed trade(s)</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Drawdown</div>
            <div class="stat-value {dd_cls}">{drawdown:.1f}%</div>
            <div class="stat-sub">Max {max_dd:.0f}%</div>
        </div>
    </div>
    <div class="section">
        <div class="section-header">
            <span class="section-title">Open Positions</span>
            <span class="count-pill">{open_count}</span>
        </div>
        {positions_block}
    </div>
    <div class="section">
        <div class="section-header">
            <span class="section-title">Closed Trades</span>
            <span class="count-pill">{closed_count}</span>
        </div>
        {closed_block}
    </div>
</div>
</body>
</html>
"""

def _signal_badge(source: str) -> str:
    cls = {
        "ws":   "badge-src-ws",
        "rest": "badge-src-rest",
    }.get(source, "badge-src-rest")
    return f'<span class="{cls}">{source}</span>'

def build_dashboard(bot) -> dict:
    def _sign(v): return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v):  return "pos" if v > 0 else ("neg" if v < 0 else "neu")

    bankroll  = bot.balance.cached_balance or 0.0
    drawdown  = min(((cfg.peak_bankroll - bankroll) / cfg.peak_bankroll * 100), 100.0) if cfg.peak_bankroll > 0 else 0.0
    is_paused = bool(cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until)

    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label   = "Dry Run" if bot.dry_run else "Live"
    mode_badge   = "badge-dry" if bot.dry_run else "badge-live"

    positions_snapshot = list(bot.positions.values())
    closed_list = list(getattr(bot, "closed_positions", []))

    unrealised = 0.0
    pos_rows   = ""
    for p in positions_snapshot:
        mid    = p.current_price if p.current_price > 0 else p.entry_price
        unreal = (mid - p.entry_price) * p.shares
        unrealised += unreal

        outcome_cls  = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_cls      = _cls(unreal)
        pnl_fmt      = ".4f" if abs(unreal) < 0.005 else ".2f"
        pnl_str      = f"{_sign(unreal)}${abs(unreal):{pnl_fmt}}"
        cur_str      = f"{mid:.3f}" if p.current_price > 0 else "—"
        _src_name    = html.escape(p.source_name)
        _question    = html.escape(p.question[:55])
        _outcome     = html.escape(p.outcome)
        _sig_source  = html.escape(p.signal_source)

        pos_rows += f"""
        <tr>
            <td><span class="source-tag">{_src_name}</span>&nbsp;{_signal_badge(_sig_source)}</td>
            <td class="market-name">{_question}</td>
            <td><span class="outcome-pill {outcome_cls}">{_outcome}</span></td>
            <td>${p.size_usd:.2f}<br><span style="font-size:0.70rem;color:#475569;">{p.shares:.4f} shares</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{cur_str}</td>
            <td class="pnl-cell {pnl_cls}">{pnl_str}</td>
        </tr>"""

    positions_block = (
        f'<div class="tbl-wrap"><table>'
        f'<thead><tr><th>Source</th><th>Market</th><th>Side</th><th>Size</th>'
        f'<th>Entry</th><th>Current</th><th>Unreal PnL</th></tr></thead>'
        f'<tbody>{pos_rows}</tbody></table></div>'
        if pos_rows else
        '<div class="empty"><div class="empty-icon">📭</div>No open positions</div>'
    )

    realised    = sum(p.pnl for p in closed_list)
    closed_rows = ""
    for p in reversed(closed_list):
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_str     = f"{_sign(p.pnl)}${abs(p.pnl):.2f}"
        _src_name   = html.escape(p.source_name)
        _question   = html.escape(p.question[:55])
        closed_rows += f"""
        <tr>
            <td><span class="source-tag">{_src_name}</span>&nbsp;{_signal_badge(p.signal_source)}</td>
            <td class="market-name">{_question}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{p.exit_price:.3f}</td>
            <td class="pnl-cell {_cls(p.pnl)}">{pnl_str}</td>
        </tr>"""

    closed_block = (
        f'<div class="tbl-wrap"><table>'
        f'<thead><tr><th>Source</th><th>Market</th><th>Side</th>'
        f'<th>Entry</th><th>Exit</th><th>Realised PnL</th></tr></thead>'
        f'<tbody>{closed_rows}</tbody></table></div>'
        if closed_rows else
        '<div class="empty"><div class="empty-icon">📋</div>No closed trades yet</div>'
    )

    total_pnl  = realised + unrealised
    def _fmt(v): return f"{abs(v):.4f}" if abs(v) < 0.005 else f"{abs(v):.2f}"
    # comp_delta: growth of the compounding bankroll relative to the peak bankroll
    # baseline. Using cfg.peak_bankroll (updated on every new high-water mark) gives
    # a meaningful green/red signal. The fallback uses the current balance so we
    # never divide by zero or produce a spuriously large positive delta (#13).
    _peak_ref  = cfg.peak_bankroll if cfg.peak_bankroll > 0 else (bankroll or 1.0)
    comp_delta = cfg.compounding_bankroll - _peak_ref

    return {
        "last_updated":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label":     mode_label,
        "mode_badge":     mode_badge,
        "status_label":   status_label,
        "status_badge":   status_badge,
        "ws_token_count": len(bot._ws_tracked),
        "balance":        bankroll,
        "peak":           cfg.peak_bankroll,
        "drawdown":       drawdown,
        "dd_cls":         "neg" if drawdown > 10 else ("neu" if drawdown > 5 else "pos"),
        "max_dd":         MAX_DRAWDOWN * 100,
        "comp_bankroll":  cfg.compounding_bankroll,
        "comp_cls":       _cls(comp_delta),
        "comp_rate":      cfg.COMPOUNDING_RATE * 100,
        "total_pnl_cls":  _cls(total_pnl),
        "total_pnl_sign": _sign(total_pnl),
        "total_pnl_abs":  _fmt(total_pnl),
        "unreal_cls":     _cls(unrealised),
        "unreal_sign":    _sign(unrealised),
        "unreal_abs":     _fmt(unrealised),
        "real_cls":       _cls(realised),
        "real_sign":      _sign(realised),
        "real_abs":       _fmt(realised),
        "open_count":     len(bot.positions),
        "closed_count":   len(closed_list),
        "positions_block": positions_block,
        "closed_block":    closed_block,
    }

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/data" and cfg._bot_ref:
            import json as _json
            bot = cfg._bot_ref
            closed = list(getattr(bot, "closed_positions", []))

            # Per-wallet breakdown
            wallets = {}
            for w_addr, w_cfg in cfg.WALLETS.items():
                name     = w_cfg["name"]
                w_closed = [p for p in closed if p.source_wallet == w_addr]
                w_open   = [p for p in bot.positions.values() if p.source_wallet == w_addr]
                wins     = [p for p in w_closed if p.pnl > 0]
                losses   = [p for p in w_closed if p.pnl <= 0]
                wallets[name] = {
                    "total_pnl": round(sum(p.pnl for p in w_closed), 2),
                    "win_rate":  round(len(wins) / len(w_closed) * 100, 1) if w_closed else 0,
                    "wins":      len(wins),
                    "losses":    len(losses),
                    "open":      len(w_open),
                    "closed_trades": [
                        {
                            "outcome":     p.outcome,
                            "entry_price": round(p.entry_price, 4),
                            "exit_price":  round(p.exit_price, 4),
                            "pnl":         round(p.pnl, 2),
                        }
                        for p in w_closed[-5:]
                    ],
                }

            total_pnl = sum(p.pnl for p in closed)
            all_wins  = [p for p in closed if p.pnl > 0]
            win_rate  = round(len(all_wins) / len(closed) * 100, 1) if closed else 0

            payload = {
                "bankroll":      round(bot.balance.cached_balance or 0, 2),
                "total_pnl":     round(total_pnl, 2),
                "win_rate":      win_rate,
                "open_count":    len(bot.positions),
                "max_positions": cfg.MAX_POSITIONS,
                "wallets":       wallets,
            }

            body = _json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        elif self.path == "/dashboard":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                with open("dashboard.html", "rb") as f:
                    self.wfile.write(f.read())
            except FileNotFoundError:
                self.wfile.write(b"<h1>dashboard.html not found - place it next to bot.py</h1>")

        elif self.path == "/" and cfg._bot_ref:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                data = build_dashboard(cfg._bot_ref)
                html = HTML_TEMPLATE.format(**data)
                self.wfile.write(html.encode())
            except Exception:
                self.wfile.write(b"<h1>Dashboard loading...</h1>")

        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK - CopyTrader V2 running")

    def log_message(self, format, *args):
        pass

def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"🌐 Dashboard live at http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()
