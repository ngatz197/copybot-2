#!/usr/bin/env python3
import os
import time
import json
import logging
import asyncio
import requests
from typing import Tuple, Optional, Set, Callable, Awaitable
import config as cfg

# ==================== OPTIONAL DEPENDENCIES ====================
try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, MarketOrderArgs,
        OrderType, Side, ApiCreds, PartialCreateOrderOptions,
    )
    CLOB_AVAILABLE = True
    logging.info("✅ py_clob_client_v2 loaded successfully")
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py_clob_client_v2 not installed — running in simulation mode.")

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    logging.warning("websockets not installed — WS listener disabled. Run: pip install websockets")

# ==================== ENVIRONMENT / CONSTANTS ====================
YOUR_PRIVATE_KEY      = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET           = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY          = os.getenv("POLY_API_KEY", "")
POLY_SECRET           = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE       = os.getenv("POLY_PASSPHRASE", "")

MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_RETRIES           = 3
RETRY_DELAY           = 5
PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    POLYGON_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ]

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.cached_balance: Optional[float] = None
        self.last_update = 0
        self.peak_balance = 0.0

    def _fetch_balance(self) -> float:
        if not YOUR_WALLET:
            logging.error("DEPOSIT_WALLET_ADDRESS not set — cannot fetch balance")
            return 0.0
        padded  = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0",
            "method":  "eth_call",
            "params":  [{"to": PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded}, "latest"],
            "id":      1,
        }
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    result = resp.json().get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        balance = int(result, 16) / 1_000_000
                        if balance > 0:
                            return balance
            except Exception as e:
                logging.warning(f"RPC balance fetch failed ({rpc}): {e}")
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if self.dry_run and self.cached_balance is not None:
            return self.cached_balance

        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                if real > self.peak_balance:
                    self.peak_balance  = real
                    cfg.peak_bankroll  = real
                    logging.info(f"New peak balance: ${self.peak_balance:.2f}")
            else:
                if self.cached_balance is None:
                    logging.error("Could not fetch real pUSD balance — bot will not trade.")
        return self.cached_balance

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        # NOTE: this MUST remain a plain synchronous method.
        # CopyTrader.__init__ calls it directly (not via await) because __init__
        # cannot be async.  If you need an async version, add a separate
        # async_fetch_with_retry that awaits asyncio.sleep instead.
        for attempt in range(1, retries + 1):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.peak_balance   = val
                self.last_update    = time.time()
                logging.info(f"Real pUSD balance confirmed: ${val:.2f}")
                return val
            logging.warning(f"Balance fetch attempt {attempt}/{retries} returned 0 — retrying...")
            time.sleep(delay)
        raise RuntimeError(f"Could not fetch real pUSD balance after {retries} attempts.")

    def check_drawdown(self) -> Tuple[Optional[bool], float]:
        """
        Returns (is_broken, drawdown_fraction).
        is_broken is None when the balance is unknown — callers must treat
        None as a blocking condition, not as "safe to proceed".
        """
        current = self.get_balance()
        if current is None or self.peak_balance == 0:
            return None, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd

    def apply_dry_run_buy(self, amount_usd: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance -= amount_usd
            # Do NOT touch cfg.compounding_bankroll here — it is the sizing base
            # and should only grow via realised profits on sells (mirrors live mode).
            logging.info(f"[DRY RUN] Deducted virtual funds: ${amount_usd:.2f} | Balance: ${self.cached_balance:.2f}")

    def apply_dry_run_sell(self, return_usd: float, realised_pnl: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance += return_usd
            # Mirror live compounding logic exactly:
            #   Wins:   reinvest only COMPOUNDING_RATE fraction of profit
            #   Losses: absorb the full loss immediately (no dampening)
            if realised_pnl >= 0:
                delta = realised_pnl * cfg.COMPOUNDING_RATE
            else:
                delta = realised_pnl
            cfg.compounding_bankroll = max(cfg.compounding_bankroll + delta, 0.0)
            if cfg.compounding_bankroll > cfg.peak_bankroll:
                cfg.peak_bankroll = cfg.compounding_bankroll
            if self.cached_balance > self.peak_balance:
                self.peak_balance = self.cached_balance
                cfg.peak_bankroll = max(cfg.peak_bankroll, self.cached_balance)
            logging.info(
                f"[DRY RUN] Sell return=${return_usd:.2f} | "
                f"pnl={realised_pnl:+.4f} | delta={delta:+.4f} | "
                f"sizing_base=${cfg.compounding_bankroll:.2f} | "
                f"balance=${self.cached_balance:.2f}"
            )

    def apply_dry_run_cancel(self, amount_usd: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance += amount_usd
            # compounding_bankroll intentionally NOT touched here.
            # A cancelled unfilled order has zero realised PnL — the sizing base
            # must not change. Previously this hard-set compounding_bankroll to
            # cached_balance which wiped accumulated compounding history.
            logging.info(
                f"[DRY RUN] Cancel refund=${amount_usd:.2f} | "
                f"balance=${self.cached_balance:.2f} | "
                f"sizing_base=${cfg.compounding_bankroll:.2f} (unchanged)"
            )

# ==================== EXECUTOR (V2) ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None
        self._dry_run_fill_counter: dict = {}
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(
                    api_key        = POLY_API_KEY,
                    api_secret     = POLY_SECRET,
                    api_passphrase = POLY_PASSPHRASE,
                )
                self.client = ClobClient(
                    host     = "https://clob.polymarket.com",
                    chain_id = 137,
                    key      = YOUR_PRIVATE_KEY,
                    creds    = creds,
                )
                logging.info("ClobClient V2 initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient V2 init failed: {e}")
                self.client = None

    def place_limit_buy(self, token_id: str, amount_usd: float, limit_price: float) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] LIMIT BUY {shares:.4f} shares @ {limit_price:.4f} (${amount_usd:.2f})")
            return True, "dry-run-limit-buy", limit_price
        for attempt in range(MAX_RETRIES):
            try:
                result   = self.client.create_and_post_order(
                    order_args = OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.GTC,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"LIMIT BUY placed (V2): {order_id} | {shares:.4f} shares @ {limit_price:.4f}")
                return True, order_id, limit_price
            except Exception as e:
                logging.warning(f"LIMIT BUY attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, "", limit_price

    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] CANCEL order {order_id}")
            return True
        try:
            self.client.cancel(order_id)
            logging.info(f"Cancelled order {order_id}")
            return True
        except Exception as e:
            logging.warning(f"Cancel failed for {order_id}: {e}")
            return False

    def is_order_filled(self, order_id: str) -> bool:
        if self.dry_run or self.client is None:
            # Simulate a 2-cycle fill delay for more realistic dry-run behaviour.
            count = self._dry_run_fill_counter.get(order_id, 0) + 1
            self._dry_run_fill_counter[order_id] = count
            if count >= 2:
                self._dry_run_fill_counter.pop(order_id, None)
                return True
            return False
        try:
            status = self.client.get_order(order_id).get("status", "").lower()
            return status in ("matched", "filled")
        except Exception as e:
            logging.warning(f"Could not check order status for {order_id}: {e}")
            return False

    def place_sell(self, token_id: str, shares: float, reference_price: float = 0.0) -> Tuple[bool, str]:
        """
        Sell *shares* of token_id.

        Slippage control
        ----------------
        We fetch the live orderbook before placing so we know the best bid.
        We then clamp the limit price to:

            limit_px = max(best_bid, mid * (1 - SELL_LIMIT_MAX_DISCOUNT))

        This means we will never post a sell more than SELL_LIMIT_MAX_DISCOUNT
        below mid-price.  If reference_price > 0 it is used as the mid fallback
        when the orderbook fetch fails (typically the WS signal price).

        Execution sequence
        ------------------
        1. Try a FOK market sell (instant fill at best available bid).
        2. If FOK fails, post a GTC limit sell at the clamped limit price.
        """
        import config as cfg  # imported here to avoid circular at module level

        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] SELL {shares:.4f} shares (ref={reference_price:.4f})")
            return True, "dry-run-sell"

        # ── Fetch live orderbook to derive a slippage-safe limit price ──────
        best_bid = 0.0
        mid      = reference_price  # fallback if orderbook unavailable
        try:
            book     = requests.get(
                f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8
            ).json()
            bids     = book.get("bids", [])
            asks     = book.get("asks", [])
            best_bid = float(bids[0]["price"]) if bids else 0.0
            best_ask = float(asks[0]["price"]) if asks else 0.0
            if best_bid and best_ask:
                mid = (best_bid + best_ask) / 2
            elif best_bid or best_ask:
                mid = best_bid or best_ask
            # else keep reference_price as mid
        except Exception as e:
            logging.warning(f"Orderbook fetch before sell failed: {e} — using reference price")

        if mid <= 0:
            mid = 0.50  # last-resort fallback; should rarely trigger
        floor_px  = round(mid * (1.0 - cfg.SELL_LIMIT_MAX_DISCOUNT), 4)
        limit_px  = max(best_bid, floor_px) if best_bid > 0 else floor_px
        limit_px  = max(limit_px, 0.01)   # never post below 1¢

        logging.info(
            f"[SELL] token={token_id[:12]}… shares={shares:.4f} "
            f"best_bid={best_bid:.4f} mid={mid:.4f} "
            f"floor={floor_px:.4f} limit_px={limit_px:.4f}"
        )

        # --- Attempt 1: FOK market sell (single attempt — fast fill or immediate fallback) ---
        try:
            result   = self.client.create_and_post_market_order(
                order_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL),
                options    = PartialCreateOrderOptions(tick_size="0.01"),
                order_type = OrderType.FOK,
            )
            order_id = result.get("orderID", result.get("id", "unknown"))
            logging.info(f"MARKET SELL placed (FOK): {order_id}")
            return True, order_id
        except Exception as e:
            logging.warning(f"FOK SELL failed: {e} — falling through to GTC limit sell")

        # --- Fallback: GTC limit sell at the slippage-clamped price -----------
        logging.warning(
            f"⚠️  FOK sell failed for token {token_id[:12]}… — "
            f"posting GTC limit sell @ {limit_px:.4f}"
        )
        for attempt in range(MAX_RETRIES):
            try:
                result   = self.client.create_and_post_order(
                    order_args = OrderArgs(
                        token_id = token_id,
                        price    = limit_px,
                        size     = shares,
                        side     = Side.SELL,
                    ),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.GTC,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"GTC limit SELL placed @ {limit_px:.4f}: {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"GTC SELL fallback attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)

        logging.critical(
            f"🚨 ALL SELL ATTEMPTS FAILED for token {token_id[:12]}… "
            f"({shares:.4f} shares).  Position is STUCK — manual intervention required."
        )
        return False, ""

# ==================== WEBSOCKET LISTENER (market channel — wallet trade detection) ====================
class PolymarketWSListener:
    """
    Subscribes to the Polymarket /ws/market channel for two purposes:

    1. Token-level trade events — fires when a tracked token is filled,
       giving maker/taker addresses and direction for copy-trade mirroring.

    2. Wallet-level order_placed events — fires when a tracked wallet posts
       a NEW resting order on ANY token, including tokens we have never seen
       before.  This is the fix for new-token signal latency: we learn about
       a whale's new position at order-placement time (before the fill) and
       immediately self-subscribe the token so the subsequent fill event is
       caught in real time rather than discovered up to POLL_INTERVAL seconds
       later via REST.

    Both subscription types use the same /ws/market channel and the same
    "asset_ids" field — Polymarket accepts wallet addresses there too.
    """

    WS_URL_MARKET  = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    PING_INTERVAL  = 20
    RECONNECT_BASE =  2
    RECONNECT_MAX  = 60

    def __init__(
        self,
        token_ids:              Set[str],
        wallet_addrs:           Set[str],
        ws_price_queue:         asyncio.Queue,
        on_trade_callback:      Optional[Callable[[dict], Awaitable[None]]] = None,
        on_order_placed_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ):
        self.token_ids               = token_ids
        self.wallet_addrs            = wallet_addrs   # tracked source wallets
        self.ws_price_queue          = ws_price_queue
        self.on_trade_callback       = on_trade_callback
        self.on_order_placed_callback = on_order_placed_callback
        self._running                = False
        self._ws_market: Optional[object] = None
        self._subscribed: Set[str]   = set()

    async def subscribe_token(self, token_id: str):
        if token_id in self._subscribed:
            return
        self.token_ids.add(token_id)
        if self._ws_market is not None:
            try:
                await self._send_subscribe(self._ws_market, {token_id})
                logging.info(f"[WS] Live-subscribed token {token_id[:12]}…")
                self._subscribed.add(token_id)
            except Exception as e:
                logging.warning(f"[WS] Live subscribe failed for {token_id[:12]}: {e}")

    async def run(self):
        if not WEBSOCKETS_AVAILABLE:
            logging.warning("[WS] websockets not installed — listener inactive.")
            return
        self._running = True
        await self._run_channel()

    async def _run_channel(self):
        delay = self.RECONNECT_BASE
        while self._running:
            try:
                await self._connect_and_listen()
                delay = self.RECONNECT_BASE
            except Exception as e:
                logging.warning(f"[WS] Disconnected: {e} — reconnecting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.RECONNECT_MAX)

    def stop(self):
        self._running = False

    async def _connect_and_listen(self):
        logging.info(f"[WS] Connecting to {self.WS_URL_MARKET} …")
        async with websockets.connect(
            self.WS_URL_MARKET,
            ping_interval = self.PING_INTERVAL,
            ping_timeout  = 30,
            close_timeout = 10,
        ) as ws:
            self._ws_market = ws
            self._subscribed.clear()
            logging.info("[WS] Connected ✅")

            # Subscribe existing known tokens for fill detection.
            if self.token_ids:
                await self._send_subscribe(ws, self.token_ids)
                self._subscribed.update(self.token_ids)
            else:
                logging.info("[WS] No token_ids yet — awaiting first REST poll.")

            # Subscribe wallet addresses for order_placed detection on new tokens.
            # This fires before the fill so we can self-subscribe the token in time
            # to catch the fill event — eliminating new-token signal latency.
            if self.wallet_addrs:
                await self._send_subscribe(ws, self.wallet_addrs)
                self._subscribed.update(self.wallet_addrs)
                logging.info(f"[WS] Subscribed {len(self.wallet_addrs)} wallet address(es) for order_placed detection")

            async for raw in ws:
                if not self._running:
                    break
                try:
                    await self._handle_message(raw)
                except Exception as e:
                    logging.debug(f"[WS] Message parse error: {e}")
        self._ws_market = None

    async def _send_subscribe(self, ws, ids: Set[str]):
        payload = {
            "type":      "subscribe",
            "channel":   "market",
            "asset_ids": list(ids),
        }
        await ws.send(json.dumps(payload))
        logging.info(f"[WS] Subscribed {len(ids)} id(s)")

    async def _handle_message(self, raw: str):
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:
            return

        if not isinstance(events, list):
            events = [events]

        for ev in events:
            ev_type = ev.get("event_type") or ev.get("type") or ""

            # ── Order placement on a new token (new-token latency fix) ──────────
            # Fires when a tracked wallet posts a resting order on any token —
            # including tokens we have never seen before.  We self-subscribe the
            # token immediately so the subsequent fill event is caught in real
            # time.  We also forward a pre-signal to the engine so it can
            # prepare (fetch orderbook, check limits) before the fill arrives.
            if ev_type in ("order_placed", "order_posted", "new_order"):
                maker_addr = (
                    ev.get("maker_address") or ev.get("maker") or ev.get("owner") or ""
                ).lower()
                token_id = ev.get("asset_id") or ev.get("market") or ""
                price    = float(ev.get("price", 0))
                side     = (ev.get("side") or "").upper()
                outcome  = (ev.get("outcome") or "").upper()

                if not token_id or not maker_addr:
                    continue

                # Only act if this wallet is one we track.
                if maker_addr not in {w.lower() for w in self.wallet_addrs}:
                    continue

                # Self-subscribe the token immediately so we catch the fill.
                if token_id not in self._subscribed:
                    await self.subscribe_token(token_id)
                    logging.info(
                        f"[WS NEW TOKEN] {maker_addr[:10]}… placed order on "
                        f"unseen token {token_id[:12]}… — subscribed ahead of fill"
                    )

                # Forward pre-signal to engine so it can prepare.
                if self.on_order_placed_callback and side == "BUY":
                    await self.on_order_placed_callback({
                        "kind":        "order_placed",
                        "token_id":    token_id,
                        "price":       price,
                        "side":        side,
                        "outcome":     outcome,
                        "maker_addr":  maker_addr,
                    })
                continue

            # ── Price updates ────────────────────────────────────────────────────
            if ev_type in ("price_change", "book", "last_trade_price"):
                token_id = ev.get("asset_id") or ev.get("market") or ""
                price    = (
                    float(ev.get("price", 0))
                    or float(ev.get("mid_price", 0))
                    or float(ev.get("last_trade_price", 0))
                )
                if token_id and price:
                    try:
                        self.ws_price_queue.put_nowait({
                            "kind":     "price_update",
                            "token_id": token_id,
                            "price":    price,
                        })
                    except asyncio.QueueFull:
                        try:
                            self.ws_price_queue.get_nowait()
                            self.ws_price_queue.put_nowait({
                                "kind": "price_update", "token_id": token_id, "price": price,
                            })
                        except Exception:
                            pass

            # ── Fill events ──────────────────────────────────────────────────────
            elif ev_type in ("trade", "order_filled"):
                token_id   = ev.get("asset_id") or ev.get("market") or ""
                price      = float(ev.get("price", 0))
                size       = float(ev.get("size", 0))
                outcome    = (ev.get("outcome") or "").upper()
                maker_addr = (ev.get("maker_address") or ev.get("maker") or "").lower()
                taker_addr = (ev.get("taker_address") or ev.get("taker") or "").lower()
                maker_side = (ev.get("maker_side") or "").upper()
                taker_side = (ev.get("taker_side") or "").upper()

                if token_id and price and self.on_trade_callback:
                    await self.on_trade_callback({
                        "kind":       "trade",
                        "token_id":   token_id,
                        "price":      price,
                        "size":       size,
                        "outcome":    outcome,
                        "maker_addr": maker_addr,
                        "taker_addr": taker_addr,
                        "maker_side": maker_side,
                        "taker_side": taker_side,
                    })

# ==================== USER CHANNEL LISTENER (your wallet only — order-fill confirmation) ====================
class PolymarketUserChannelListener:
    """
    Connects to the Polymarket user channel for YOUR OWN wallet only.

    The user channel is an authenticated, private endpoint — it rejects any
    connection that is not scoped to the authenticated wallet.  Attempting to
    subscribe to third-party wallets causes an immediate server-side TCP drop,
    which is why the old multi-wallet approach produced an instant reconnect loop.

    This class is now used exclusively for monitoring your own order fills
    (confirmations that a BUY or SELL you placed has been matched).  Source-wallet
    *detection* — i.e. spotting when a tracked whale buys or sells — is handled
    entirely by PolymarketWSListener on the public /ws/market channel.

    Authentication
    --------------
    Polymarket's user channel requires a signed auth message sent immediately
    after the WebSocket handshake, before any subscribe message.  The signature
    is an L1 EIP-712 message signed with the wallet's private key via the CLOB
    client library.  Without it the server closes the connection immediately.

    If CLOB credentials are not configured (dry-run or missing env vars) this
    listener logs a warning and stays idle — the REST poller and market-channel
    WS handle all required state.
    """

    WS_URL_USER    = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    PING_INTERVAL  = 20
    RECONNECT_BASE =  2
    RECONNECT_MAX  = 60

    def __init__(
        self,
        on_fill_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ):
        """
        on_fill_callback receives a dict:
            {
                "kind":      "own_fill",
                "token_id":  str,
                "price":     float,
                "size":      float,
                "side":      "BUY" | "SELL",
                "order_id":  str,
            }
        """
        self.on_fill_callback = on_fill_callback
        self._running         = False
        self._ws: Optional[object] = None
        self._own_wallet      = YOUR_WALLET.lower() if YOUR_WALLET else ""

    async def run(self):
        if not WEBSOCKETS_AVAILABLE:
            logging.warning("[USER-WS] websockets not installed — user channel inactive.")
            return
        if not YOUR_PRIVATE_KEY or not YOUR_WALLET:
            logging.warning(
                "[USER-WS] PRIVATE_KEY / DEPOSIT_WALLET_ADDRESS not set — "
                "user channel inactive.  Own-fill confirmations will come from REST polling only."
            )
            return
        if not CLOB_AVAILABLE:
            logging.warning(
                "[USER-WS] py_clob_client_v2 not installed — cannot sign auth message. "
                "User channel inactive."
            )
            return
        self._running = True
        await self._run_channel()

    def stop(self):
        self._running = False

    async def _run_channel(self):
        delay = self.RECONNECT_BASE
        while self._running:
            try:
                await self._connect_and_listen()
                # Clean exit — reset backoff.
                delay = self.RECONNECT_BASE
            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
            ) as e:
                logging.info(
                    f"[USER-WS] Connection closed ({e}) — "
                    f"reconnecting in {delay}s"
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.RECONNECT_MAX)
            except Exception as e:
                logging.warning(f"[USER-WS] Disconnected: {e} — reconnecting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.RECONNECT_MAX)

    def _build_auth_message(self) -> Optional[str]:
        """
        Build the authentication payload required by Polymarket's user
        channel.  The user channel only needs L2 API credentials; no
        L1 signature is required for the WebSocket subscription.
        Returns None if credentials are missing.
        """
        if not POLY_API_KEY or not POLY_SECRET or not POLY_PASSPHRASE:
            logging.error("[USER-WS] API credentials not fully configured — cannot build auth message")
            return None
        return json.dumps({
            "type":    "auth",
            "channel": "user",
            "auth": {
                "apiKey":     POLY_API_KEY,
                "secret":     POLY_SECRET,
                "passphrase": POLY_PASSPHRASE,
            },
            "markets": [self._own_wallet],
        })

    async def _connect_and_listen(self):
        loop        = asyncio.get_running_loop()
        auth_msg    = await loop.run_in_executor(None, self._build_auth_message)
        if auth_msg is None:
            logging.warning("[USER-WS] Auth message unavailable — waiting 60s before retry.")
            await asyncio.sleep(60)
            return

        logging.info(f"[USER-WS] Connecting to {self.WS_URL_USER} …")
        async with websockets.connect(
            self.WS_URL_USER,
            ping_interval = self.PING_INTERVAL,
            ping_timeout  = 30,
            close_timeout = 5,
            max_size      = 2 ** 23,
            open_timeout  = 15,
        ) as ws:
            self._ws = ws
            logging.info("[USER-WS] Connected ✅")

            # Send signed auth immediately — server closes without it.
            await ws.send(auth_msg)
            logging.info(f"[USER-WS] Auth sent for own wallet {self._own_wallet[:10]}…")

            async for raw in ws:
                if not self._running:
                    break
                try:
                    await self._handle_message(raw)
                except Exception as e:
                    logging.debug(f"[USER-WS] Message parse error: {e}")
        self._ws = None

    async def _handle_message(self, raw: str):
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:
            return

        if not isinstance(events, list):
            events = [events]

        for ev in events:
            ev_type = (ev.get("event_type") or ev.get("type") or "").lower()

            # Only care about confirmed fills on our own orders.
            if ev_type not in ("order_fill", "order_filled", "trade"):
                continue

            token_id = ev.get("asset_id") or ev.get("market") or ""
            price    = float(ev.get("price", 0))
            size     = float(ev.get("size", 0))
            side     = (ev.get("side") or "").upper()
            order_id = ev.get("id") or ev.get("order_id") or ""

            if not token_id or not price:
                continue

            logging.debug(
                f"[USER-WS] Own fill: {side} {size:.4f} @ {price:.4f} "
                f"token={token_id[:12]}… order={order_id[:12]}…"
            )

            if self.on_fill_callback:
                await self.on_fill_callback({
                    "kind":     "own_fill",
                    "token_id": token_id,
                    "price":    price,
                    "size":     size,
                    "side":     side,
                    "order_id": order_id,
                })
