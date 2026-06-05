import os
import logging
from datetime import datetime
from typing import Optional, Dict
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ==================== OPERATIONAL FLAGS ====================
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() == "true"
# Background polling slowed down; main path is now pure event-driven WebSockets
POLL_INTERVAL    = int(os.getenv("POLL_SECONDS", "15")) 
COMPOUNDING_RATE = float(os.getenv("COMPOUNDING_RATE", "0.10"))
MAX_DRAWDOWN     = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT      = int(os.getenv("PORT", "8080"))

# ==================== HIGH-PERFORMANCE SNAP-TARGET MATRIX ====================
# copy_mode set to "all" to dynamically catch position additions and accumulations
WALLETS: Dict[str, dict] = {
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {
        "name": "TheSpirit",
        "risk_type": "price_based",
        "copy_mode": "all",
    },
    "0x2005d16a84ceefa912d4e380cd32e7ff827875ea": {
        "name": "RN",
        "risk_type": "price_based",
        "copy_mode": "all",
    },
    "0xf903c4cd098184e67a06a04f9b8fdb36e7bbe028": {
        "name": "Viser",
        "risk_type": "price_based",
        "copy_mode": "all",
    },
    "0xe8ca3f758c93f44f3ec210542ab78afb7c0bcccb": {
        "name": "Kruto",
        "risk_type": "price_based",
        "copy_mode": "all",
        "limit_buy_max_premium": 0.10,
        "copy_sub_dollar": True,
    },
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {
        "name": "Coniyr",
        "risk_type": "fixed",
        "fixed_risk": 0.025,
        "copy_mode": "all",
    },
}

# ==================== SECRETS & REQS ====================
YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL     = os.getenv("DATABASE_URL", "")

INITIAL_BANKROLL      = float(os.getenv("INITIAL_BANKROLL", "100.0"))  # Override via env; used as fallback until live balance is fetched
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "8"))
PAUSE_HOURS           = 48
MAX_RETRIES           = 3
RETRY_DELAY           = 5

LIMIT_BUY_MAX_PREMIUM   = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.08"))
LIMIT_EXPIRY_SECONDS    = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE        = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
PUSD_CONTRACT_ADDRESS   = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

PARTIAL_SELL_THRESHOLD  = float(os.getenv("PARTIAL_SELL_THRESHOLD", "0.20"))

# ==================== POLYGUN EXECUTION TUNING ====================
STRICT_PRICE_MATCH      = True 
SELL_LIMIT_MAX_DISCOUNT = 0.00  # Zero leeway; match exact exit parameters or pass

# ==================== RUNTIME SYSTEM HOOKS ====================
bot_paused_until:     Optional[datetime] = None
compounding_bankroll: float = 0.0   
peak_bankroll:        float = 0.0   
_bot_ref                    = None
