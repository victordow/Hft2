#!/usr/bin/env python3
"""
HFT Simulation Lab v2 — Polymarket NegRisk edge survey (VPS standalone)

Mudancas em relacao ao v1 (ver AUDITORIA.md para rationale completo):

  * Persist INCREMENTAL (so o delta desde o ultimo batch) -- fim do
    double-count que inflacionava P&L em ~30x.
  * Cooldown baseado em JANELA (opportunity aberta vs. fechada), nao
    timer de 3s -- mesmo mercado so gera nova detecao quando o edge
    volta a zero e reabre.
  * Filtro ghost/terminal/dust multi-camada POR SNAPSHOT (nao por
    mercado) -- mercado pode estar ruim agora e bom em 2h.
  * Book DEPTH real calculado de price*size em todos os levels (v1
    usava sum_bid*50, um placeholder que sempre caia em <$200).
  * price_change handler aplica deltas ao book local (v1 lia campos
    que nao existem no payload).
  * Edge DECAY real: scheduler aguarda, le snapshot que so foi criado
    a partir de update real do WS pos-detecao (v1 pegava cache
    rematado a cada 200ms).
  * net_edge = gross_edge - fee_load (aproximacao upper-bound de
    n*fee_rate*0.25); DetectionRecord persiste os dois.
  * Shutdown graceful via asyncio.Event (v1 usava bool que demorava
    ate 2min para propagar).
  * status.json reescrito a cada 30s para monitoramento mobile.
  * Sanity alerts em tempo real (edge P90>10%, book locked >5%, etc).
  * Exceptions de WS logadas com tipo + mensagem (v1 engolia).
  * Re-discovery de mercados a cada 30min (v1 selecionava uma vez
    e rodava com survivorship bias).
  * Metrica FAKE de actual_delay removida -- substituida por
    surviving_edge_ratio contra snapshot pos-update.
  * MIN_DAYS = 1.0 (v1 era 2h, fabrica de ghost em mercados resolvendo).

Uso:
    python3 hft_sim_v2.py                   # 16h default, 80 mercados
    python3 hft_sim_v2.py --hours 2         # teste rapido
    python3 hft_sim_v2.py --markets 120     # mais mercados

Para rodar em VPS via SSH (Terminus, etc):
    tmux new -s hft
    python3 hft_sim_v2.py --hours 16
    # Ctrl+B depois D -> detach, pode fechar SSH
    # tmux attach -t hft -> voltar a sessao
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
import traceback
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Deque, Optional

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    print("ERROR: pandas nao instalado. Rode: pip install pandas pyarrow", file=sys.stderr)
    HAS_PANDAS = False

getcontext().prec = 28

# ============================================================
# CONSTANTES
# ============================================================

GAMMA = "https://gamma-api.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Fees por categoria (Polymarket, regime vigente a partir de 30/mar/2026).
FEE_RATE: dict[str, Decimal] = {
    "crypto": Decimal("0.0720"),
    "economics": Decimal("0.0600"),
    "mentions": Decimal("0.0624"),
    "culture": Decimal("0.0500"),
    "weather": Decimal("0.0500"),
    "pop-culture": Decimal("0.0500"),
    "climate": Decimal("0.0500"),
    "science": Decimal("0.0500"),
    "finance": Decimal("0.0400"),
    "politics": Decimal("0.0400"),
    "tech": Decimal("0.0400"),
    "elections": Decimal("0.0400"),
    "sports": Decimal("0.0300"),
    "geopolitics": Decimal("0"),
    "middle-east": Decimal("0"),
}
DEFAULT_FEE = Decimal("0.0720")

# --- Parametros de simulacao ---
DEFAULT_DURATION_HOURS = 16.0
DEFAULT_N_MARKETS = 80
MAX_TOKENS_PER_CONN = 50

# Universe selection (operador aprovou: NAO cortar 30-40% por volume24hr)
VOL_MIN_TOTAL = Decimal("5000")
VOL_MAX_TOTAL = Decimal("5000000")
MIN_OUTCOMES = 2
MAX_OUTCOMES = 20
MIN_DAYS = Decimal("1.0")      # v1 era 0.0833 (2h). Eleva para 1d.
MAX_DAYS = Decimal("180")

# Intervalos
DETECTION_INTERVAL_SEC = 0.5
SNAPSHOT_INTERVAL_SEC = 0.2
STATUS_INTERVAL_SEC = 30
PERSIST_INTERVAL_SEC = 900         # 15 min (v1: 120s)
REDISCOVERY_INTERVAL_SEC = 1800    # 30 min (v1: nunca)
STALENESS_MS = 30_000

# Edge decay (substitui a metrica fake de actual_delay do v1)
DECAY_TARGETS_MS = [300, 1000, 2000, 5000]
MAX_ACTIVE_DECAY_TASKS = 2000

# Deteccao (thresholds)
MIN_GROSS_EDGE = Decimal("0.002")  # 0.2% gross (abaixo eh ruido)
SNAPSHOT_HISTORY_LEN = 300          # ~60s a 200ms (v1: 10s, insuficiente)

# Ghost/terminal filters (per-snapshot)
MAX_SPREAD_PER_OUTCOME = Decimal("0.30")
GHOST_DUST_ASK = Decimal("0.02")
GHOST_DUST_BID = Decimal("0.01")
MIN_LEVEL_NOTIONAL_USD = Decimal("1.0")  # dust: price*size < $1 ignorado
SUM_BID_RANGE = (Decimal("0.5"), Decimal("1.5"))  # sanity NegRisk

# Cooldown por janela (nao por tempo)
MIN_OPPORTUNITY_GAP_MS = 2000  # entre "fechou" e considera "reabriu"

# Sanity alerts
ALERT_P90_EDGE_THRESHOLD = Decimal("0.10")  # 10%
ALERT_EVENT_CONCENTRATION = 0.50            # mesmo event >50% detecoes
ALERT_BUCKET_CONCENTRATION = 0.90           # mesmo bucket >90% detecoes
ALERT_LOCKED_BOOK_RATE = 0.05               # bid>=ask em >5% outcomes
ALERT_ROLLING_WINDOW_SEC = 600              # janela de 10min

# ============================================================
# UTILS
# ============================================================

def to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        return Decimal(int(v))
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return Decimal(s)
        except Exception:
            return None
    return None


d = to_decimal


def now_ms() -> int:
    return int(time.time() * 1000)


def infer_category(tags: list) -> str:
    """Ordem de prioridade: mais especifico primeiro."""
    labels = [
        (t.get("label") or t.get("slug") or "").lower()
        for t in (tags or [])
        if isinstance(t, dict)
    ]
    priority = [
        "geopolitics", "middle-east",
        "crypto",
        "sports",
        "elections",      # antes de "politics" (mais especifico)
        "weather", "climate",
        "culture", "pop-culture",
        "tech", "science",
        "finance", "economics",
        "politics",
        "mentions",
    ]
    for p in priority:
        for label in labels:
            if p in label:
                return p
    return "unknown"


def fee_for(cat: str) -> Decimal:
    return FEE_RATE.get(cat, DEFAULT_FEE)


def fee_load_approx(fee_rate: Decimal, n_outcomes: int) -> Decimal:
    """
    Upper bound do fee load agregado em p=0.5 (pior caso individual):
        fee_load_max = n_outcomes * fee_rate * 0.25
    Conservador (super-estima ligeiramente).
    """
    return Decimal(n_outcomes) * fee_rate * Decimal("0.25")


def parse_end_date(raw: Any) -> Optional[datetime]:
    if not raw or not isinstance(raw, str):
        return None
    try:
        s = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def days_until(end_dt: Optional[datetime]) -> Optional[Decimal]:
    if end_dt is None:
        return None
    delta = end_dt - datetime.now(timezone.utc)
    days = Decimal(str(delta.total_seconds())) / Decimal("86400")
    return days if days > 0 else Decimal("0")


def volume_bucket(v: Decimal) -> str:
    f = float(v)
    if f < 10_000:
        return "<$10K"
    if f < 50_000:
        return "$10-50K"
    if f < 200_000:
        return "$50-200K"
    if f < 1_000_000:
        return "$200K-1M"
    return ">$1M"


def book_depth_bucket(d_val: Decimal) -> str:
    f = float(d_val)
    if f < 200:
        return "<$200"
    if f < 1000:
        return "$200-1k"
    if f < 3000:
        return "$1-3k"
    return ">$3k"


def time_bucket(hours: float) -> str:
    if hours < 24:
        return "<24h"
    if hours < 7 * 24:
        return "1-7d"
    if hours < 30 * 24:
        return "7-30d"
    return ">30d"


# ============================================================
# DATACLASSES
# ============================================================

@dataclass
class BookLevel:
    price: Decimal
    size: Decimal

    @property
    def notional(self) -> Decimal:
        return self.price * self.size


@dataclass
class OutcomeState:
    """Estado de 1 outcome (YES-token). Guarda book completo, nao so best."""
    event_idx: int
    question: str
    yes_token: str
    bids: list[BookLevel] = field(default_factory=list)  # ordenado desc por price
    asks: list[BookLevel] = field(default_factory=list)  # ordenado asc por price
    last_update_ms: int = 0

    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks[0].price if self.asks else None

    @property
    def book_notional_usd(self) -> Decimal:
        bid_not = sum((lvl.notional for lvl in self.bids), start=Decimal("0"))
        ask_not = sum((lvl.notional for lvl in self.asks), start=Decimal("0"))
        return bid_not + ask_not


@dataclass
class EventState:
    idx: int
    title: str
    category: str
    fee_rate: Decimal
    volume: Decimal
    outcomes: list[OutcomeState]
    days_until_resolution: Decimal
    volume_bucket_label: str


@dataclass(frozen=True)
class BookSnapshot:
    """
    based_on_update_ts = max(outcome.last_update_ms).
    Usado como "data real" do snapshot (v1 usava now_ms, que induzia
    a ideia de que snapshots recombinados do cache eram novos).
    """
    timestamp_ms: int
    based_on_update_ts: int
    sum_ask: Decimal
    sum_bid: Decimal
    total_book_depth_usd: Decimal
    locked_outcomes: int
    rejected_snapshot: bool


@dataclass
class DetectionRecord:
    detection_id: int
    detected_at_ms: int
    event_idx: int
    event_title: str
    category: str
    fee_rate: float
    n_outcomes: int
    volume_bucket: str
    time_bucket_label: str
    days_until_resolution: float
    direction: str                  # LONG | SHORT
    initial_sum_ask: float
    initial_sum_bid: float
    initial_gross_edge: float
    initial_fee_load_approx: float
    initial_net_edge: float
    book_depth_usd: float
    book_depth_bucket: str
    closed_at_ms: Optional[int] = None
    persisted_ticks: int = 0


@dataclass
class DecayMeasurement:
    detection_id: int
    target_ms: int
    measured_at_ms: int
    book_source_update_ms: Optional[int]
    book_available: bool
    surviving_gross_edge: Optional[float]
    surviving_direction_matches: bool


# ============================================================
# LAB
# ============================================================

class Lab:
    def __init__(self, duration_sec: float, n_markets: int, output_dir: Path):
        self.duration_sec = duration_sec
        self.n_markets = n_markets
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # State
        self.events: list[EventState] = []
        self.token_to_outcome: dict[str, OutcomeState] = {}
        self.snapshot_history: dict[int, Deque[BookSnapshot]] = defaultdict(
            lambda: deque(maxlen=SNAPSHOT_HISTORY_LEN)
        )
        self.detections: list[DetectionRecord] = []
        self.decays: list[DecayMeasurement] = []

        # Opportunity window tracking
        self.open_opportunities: dict[tuple[int, str], DetectionRecord] = {}
        self.last_close_ms: dict[tuple[int, str], int] = {}

        # Persistence cursors (incremental, fix do bug #15 do v1)
        self._persisted_detections_cursor: int = 0
        self._persisted_decays_cursor: int = 0
        self._persist_seq: int = 0

        # WS / tasks
        self.conn_stats: dict[int, dict[str, int]] = defaultdict(
            lambda: {"events": 0, "reconnects": 0, "last_event_ms": 0}
        )
        self.ws_subscriptions: dict[int, list[str]] = {}
        self._next_detection_id: int = 0
        self._active_decay_tasks: int = 0

        # Control
        self.shutdown_event: Optional[asyncio.Event] = None  # criado dentro do loop
        self.start_time: Optional[float] = None

        # Logging
        self.log_path = output_dir / "lab.log"
        self.jsonl_path = output_dir / "events.jsonl"
        self.status_path = output_dir / "status.json"
        self.log_file = open(self.log_path, "a", buffering=1)
        self.jsonl_file = open(self.jsonl_path, "a", buffering=1)

        # Sanity alerts
        self.active_alerts: dict[str, str] = {}

    def ensure_shutdown_event(self) -> asyncio.Event:
        if self.shutdown_event is None:
            self.shutdown_event = asyncio.Event()
        return self.shutdown_event

    def log(self, msg: str, level: str = "INFO") -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{ts}] [{level}] {msg}"
        print(line, flush=True)
        try:
            self.log_file.write(line + "\n")
        except Exception:
            pass

    def log_event(self, event_type: str, payload: dict) -> None:
        record = {"ts_ms": now_ms(), "type": event_type, **payload}
        try:
            self.jsonl_file.write(json.dumps(record, default=str) + "\n")
        except Exception:
            pass

    def next_detection_id(self) -> int:
        self._next_detection_id += 1
        return self._next_detection_id

    def close(self) -> None:
        try:
            self.log_file.close()
        except Exception:
            pass
        try:
            self.jsonl_file.close()
        except Exception:
            pass


# ============================================================
# GHOST FILTERS (POR SNAPSHOT, NAO POR MERCADO)
# ============================================================

def filter_dust_levels(levels: list[BookLevel]) -> list[BookLevel]:
    return [lvl for lvl in levels if lvl.notional >= MIN_LEVEL_NOTIONAL_USD]


def outcome_passes_sanity(o: OutcomeState, cur_ms: int) -> tuple[bool, str]:
    bb = o.best_bid
    ba = o.best_ask
    if bb is None or ba is None:
        return False, "missing_bid_or_ask"
    if bb >= ba:
        return False, "locked_or_crossed"
    spread = ba - bb
    if spread > MAX_SPREAD_PER_OUTCOME:
        return False, "spread_too_wide"
    if ba <= GHOST_DUST_ASK and bb <= GHOST_DUST_BID:
        return False, "ghost_dust"
    if cur_ms - o.last_update_ms > STALENESS_MS:
        return False, "stale"
    non_dust_bids = filter_dust_levels(o.bids)
    non_dust_asks = filter_dust_levels(o.asks)
    if not non_dust_bids or not non_dust_asks:
        return False, "all_levels_dust"
    return True, ""


def compute_snapshot(ev: EventState, cur_ms: int) -> BookSnapshot:
    """
    Retorna rejected_snapshot=True se NegRisk nao esta saudavel.
    """
    locked_count = sum(
        1 for o in ev.outcomes
        if o.best_bid is not None and o.best_ask is not None and o.best_bid >= o.best_ask
    )
    motivos: list[str] = []
    for o in ev.outcomes:
        ok, motivo = outcome_passes_sanity(o, cur_ms)
        if not ok:
            motivos.append(motivo)

    if motivos:
        return BookSnapshot(
            timestamp_ms=cur_ms,
            based_on_update_ts=max((o.last_update_ms for o in ev.outcomes), default=0),
            sum_ask=Decimal("0"),
            sum_bid=Decimal("0"),
            total_book_depth_usd=Decimal("0"),
            locked_outcomes=locked_count,
            rejected_snapshot=True,
        )

    sum_ask = sum((o.best_ask for o in ev.outcomes), start=Decimal("0"))
    sum_bid = sum((o.best_bid for o in ev.outcomes), start=Decimal("0"))

    rejected = False
    if not (SUM_BID_RANGE[0] <= sum_bid <= SUM_BID_RANGE[1]):
        rejected = True
    if sum_ask < sum_bid:
        rejected = True

    total_depth = Decimal("0")
    for o in ev.outcomes:
        for lvl in filter_dust_levels(o.bids):
            total_depth += lvl.notional
        for lvl in filter_dust_levels(o.asks):
            total_depth += lvl.notional

    based_on_update = max((o.last_update_ms for o in ev.outcomes), default=0)
    return BookSnapshot(
        timestamp_ms=cur_ms,
        based_on_update_ts=based_on_update,
        sum_ask=sum_ask,
        sum_bid=sum_bid,
        total_book_depth_usd=total_depth,
        locked_outcomes=locked_count,
        rejected_snapshot=rejected,
    )


def check_deviation(snap: BookSnapshot) -> Optional[tuple[str, Decimal]]:
    if snap.rejected_snapshot:
        return None
    if snap.sum_ask < Decimal("1"):
        return ("LONG", Decimal("1") - snap.sum_ask)
    if snap.sum_bid > Decimal("1"):
        return ("SHORT", snap.sum_bid - Decimal("1"))
    return None


# ============================================================
# WEBSOCKET — handlers
# ============================================================

def _parse_levels(raw_levels: list) -> list[BookLevel]:
    out = []
    for raw in raw_levels or []:
        p = to_decimal(raw.get("price"))
        s = to_decimal(raw.get("size"))
        if p is None or s is None or s <= 0:
            continue
        out.append(BookLevel(price=p, size=s))
    return out


def _sort_levels(bids: list[BookLevel], asks: list[BookLevel]) -> tuple[list, list]:
    return (
        sorted(bids, key=lambda x: x.price, reverse=True),
        sorted(asks, key=lambda x: x.price),
    )


def handle_book_event(lab: Lab, m: dict, ts: int) -> None:
    aid = m.get("asset_id")
    o = lab.token_to_outcome.get(aid)
    if not o:
        return
    bids = _parse_levels(m.get("bids") or [])
    asks = _parse_levels(m.get("asks") or [])
    bids, asks = _sort_levels(bids, asks)
    o.bids = bids
    o.asks = asks
    o.last_update_ms = ts


def handle_best_bid_ask_event(lab: Lab, m: dict, ts: int) -> None:
    """
    Update apenas do top-of-book. Se o novo best nao bate com topo do
    livro local, substituimos por placeholder; sera sobrescrito quando
    vier proximo book completo.
    """
    aid = m.get("asset_id")
    o = lab.token_to_outcome.get(aid)
    if not o:
        return
    bb = to_decimal(m.get("best_bid"))
    ba = to_decimal(m.get("best_ask"))
    if bb is not None:
        if not o.bids or o.bids[0].price != bb:
            o.bids = [BookLevel(price=bb, size=Decimal("1.0"))]
    if ba is not None:
        if not o.asks or o.asks[0].price != ba:
            o.asks = [BookLevel(price=ba, size=Decimal("1.0"))]
    o.last_update_ms = ts


def handle_price_change_event(lab: Lab, m: dict, ts: int) -> None:
    """
    Aplica deltas ao book local. CORRIGE BUG #9 DO V1 (que lia
    best_bid/best_ask de um payload que nao tem esses campos).

    Schema real:
        {
          "asset_id": "...",
          "changes": [{"price": "...", "side": "BUY"|"SELL", "size": "..."}]
        }
    side BUY = bid, SELL = ask. size=0 remove level.
    """
    aid = m.get("asset_id")
    o = lab.token_to_outcome.get(aid)
    if not o:
        return
    changes = m.get("changes") or m.get("price_changes") or []
    if not changes:
        return
    bids_by_price = {lvl.price: lvl for lvl in o.bids}
    asks_by_price = {lvl.price: lvl for lvl in o.asks}
    for ch in changes:
        p = to_decimal(ch.get("price"))
        s = to_decimal(ch.get("size"))
        side = (ch.get("side") or "").upper()
        if p is None or s is None:
            continue
        if side in ("BUY", "BID"):
            if s == 0:
                bids_by_price.pop(p, None)
            else:
                bids_by_price[p] = BookLevel(price=p, size=s)
        elif side in ("SELL", "ASK"):
            if s == 0:
                asks_by_price.pop(p, None)
            else:
                asks_by_price[p] = BookLevel(price=p, size=s)
    bids, asks = _sort_levels(list(bids_by_price.values()), list(asks_by_price.values()))
    o.bids = bids
    o.asks = asks
    o.last_update_ms = ts


def normalize_ws_timestamp(raw: Any) -> int:
    """
    Polymarket CLOB WS pode mandar timestamp em SEGUNDOS (string), nao ms.
    Observacao empirica (run de 20/abr/2026): 68k rejeicoes stale_outcome em
    12min com STALENESS_MS=30000 -- confirma s, nao ms.

    Threshold 10^11: abaixo -> segundos (multiplica por 1000). Unix ts
    atual em s ~1.8e9, em ms ~1.8e12. Qualquer valor abaixo de 10^11
    so poderia ser ms se fosse ano ~5138, entao eh seguro tratar como s.
    """
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return now_ms()
    if val <= 0:
        return now_ms()
    if val < 10**11:
        val *= 1000
    return val


def process_ws_message(lab: Lab, m: dict, conn_id: int) -> None:
    et = m.get("event_type")
    ts = normalize_ws_timestamp(m.get("timestamp"))
    stats = lab.conn_stats[conn_id]
    stats["events"] += 1
    stats["last_event_ms"] = ts
    if et == "book":
        handle_book_event(lab, m, ts)
    elif et == "best_bid_ask":
        handle_best_bid_ask_event(lab, m, ts)
    elif et == "price_change":
        handle_price_change_event(lab, m, ts)


async def reader_task(lab: Lab, tokens_chunk: list, conn_id: int, deadline: float) -> None:
    sub_msg = {"assets_ids": tokens_chunk, "type": "market"}
    lab.ws_subscriptions[conn_id] = tokens_chunk
    backoff = 1.0
    connected_since: Optional[float] = None
    shutdown = lab.ensure_shutdown_event()

    while time.time() < deadline and not shutdown.is_set():
        try:
            async with websockets.connect(
                CLOB_WS,
                ping_interval=20,
                ping_timeout=20,
                max_size=10 * 1024 * 1024,
                close_timeout=5,
            ) as ws:
                await ws.send(json.dumps(sub_msg))
                connected_since = time.time()
                backoff = 1.0
                while time.time() < deadline and not shutdown.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        if connected_since and (time.time() - connected_since) > 60:
                            backoff = 1.0
                        continue
                    except ConnectionClosed as exc:
                        lab.log(f"[conn {conn_id}] WS closed: {exc}", level="WARN")
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        lab.log(f"[conn {conn_id}] JSON decode: {exc}", level="WARN")
                        continue
                    msgs = data if isinstance(data, list) else [data]
                    for m in msgs:
                        if isinstance(m, dict):
                            try:
                                process_ws_message(lab, m, conn_id)
                            except Exception as exc:
                                lab.log(
                                    f"[conn {conn_id}] proc error: {type(exc).__name__}: {exc}",
                                    level="WARN",
                                )
        except Exception as exc:
            lab.conn_stats[conn_id]["reconnects"] += 1
            lab.log(
                f"[conn {conn_id}] reconnect caused by {type(exc).__name__}: {exc}",
                level="WARN",
            )

        if time.time() < deadline and not shutdown.is_set():
            sleep_sec = min(backoff, 60.0)
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=sleep_sec)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60.0)


# ============================================================
# SNAPSHOT LOOP (registra so quando update real mudou, fix bug #2)
# ============================================================

async def snapshot_task(lab: Lab, deadline: float) -> None:
    last_registered_ts: dict[int, int] = {}
    shutdown = lab.ensure_shutdown_event()

    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=SNAPSHOT_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass

        cur_ms = now_ms()
        for ev in lab.events:
            snap = compute_snapshot(ev, cur_ms)
            last_ts = last_registered_ts.get(ev.idx, -1)
            if snap.based_on_update_ts > last_ts:
                lab.snapshot_history[ev.idx].append(snap)
                last_registered_ts[ev.idx] = snap.based_on_update_ts


# ============================================================
# DETECTOR + EDGE DECAY
# ============================================================

async def measure_decay(
    lab: Lab,
    det_id: int,
    event_idx: int,
    direction: str,
    detection_ts_ms: int,
    target_ms: int,
) -> None:
    lab._active_decay_tasks += 1
    shutdown = lab.ensure_shutdown_event()
    try:
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=target_ms / 1000.0)
            return  # shutdown antes do target
        except asyncio.TimeoutError:
            pass

        measured_at = now_ms()
        target_ts = detection_ts_ms + target_ms
        history = lab.snapshot_history.get(event_idx)

        book_source_update: Optional[int] = None
        available = False
        surviving_gross: Optional[Decimal] = None
        direction_matches = False

        if history:
            # Exige snapshot baseado em update REAL pos-detecao
            candidates = [
                s for s in history
                if s.based_on_update_ts > detection_ts_ms
                and s.timestamp_ms <= target_ts + 500
                and not s.rejected_snapshot
            ]
            if candidates:
                best = candidates[-1]
                book_source_update = best.based_on_update_ts
                available = True
                dev = check_deviation(best)
                if dev is None:
                    surviving_gross = Decimal("0")
                    direction_matches = False
                else:
                    new_dir, new_edge = dev
                    if new_dir == direction:
                        surviving_gross = new_edge
                        direction_matches = True
                    else:
                        surviving_gross = Decimal("0")
                        direction_matches = False

        lab.decays.append(
            DecayMeasurement(
                detection_id=det_id,
                target_ms=target_ms,
                measured_at_ms=measured_at,
                book_source_update_ms=book_source_update,
                book_available=available,
                surviving_gross_edge=(
                    float(surviving_gross) if surviving_gross is not None else None
                ),
                surviving_direction_matches=direction_matches,
            )
        )
    finally:
        lab._active_decay_tasks = max(0, lab._active_decay_tasks - 1)


def opportunity_should_fire(
    lab: Lab,
    event_idx: int,
    direction: str,
    cur_ms: int,
) -> bool:
    """Fix bug #13. So fire se (nao aberta) E (passou gap minimo desde fechamento)."""
    key = (event_idx, direction)
    if key in lab.open_opportunities:
        return False
    last_close = lab.last_close_ms.get(key, 0)
    if cur_ms - last_close < MIN_OPPORTUNITY_GAP_MS:
        return False
    return True


async def detector_task(lab: Lab, deadline: float) -> None:
    shutdown = lab.ensure_shutdown_event()
    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=DETECTION_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass

        if lab._active_decay_tasks > MAX_ACTIVE_DECAY_TASKS:
            continue

        cur_ms = now_ms()
        for ev in lab.events:
            history = lab.snapshot_history.get(ev.idx)
            if not history:
                continue
            snap = history[-1]
            dev = check_deviation(snap)

            # Fechar oportunidades abertas que nao persistem mais
            for direction in ("LONG", "SHORT"):
                key = (ev.idx, direction)
                if key in lab.open_opportunities:
                    open_det = lab.open_opportunities[key]
                    open_det.persisted_ticks += 1
                    still_open = dev is not None and dev[0] == direction
                    if not still_open:
                        open_det.closed_at_ms = cur_ms
                        lab.last_close_ms[key] = cur_ms
                        del lab.open_opportunities[key]

            # Criar nova deteccao se edge valido e nao ha aberta
            if dev is None:
                continue
            direction, gross_edge = dev
            if gross_edge < MIN_GROSS_EDGE:
                continue
            if not opportunity_should_fire(lab, ev.idx, direction, cur_ms):
                continue

            fee_load = fee_load_approx(ev.fee_rate, len(ev.outcomes))
            net_edge = gross_edge - fee_load

            det = DetectionRecord(
                detection_id=lab.next_detection_id(),
                detected_at_ms=cur_ms,
                event_idx=ev.idx,
                event_title=ev.title,
                category=ev.category,
                fee_rate=float(ev.fee_rate),
                n_outcomes=len(ev.outcomes),
                volume_bucket=ev.volume_bucket_label,
                time_bucket_label=time_bucket(float(ev.days_until_resolution) * 24),
                days_until_resolution=float(ev.days_until_resolution),
                direction=direction,
                initial_sum_ask=float(snap.sum_ask),
                initial_sum_bid=float(snap.sum_bid),
                initial_gross_edge=float(gross_edge),
                initial_fee_load_approx=float(fee_load),
                initial_net_edge=float(net_edge),
                book_depth_usd=float(snap.total_book_depth_usd),
                book_depth_bucket=book_depth_bucket(snap.total_book_depth_usd),
                persisted_ticks=1,
            )
            lab.detections.append(det)
            lab.open_opportunities[(ev.idx, direction)] = det
            lab.log_event("detection", {
                "id": det.detection_id,
                "event": ev.title[:60],
                "dir": direction,
                "gross": float(gross_edge),
                "net": float(net_edge),
                "depth": float(snap.total_book_depth_usd),
            })

            for target in DECAY_TARGETS_MS:
                asyncio.create_task(
                    measure_decay(lab, det.detection_id, ev.idx, direction, cur_ms, target)
                )


# ============================================================
# GAMMA DISCOVERY + RE-DISCOVERY
# ============================================================

async def fetch_all_events() -> list:
    collected: list = []
    offset = 0
    async with httpx.AsyncClient(base_url=GAMMA, timeout=30) as c:
        for _ in range(10):
            r = await c.get("/events", params={
                "limit": 200, "offset": offset,
                "active": "true", "closed": "false",
                "order": "volume", "ascending": "false",
            })
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            collected.extend(batch)
            if len(batch) < 200:
                break
            offset += 200
    return collected


def event_passes_filters(ev: dict) -> bool:
    markets = ev.get("markets") or []
    n = len(markets)
    if n < MIN_OUTCOMES or n > MAX_OUTCOMES:
        return False
    if not all(m.get("negRisk") for m in markets):
        return False
    tradable = all(
        m.get("enableOrderBook")
        and m.get("acceptingOrders", False)  # bug #6: default False em vez de True
        and not m.get("closed")
        and m.get("active", True)
        for m in markets
    )
    if not tradable:
        return False
    vol = d(ev.get("volume")) or Decimal("0")
    if not (VOL_MIN_TOTAL <= vol <= VOL_MAX_TOTAL):
        return False
    end_dt = parse_end_date(ev.get("endDate"))
    days_val = days_until(end_dt)
    if days_val is None or days_val < MIN_DAYS or days_val > MAX_DAYS:
        return False
    return True


def select_diverse_markets(eligible: list, n_target: int) -> list:
    by_group: dict[tuple[str, str], list] = defaultdict(list)
    for ev in eligible:
        vol = d(ev.get("volume")) or Decimal("0")
        cat = infer_category(ev.get("tags") or [])
        by_group[(volume_bucket(vol), cat)].append(ev)
    for key in by_group:
        by_group[key].sort(key=lambda e: -float(d(e.get("volume")) or 0))

    selected = []
    idx_per_group: dict[tuple[str, str], int] = defaultdict(int)
    for _ in range(20):
        for key in list(by_group.keys()):
            if len(selected) >= n_target:
                break
            idx = idx_per_group[key]
            if idx < len(by_group[key]):
                selected.append(by_group[key][idx])
                idx_per_group[key] = idx + 1
        if len(selected) >= n_target:
            break
    return selected


async def rediscovery_task(lab: Lab, deadline: float) -> None:
    """
    A cada REDISCOVERY_INTERVAL_SEC, verifica se mercados selecionados
    ainda estao ativos/tradable. Loga taxa de mortalidade (nao troca
    universo on-the-fly -- exigiria re-inscrever WS).
    """
    shutdown = lab.ensure_shutdown_event()
    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=REDISCOVERY_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass
        try:
            all_events = await fetch_all_events()
            eligible_ids = {str(ev.get("id")) for ev in all_events if event_passes_filters(ev)}
            try:
                with open(lab.output_dir / "markets.json") as f:
                    selected_info = json.load(f)
            except Exception:
                selected_info = []
            still_active = sum(1 for info in selected_info if info.get("event_id") in eligible_ids)
            total = len(selected_info)
            mortality_pct = 0.0 if total == 0 else (1 - still_active / total) * 100
            lab.log(
                f"[REDISCOVERY] {still_active}/{total} mercados ativos "
                f"({mortality_pct:.1f}% mortalidade)"
            )
            lab.log_event("rediscovery", {
                "still_active": still_active,
                "total": total,
                "mortality_pct": mortality_pct,
            })
        except Exception as exc:
            lab.log(f"[REDISCOVERY] erro: {type(exc).__name__}: {exc}", level="WARN")


# ============================================================
# PERSISTENCE (INCREMENTAL, CORRIGE BUG #15)
# ============================================================

def persist_incremental(lab: Lab, tag: str = "") -> None:
    if not HAS_PANDAS:
        return
    try:
        new_detections = lab.detections[lab._persisted_detections_cursor:]
        new_decays = lab.decays[lab._persisted_decays_cursor:]

        if not new_detections and not new_decays:
            return

        lab._persist_seq += 1
        seq = lab._persist_seq
        label = tag or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

        if new_detections:
            df_d = pd.DataFrame([det.__dict__ for det in new_detections])
            out = lab.output_dir / f"detections_{seq:04d}_{label}.parquet"
            df_d.to_parquet(out, index=False)
            lab._persisted_detections_cursor = len(lab.detections)

        if new_decays:
            df_l = pd.DataFrame([dec.__dict__ for dec in new_decays])
            out = lab.output_dir / f"decays_{seq:04d}_{label}.parquet"
            df_l.to_parquet(out, index=False)
            lab._persisted_decays_cursor = len(lab.decays)

        lab.log(
            f"[PERSIST #{seq}] +{len(new_detections)} detecoes, "
            f"+{len(new_decays)} decays (cursores: det={lab._persisted_detections_cursor}, "
            f"dec={lab._persisted_decays_cursor})"
        )
    except Exception as exc:
        lab.log(
            f"[PERSIST] erro: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            level="ERROR",
        )


async def persist_task(lab: Lab, deadline: float) -> None:
    shutdown = lab.ensure_shutdown_event()
    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=PERSIST_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass
        persist_incremental(lab)


# ============================================================
# STATUS / SANITY ALERTS
# ============================================================

def _rolling_detections(lab: Lab, window_sec: int) -> list[DetectionRecord]:
    cutoff = now_ms() - window_sec * 1000
    return [d for d in lab.detections if d.detected_at_ms >= cutoff]


def compute_sanity_alerts(lab: Lab) -> dict[str, str]:
    alerts: dict[str, str] = {}
    recent = _rolling_detections(lab, ALERT_ROLLING_WINDOW_SEC)
    n = len(recent)

    if n >= 20:
        edges = sorted(r.initial_gross_edge for r in recent)
        p90 = edges[int(0.9 * len(edges))]
        if p90 > float(ALERT_P90_EDGE_THRESHOLD):
            alerts["edge_p90_high"] = (
                f"P90 gross edge = {p90:.2%} em {n} detecoes (ultimos "
                f"{ALERT_ROLLING_WINDOW_SEC//60}min)"
            )

    if n >= 20:
        event_counts = Counter(r.event_idx for r in recent)
        top_idx, top_count = event_counts.most_common(1)[0]
        frac = top_count / n
        if frac > ALERT_EVENT_CONCENTRATION:
            alerts["event_concentration"] = (
                f"Event idx={top_idx} tem {frac:.1%} das ultimas {n} detecoes"
            )

    if n >= 20:
        bucket_counts = Counter(r.book_depth_bucket for r in recent)
        top_bucket, top_count = bucket_counts.most_common(1)[0]
        frac = top_count / n
        if frac > ALERT_BUCKET_CONCENTRATION:
            alerts["bucket_concentration"] = (
                f"Bucket '{top_bucket}' tem {frac:.1%} das ultimas {n} detecoes"
            )

    # Locked books nos snapshots recentes
    locked_total = 0
    outcomes_total = 0
    cutoff = now_ms() - 30_000
    for ev in lab.events:
        hist = lab.snapshot_history.get(ev.idx)
        if not hist:
            continue
        recent_snaps = [s for s in hist if s.timestamp_ms >= cutoff]
        for s in recent_snaps:
            locked_total += s.locked_outcomes
            outcomes_total += len(ev.outcomes)
    if outcomes_total > 100:
        rate = locked_total / outcomes_total
        if rate > ALERT_LOCKED_BOOK_RATE:
            alerts["locked_book_rate"] = (
                f"Taxa de locked books = {rate:.1%} (amostras: {outcomes_total})"
            )
    return alerts


async def status_task(lab: Lab, deadline: float) -> None:
    shutdown = lab.ensure_shutdown_event()
    while time.time() < deadline and not shutdown.is_set():
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=STATUS_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass

        elapsed = time.time() - (lab.start_time or time.time())
        total_ws = sum(s["events"] for s in lab.conn_stats.values())
        reconnects = sum(s["reconnects"] for s in lab.conn_stats.values())

        decay_by_target: dict[int, list[float]] = defaultdict(list)
        for dec in lab.decays[-2000:]:
            if dec.surviving_gross_edge is not None:
                decay_by_target[dec.target_ms].append(dec.surviving_gross_edge)

        decay_summary = {}
        for t in DECAY_TARGETS_MS:
            vals = decay_by_target.get(t, [])
            decay_summary[f"{t}ms"] = {
                "n": len(vals),
                "median_surviving": (sorted(vals)[len(vals) // 2] if vals else None),
            }

        alerts = compute_sanity_alerts(lab)
        for key, msg in alerts.items():
            if key not in lab.active_alerts:
                lab.log(f"[ALERT] {key}: {msg}", level="WARN")
        for key in list(lab.active_alerts.keys()):
            if key not in alerts:
                lab.log(f"[ALERT CLEARED] {key}")
        lab.active_alerts = alerts

        status = {
            "elapsed_min": round(elapsed / 60, 1),
            "duration_min": round(lab.duration_sec / 60, 1),
            "pct_done": round(100 * elapsed / lab.duration_sec, 1),
            "ws_events_total": total_ws,
            "ws_reconnects_total": reconnects,
            "ws_conns": len(lab.conn_stats),
            "events_monitored": len(lab.events),
            "detections_total": len(lab.detections),
            "detections_last_1h": len(_rolling_detections(lab, 3600)),
            "open_opportunities": len(lab.open_opportunities),
            "active_decay_tasks": lab._active_decay_tasks,
            "decays_total": len(lab.decays),
            "decay_summary": decay_summary,
            "active_alerts": alerts,
            "last_update_utc": datetime.now(timezone.utc).isoformat(),
        }
        try:
            tmp = lab.status_path.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(status, f, indent=2, default=str)
            os.replace(tmp, lab.status_path)
        except Exception as exc:
            lab.log(f"[STATUS] erro gravando status.json: {exc}", level="WARN")

        lab.log(
            f"[{elapsed/60:.1f}min/{lab.duration_sec/60:.0f}min] "
            f"ws={total_ws} rec={reconnects} det={len(lab.detections)} "
            f"open={len(lab.open_opportunities)} alerts={len(alerts)}"
        )


# ============================================================
# MAIN ORCHESTRATION
# ============================================================

async def run_simulation(lab: Lab) -> None:
    lab.log("=== HFT Sim v2 ===")
    lab.log(f"Duracao: {lab.duration_sec/3600:.1f}h")
    lab.log(f"Mercados alvo: {lab.n_markets}")
    lab.log(f"Output: {lab.output_dir}")

    lab.log("Buscando events da Gamma API...")
    all_events = await fetch_all_events()
    lab.log(f"  {len(all_events)} events totais")

    eligible = [ev for ev in all_events if event_passes_filters(ev)]
    lab.log(f"  {len(eligible)} passaram filtros")

    if len(eligible) < 10:
        lab.log(f"ERRO: muito poucos elegiveis ({len(eligible)}).", level="ERROR")
        return

    selected = select_diverse_markets(eligible, lab.n_markets)
    lab.log(f"  {len(selected)} selecionados para monitorar")

    markets_info = []
    for i, ev in enumerate(selected):
        vol = float(d(ev.get("volume")) or 0)
        cat = infer_category(ev.get("tags") or [])
        end_dt = parse_end_date(ev.get("endDate"))
        days_val = float(days_until(end_dt) or 0)
        markets_info.append({
            "idx": i,
            "event_id": str(ev.get("id", "")),
            "slug": ev.get("slug", ""),
            "title": ev.get("title", ""),
            "category": cat,
            "fee_rate": float(fee_for(cat)),
            "volume_total": vol,
            "volume_bucket": volume_bucket(Decimal(str(vol))),
            "days_until_resolution": days_val,
            "n_outcomes": len(ev.get("markets", [])),
        })
    with open(lab.output_dir / "markets.json", "w") as f:
        json.dump(markets_info, f, indent=2)

    by_vol = Counter(m["volume_bucket"] for m in markets_info)
    by_cat = Counter(m["category"] for m in markets_info)
    lab.log(f"  Por volume: {dict(by_vol)}")
    lab.log(f"  Por categoria: {dict(by_cat.most_common(6))}")

    for i, ev in enumerate(selected):
        cat = infer_category(ev.get("tags") or [])
        vol = d(ev.get("volume")) or Decimal("0")
        end_dt = parse_end_date(ev.get("endDate"))
        days_val = days_until(end_dt) or Decimal("0")
        es = EventState(
            idx=i,
            title=(ev.get("title") or "")[:120],
            category=cat,
            fee_rate=fee_for(cat),
            volume=vol,
            outcomes=[],
            days_until_resolution=days_val,
            volume_bucket_label=volume_bucket(vol),
        )
        for m in ev.get("markets", []):
            tok_raw = m.get("clobTokenIds") or m.get("clob_token_ids")
            if isinstance(tok_raw, str):
                try:
                    tok_raw = json.loads(tok_raw)
                except Exception:
                    tok_raw = None
            if not tok_raw or len(tok_raw) < 2:
                continue
            os_ = OutcomeState(
                event_idx=i,
                question=(m.get("question") or "?"),
                yes_token=str(tok_raw[0]),
            )
            es.outcomes.append(os_)
            lab.token_to_outcome[os_.yes_token] = os_
        lab.events.append(es)

    all_tokens = list(lab.token_to_outcome.keys())
    chunks = [
        all_tokens[i:i + MAX_TOKENS_PER_CONN]
        for i in range(0, len(all_tokens), MAX_TOKENS_PER_CONN)
    ]
    lab.log(f"Total: {len(lab.events)} events, {len(all_tokens)} tokens, {len(chunks)} conexoes WS")

    lab.start_time = time.time()
    deadline = lab.start_time + lab.duration_sec

    # Registrar handlers de sinal apos criacao do loop (para call_soon_threadsafe)
    loop = asyncio.get_running_loop()
    shutdown = lab.ensure_shutdown_event()

    def _set_shutdown() -> None:
        lab.log("Shutdown signal recebido, finalizando graceful...", level="WARN")
        shutdown.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _set_shutdown)
        loop.add_signal_handler(signal.SIGTERM, _set_shutdown)
    except (NotImplementedError, RuntimeError):
        # Fallback para Windows / contextos sem add_signal_handler
        pass

    readers = [
        asyncio.create_task(reader_task(lab, chunk, i, deadline))
        for i, chunk in enumerate(chunks)
    ]
    snap_t = asyncio.create_task(snapshot_task(lab, deadline))
    det_t = asyncio.create_task(detector_task(lab, deadline))
    persist_t = asyncio.create_task(persist_task(lab, deadline))
    status_t = asyncio.create_task(status_task(lab, deadline))
    rediscover_t = asyncio.create_task(rediscovery_task(lab, deadline))
    all_tasks = readers + [snap_t, det_t, persist_t, status_t, rediscover_t]

    try:
        await asyncio.gather(*all_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        lab.log("Shutdown via CancelledError", level="WARN")
    finally:
        lab.log("Finalizando: persist final...")
        persist_incremental(lab, tag="final")
        lab.log(f"Total detecoes: {len(lab.detections)}")
        lab.log(f"Total decay measurements: {len(lab.decays)}")
        lab.log(f"Total WS events: {sum(s['events'] for s in lab.conn_stats.values())}")
        lab.log(f"Total reconnects: {sum(s['reconnects'] for s in lab.conn_stats.values())}")
        lab.log(f"Output em: {lab.output_dir}")


def main() -> int:
    parser = argparse.ArgumentParser(description="HFT Simulation Lab v2")
    parser.add_argument(
        "--hours", type=float, default=DEFAULT_DURATION_HOURS,
        help=f"Duracao em horas (default: {DEFAULT_DURATION_HOURS})",
    )
    parser.add_argument(
        "--markets", type=int, default=DEFAULT_N_MARKETS,
        help=f"Numero de mercados (default: {DEFAULT_N_MARKETS})",
    )
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    duration_sec = args.hours * 3600
    if args.output:
        output_dir = Path(args.output)
    else:
        run_name = datetime.now(timezone.utc).strftime("run_%Y-%m-%d_%Hh%M")
        output_dir = Path("./output") / run_name

    lab = Lab(
        duration_sec=duration_sec,
        n_markets=args.markets,
        output_dir=output_dir,
    )

    try:
        asyncio.run(run_simulation(lab))
    except KeyboardInterrupt:
        lab.log("Interrompido (KeyboardInterrupt)")
    except Exception as exc:
        lab.log(
            f"FATAL: {type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            level="ERROR",
        )
        return 1
    finally:
        lab.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
