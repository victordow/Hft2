#!/usr/bin/env python3
"""
HFT Simulation Lab v2 — NegRisk arb detection on Polymarket CLOB.

Correções sobre o v1 (auditoria de 20/abr/2026):
  - Bugs de dados: ghost/terminal books filtrados por snapshot, não por universo
  - Bugs de lógica: check_deviation agora aplica fees e thresholds; book locked
    (ask<=bid) rejeita snapshot inteiro
  - Bugs de medição: `actual_delay_ms` removido (não media nada). Substituído
    por edge_decay contra snapshot com update REAL pós-detecção
  - Bugs de contagem: persist incremental (um parquet por janela de 15min com
    só o que foi adicionado); cooldown estendido com detecção de "reopen"
  - Bugs de concorrência: asyncio.Event para shutdown; log explícito de
    exceções de reconexão; backoff capped em 60s
  - Book completo (não só best_bid/ask) guardado para calcular depth executável
  - price_change aplicado como delta ao book local (v1 lia campos inexistentes)
  - Re-discovery periódica (30min) marca resolvidos como inactive
  - status.json sobrescrito a cada 30s para monitoramento remoto sem TTY
  - Sanity alerts em tempo real (P90 edge, concentração, regressão de bugs)

Uso:
    python3 hft_sim_v2.py                  # 16h, 80 mercados (defaults)
    python3 hft_sim_v2.py --hours 18       # override duração
    python3 hft_sim_v2.py --markets 120    # mais mercados

Rodar em background (Hetzner + Terminus/SSH):
    tmux new -s hft
    python3 hft_sim_v2.py --hours 16
    # Ctrl+B, D para detach. Fecha SSH tranquilo.
    # Amanhã: tmux attach -t hft
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from statistics import median
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import httpx
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    print("ERRO: pandas + pyarrow obrigatórios. Rode: pip install pandas pyarrow",
          file=sys.stderr)
    HAS_PANDAS = False

getcontext().prec = 28


# ═════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═════════════════════════════════════════════════════════════════════════════

GAMMA = "https://gamma-api.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Fee rates (Polymarket, regime pós-30/mar/2026). Fórmula para taker fee:
#   fee_usdc = shares * fee_rate * p * (1 - p)
# Para arb NegRisk com N outcomes em p médio, fee_load_per_1_usdc_notional ≈
# sum(fee_rate * p_i * (1 - p_i)) — calculado por snapshot nas funções abaixo.
FEE_RATE: Dict[str, Decimal] = {
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
    "sports": Decimal("0.0300"),
    "geopolitics": Decimal("0"),
    "middle-east": Decimal("0"),
    "elections": Decimal("0.0400"),
    "unknown": Decimal("0.0500"),  # conservador mas não extremo (era 0.072 em v1)
}
DEFAULT_FEE = FEE_RATE["unknown"]

# Duração & universo
DEFAULT_DURATION_HOURS = 16.0
DEFAULT_N_MARKETS = 80
MAX_TOKENS_PER_CONN = 50

# Filtros de descoberta (universo)
VOL_MIN = Decimal("5000")           # volume total histórico mínimo
VOL_MAX = Decimal("5000000")
MIN_OUTCOMES = 2
MIN_OUTCOMES_NEGRISK = 2
MAX_OUTCOMES = 20
MIN_DAYS = Decimal("1")             # 24h mínimo de resolução (era 2h no v1)
MAX_DAYS = Decimal("180")

# Re-discovery
REDISCOVERY_INTERVAL_SEC = 30 * 60   # 30min

# Detector
DETECTION_INTERVAL_MS = 500
STALENESS_MS = 30_000
SNAPSHOT_HISTORY_PER_EVENT = 400     # 80s de buffer @ 200ms — cobre edge_decay 5s
MAX_ACTIVE_MEASUREMENTS = 500

# Cooldown estendido (bug #13): por default 10min, mas só vale enquanto a
# oportunidade está "aberta"; se o desvio zerar entre detecções, permite nova.
DETECTION_COOLDOWN_MS = 10 * 60 * 1000  # 10min

# Thresholds para filtro de snapshot (bugs #3, #8, #19)
GHOST_ASK_MAX = Decimal("0.02")     # ask <= 0.02 + bid<=0.01 = ghost
GHOST_BID_MAX = Decimal("0.01")
TERMINAL_ASK_MIN = Decimal("0.98")  # ask>=0.98 + bid>=0.97 = quase resolveu
TERMINAL_BID_MIN = Decimal("0.97")
MAX_SPREAD = Decimal("0.30")        # spread > 30c = book vazio
MIN_DEPTH_USD_PER_OUTCOME = Decimal("5")   # dust floor — por outcome
MIN_DUST_NOTIONAL = Decimal("1.0")         # ignora levels com price*size < $1

# Edge thresholds (bug #5, #14)
MIN_GROSS_EDGE_RAW = Decimal("0.005")   # 0.5% bruto — piso cético
MIN_NET_EDGE_AFTER_FEES = Decimal("0.002")  # 0.2% líquido — piso viabilidade

# Edge decay targets (substitui latency em v1)
EDGE_DECAY_TARGETS_MS = [300, 1000, 2000, 5000]

# Persist / observabilidade
CHECKPOINT_INTERVAL_SEC = 15 * 60   # 15min
STATUS_INTERVAL_SEC = 30            # status.json a cada 30s
STATUS_CONSOLE_INTERVAL_SEC = 120   # log verboso a cada 2min

# Alert thresholds (sanity checks em tempo real)
ALERT_P90_EDGE_THRESHOLD = Decimal("0.10")   # edge P90 > 10% = WARN
ALERT_CONCENTRATION_PCT = Decimal("0.50")    # >50% detecções em 1 mercado = WARN
ALERT_BUCKET_DOMINANCE_PCT = Decimal("0.90") # >90% em 1 bucket = WARN
ALERT_LOCKED_BOOK_PCT = Decimal("0.05")      # >5% outcomes ask<=bid = WARN


# ═════════════════════════════════════════════════════════════════════════════
# UTILS
# ═════════════════════════════════════════════════════════════════════════════

def to_decimal(v: Any) -> Optional[Decimal]:
    """Conversão robusta para Decimal; qualquer input duvidoso → None."""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        return Decimal(int(v))
    if isinstance(v, (int, float)):
        return Decimal(str(v))  # via str para evitar ruído de float
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


def infer_category(tags: list) -> str:
    """
    Classifica por tag. Ordem de especificidade: categorias mais específicas
    primeiro (ex.: `elections` antes de `politics`), para casar com as taxas
    reais da Polymarket.
    """
    labels = [
        (t.get("label") or t.get("slug") or "").lower()
        for t in (tags or [])
        if isinstance(t, dict)
    ]
    # Ordem: do mais específico para o mais genérico.
    priority = [
        "middle-east", "geopolitics",
        "elections", "crypto",
        "sports", "weather", "climate",
        "pop-culture", "culture", "science",
        "tech", "mentions",
        "finance", "economics", "politics",
    ]
    for p in priority:
        for label in labels:
            if p in label:
                return p
    return "unknown"


def fee_for(cat: str) -> Decimal:
    return FEE_RATE.get(cat, DEFAULT_FEE)


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
    vf = float(v)
    if vf < 10_000:
        return "<$10K"
    if vf < 50_000:
        return "$10-50K"
    if vf < 200_000:
        return "$50-200K"
    if vf < 1_000_000:
        return "$200K-1M"
    return ">$1M"


def book_depth_bucket_usd(depth_usd: Decimal) -> str:
    vf = float(depth_usd)
    if vf < 50:
        return "<$50"
    if vf < 200:
        return "$50-200"
    if vf < 1_000:
        return "$200-1k"
    if vf < 3_000:
        return "$1-3k"
    return ">$3k"


def time_bucket_hours(hours: float) -> str:
    if hours < 24:
        return "<24h"
    if hours < 7 * 24:
        return "1-7d"
    if hours < 30 * 24:
        return "7-30d"
    return ">30d"


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ═════════════════════════════════════════════════════════════════════════════
# DATACLASSES
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class BookLevel:
    price: Decimal
    size: Decimal

    @property
    def notional(self) -> Decimal:
        return self.price * self.size


@dataclass
class OutcomeState:
    """Estado do book do token YES de um outcome do NegRisk event."""
    event_idx: int
    question: str
    yes_token: str

    # Book COMPLETO (não só best). Ordenado: bids desc por preço, asks asc.
    bids: List[BookLevel] = field(default_factory=list)
    asks: List[BookLevel] = field(default_factory=list)

    # Timestamps — separados entre "última mensagem WS recebida para este
    # outcome" (real) e "último cálculo de best" (derivado).
    last_update_ms: int = 0
    ever_updated: bool = False

    # Status — marcado False quando detectamos que o market resolveu / ficou
    # inative via re-discovery. Diferente de "stale" (que é temporal).
    is_active: bool = True

    def rebuild_from_snapshot(self, bids: List[BookLevel], asks: List[BookLevel],
                              ts: int) -> None:
        # Filtra dust (bug #8) no ingest, não depois.
        self.bids = sorted(
            [lv for lv in bids if lv.notional >= MIN_DUST_NOTIONAL],
            key=lambda lv: -lv.price,
        )
        self.asks = sorted(
            [lv for lv in asks if lv.notional >= MIN_DUST_NOTIONAL],
            key=lambda lv: lv.price,
        )
        self.last_update_ms = ts
        self.ever_updated = True

    def apply_price_change(self, side: str, price: Decimal, size: Decimal,
                           ts: int) -> None:
        """
        Aplica um delta do evento `price_change` ao book local.
        Schema Polymarket: side ∈ {"BUY", "SELL"}, size = nova quantidade
        NAQUELE preço (não delta), 0 = remove o nível.

        Bug #9 corrigido: v1 lia `best_bid`/`best_ask` que não existem nesse
        payload. Aqui aplicamos corretamente como delta ao book local.
        """
        if side == "BUY":
            levels = self.bids
            reverse = True  # bids em ordem decrescente
        elif side == "SELL":
            levels = self.asks
            reverse = False
        else:
            return

        # Remove nível existente naquele preço, se houver
        levels[:] = [lv for lv in levels if lv.price != price]

        # Se size > 0 e notional >= dust floor, adiciona de volta
        if size > 0:
            lv = BookLevel(price=price, size=size)
            if lv.notional >= MIN_DUST_NOTIONAL:
                levels.append(lv)

        # Reordena
        levels.sort(key=lambda lv: (-lv.price) if reverse else lv.price)
        self.last_update_ms = ts
        self.ever_updated = True

    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks[0].price if self.asks else None

    def depth_usd_top_n(self, n: int = 5, side: str = "ask") -> Decimal:
        """Depth executável agregado nos top-N níveis (default: ask, para compra)."""
        levels = self.asks if side == "ask" else self.bids
        return sum((lv.notional for lv in levels[:n]), Decimal("0"))


@dataclass
class EventState:
    idx: int
    title_full: str               # não trunca (bug #25)
    category: str
    fee_rate: Decimal
    volume_total: Decimal
    volume_24h: Decimal
    outcomes: List[OutcomeState]
    days_until_resolution: Decimal
    volume_bucket: str
    slug: str = ""
    is_active: bool = True        # flip para False em re-discovery se resolveu

    @property
    def title_short(self) -> str:
        return self.title_full[:60]

    @property
    def n_outcomes(self) -> int:
        return len(self.outcomes)


@dataclass
class BookSnapshot:
    """
    Snapshot consistente de um evento inteiro.
    `based_on_update_ms` = o timestamp do OUTCOME mais recentemente atualizado.
    Se snapshots consecutivos têm o mesmo valor aqui, o conteúdo é idêntico
    (bug #2).
    """
    timestamp_ms: int
    based_on_update_ms: int
    sum_ask: Decimal
    sum_bid: Decimal
    asks: Tuple[Decimal, ...]
    bids: Tuple[Decimal, ...]
    depth_usd_min: Decimal         # menor depth entre outcomes
    depth_usd_sum: Decimal         # soma dos depth_usd_top5 de todos outcomes
    n_outcomes: int
    fee_load_per_usdc: Decimal     # fee total por $1 de notional, mid-market

    def is_valid(self) -> bool:
        # Sanity: sum_ask >= sum_bid sempre (ask >= bid em todo outcome)
        return self.sum_ask >= self.sum_bid


@dataclass
class DetectionRecord:
    detection_id: int
    opportunity_id: int            # mesmo entre detecções da mesma janela contínua
    detected_at_ms: int
    event_idx: int
    event_title: str
    category: str
    volume_bucket: str
    time_bucket_label: str
    direction: str                 # "LONG" (sum_ask<1) ou "SHORT" (sum_bid>1)

    # Preços agregados
    initial_sum_ask: Decimal
    initial_sum_bid: Decimal
    initial_gross_edge: Decimal    # max(0, 1-sum_ask) ou max(0, sum_bid-1)

    # Net edge (bug #14 — v1 não tinha isso)
    fee_load_per_usdc: Decimal
    initial_net_edge_after_fees: Decimal
    is_economically_viable: bool

    # Depth & microestrutura
    depth_usd_min: Decimal
    depth_usd_sum: Decimal
    depth_bucket: str
    days_until_resolution: Decimal
    n_outcomes: int

    # Persistência de oportunidade
    is_reopen: bool                # True se veio depois de desvio zerar


@dataclass
class EdgeDecayMeasurement:
    """
    Substitui LatencyMeasurement do v1 (que media próprio sleep, bug #11).
    Mede quanto edge sobra 300ms/1s/2s/5s após detecção — só vale se houve
    update REAL do book nesse intervalo.
    """
    detection_id: int
    target_delay_ms: int
    measured_at_ms: int
    elapsed_ms_actual: int
    had_real_update_post_detection: bool
    surviving_edge: Optional[Decimal]
    surviving_net_edge: Optional[Decimal]


@dataclass
class DetectionClose:
    """Registrado quando oportunidade fecha (desvio volta a zero)."""
    detection_id: int
    opportunity_id: int
    opened_at_ms: int
    closed_at_ms: int
    duration_ms: int
    max_gross_edge_seen: Decimal


# ═════════════════════════════════════════════════════════════════════════════
# LAB STATE
# ═════════════════════════════════════════════════════════════════════════════

class Lab:
    """Estado global + I/O + observabilidade."""

    def __init__(self, duration_sec: int, n_markets: int, output_dir: Path):
        self.duration_sec = duration_sec
        self.n_markets = n_markets
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Universo
        self.events: List[EventState] = []
        self.token_to_outcome: Dict[str, OutcomeState] = {}

        # Histórico de snapshots por evento (bug #23: 400 × 200ms = 80s buffer)
        self.snapshot_history: Dict[int, Deque[BookSnapshot]] = defaultdict(
            lambda: deque(maxlen=SNAPSHOT_HISTORY_PER_EVENT)
        )

        # Detecções & medições — ACUMULATIVAS (raw store)
        self.detections: List[DetectionRecord] = []
        self.decay_measurements: List[EdgeDecayMeasurement] = []
        self.detection_closes: List[DetectionClose] = []

        # Índices para persist incremental (bug #15)
        # aponta para "próximo índice a persistir"
        self._persisted_detection_idx = 0
        self._persisted_decay_idx = 0
        self._persisted_close_idx = 0
        self.persist_batch_counter = 0
        self.last_checkpoint_ts: Optional[float] = None

        # Tracking de oportunidades abertas (para cooldown com reopen — bug #13)
        # key=(event_idx, direction) → dict com state
        self.open_opportunities: Dict[Tuple[int, str], Dict[str, Any]] = {}
        self._next_opportunity_id = 0

        # Controle de medições ativas (limite anti-OOM)
        self.active_measurements: set = set()
        self._next_detection_id = 0

        # WebSocket stats
        self.conn_stats: Dict[int, Dict[str, int]] = defaultdict(
            lambda: {"events": 0, "reconnects": 0}
        )
        self.ws_events_by_type: Counter = Counter()

        # Shutdown via Event (bug #22)
        self.shutdown_event = asyncio.Event()

        # Timestamps
        self.start_time: Optional[float] = None

        # Logger (arquivo + stdout)
        self.log_path = output_dir / "lab.log"
        self.log_file = open(self.log_path, "a", buffering=1)

        # Alertas ativos (conjunto string → timestamp)
        self.active_alerts: Dict[str, int] = {}

        # Validação de snapshot por razão — para debug
        self.rejection_counts: Counter = Counter()

    def log(self, msg: str, level: str = "INFO") -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}][{level}] {msg}"
        print(line, flush=True)
        self.log_file.write(line + "\n")

    def raise_alert(self, key: str, msg: str) -> None:
        if key not in self.active_alerts:
            self.log(f"ALERT[{key}]: {msg}", level="WARN")
        self.active_alerts[key] = now_ms()

    def clear_alert(self, key: str) -> None:
        if key in self.active_alerts:
            self.log(f"ALERT[{key}] CLEARED", level="INFO")
            del self.active_alerts[key]

    def next_detection_id(self) -> int:
        self._next_detection_id += 1
        return self._next_detection_id

    def next_opportunity_id(self) -> int:
        self._next_opportunity_id += 1
        return self._next_opportunity_id

    def close(self) -> None:
        try:
            self.log_file.close()
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════
# CORE LOGIC — book validation, depth, deviation, fees
# ═════════════════════════════════════════════════════════════════════════════

def is_outcome_ghost(o: OutcomeState) -> Tuple[bool, str]:
    """Detecta book fantasma / terminal no nível do outcome. (bug #3)"""
    if not o.bids and not o.asks:
        return True, "empty_book"
    if not o.asks or not o.bids:
        return True, "one_sided_book"
    bb, ba = o.best_bid, o.best_ask
    if bb is None or ba is None:
        return True, "no_best"
    # Locked/crossed (bug #19)
    if ba <= bb:
        return True, "locked_or_crossed"
    # Spread absurdo
    if ba - bb > MAX_SPREAD:
        return True, "wide_spread"
    # Ghost clássico: ambos lados colapsados baixo
    if ba <= GHOST_ASK_MAX and bb <= GHOST_BID_MAX:
        return True, "ghost_low"
    # Terminal: quase resolveu YES
    if ba >= TERMINAL_ASK_MIN and bb >= TERMINAL_BID_MIN:
        return True, "terminal_high"
    # Depth mínimo
    depth = o.depth_usd_top_n(5, side="ask")
    if depth < MIN_DEPTH_USD_PER_OUTCOME:
        return True, "thin_depth"
    return False, ""


def is_outcome_stale(o: OutcomeState, now: int) -> bool:
    if not o.ever_updated:
        return True
    return (now - o.last_update_ms) > STALENESS_MS


def compute_snapshot(ev: EventState, now: int) -> Tuple[Optional[BookSnapshot], str]:
    """
    Retorna (snapshot, rejection_reason).
    Snapshot=None → rejeitado. rejection_reason só para telemetria.

    Bug #2: só gera snapshot se houve update real desde o último.
    Bug #4: qualquer outcome stale em NegRisk rejeita o snapshot inteiro.
    Bug #3/#8/#19: filtros de ghost/dust/locked por outcome.
    """
    if not ev.is_active:
        return None, "event_inactive"
    if not ev.outcomes:
        return None, "no_outcomes"

    # Stale?
    for o in ev.outcomes:
        if not o.is_active:
            return None, "outcome_inactive"
        if is_outcome_stale(o, now):
            return None, "stale_outcome"

    # Ghost / terminal / locked?
    for o in ev.outcomes:
        ghost, why = is_outcome_ghost(o)
        if ghost:
            return None, f"ghost_{why}"

    # Agora seguro montar o snapshot
    asks_tuple: List[Decimal] = []
    bids_tuple: List[Decimal] = []
    depths: List[Decimal] = []
    fee_load = Decimal("0")

    for o in ev.outcomes:
        ba = o.best_ask  # type: ignore[assignment]
        bb = o.best_bid  # type: ignore[assignment]
        assert ba is not None and bb is not None  # is_outcome_ghost garantiu
        asks_tuple.append(ba)
        bids_tuple.append(bb)
        depths.append(o.depth_usd_top_n(5, side="ask"))
        # Fee load no mid-price do outcome: fee_rate * p * (1-p) por $1 notional
        p_mid = (ba + bb) / 2
        fee_load += ev.fee_rate * p_mid * (Decimal("1") - p_mid)

    sum_ask = sum(asks_tuple, Decimal("0"))
    sum_bid = sum(bids_tuple, Decimal("0"))
    based_on = max(o.last_update_ms for o in ev.outcomes)

    snap = BookSnapshot(
        timestamp_ms=now,
        based_on_update_ms=based_on,
        sum_ask=sum_ask,
        sum_bid=sum_bid,
        asks=tuple(asks_tuple),
        bids=tuple(bids_tuple),
        depth_usd_min=min(depths) if depths else Decimal("0"),
        depth_usd_sum=sum(depths, Decimal("0")),
        n_outcomes=len(ev.outcomes),
        fee_load_per_usdc=fee_load,
    )
    if not snap.is_valid():
        return None, "snapshot_invalid"

    return snap, ""


def check_deviation(snap: BookSnapshot) -> Optional[Tuple[str, Decimal, Decimal]]:
    """
    Retorna (direction, gross_edge, net_edge_after_fees) se há oportunidade viável.
    None caso contrário.

    Bug #5 / #14: aplica fee load E threshold mínimo de net edge.
    """
    # LONG: comprar YES de todos os outcomes quando sum_ask < 1
    if snap.sum_ask < Decimal("1"):
        gross = Decimal("1") - snap.sum_ask
        if gross < MIN_GROSS_EDGE_RAW:
            return None
        net = gross - snap.fee_load_per_usdc
        if net < MIN_NET_EDGE_AFTER_FEES:
            return None
        return ("LONG", gross, net)

    # SHORT: split USDC + vender em todos quando sum_bid > 1
    if snap.sum_bid > Decimal("1"):
        gross = snap.sum_bid - Decimal("1")
        if gross < MIN_GROSS_EDGE_RAW:
            return None
        net = gross - snap.fee_load_per_usdc
        if net < MIN_NET_EDGE_AFTER_FEES:
            return None
        return ("SHORT", gross, net)

    return None


# ═════════════════════════════════════════════════════════════════════════════
# GAMMA DISCOVERY
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_all_events() -> list:
    collected = []
    offset = 0
    async with httpx.AsyncClient(base_url=GAMMA, timeout=30) as c:
        for _ in range(10):
            r = await c.get(
                "/events",
                params={
                    "limit": 200,
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                    "order": "volume",
                    "ascending": "false",
                },
            )
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
    if n < MIN_OUTCOMES_NEGRISK or n > MAX_OUTCOMES:
        return False
    if not all(m.get("negRisk") for m in markets):
        return False
    # Bug #6: default False, não True
    tradable = all(
        m.get("enableOrderBook")
        and m.get("acceptingOrders", False)
        and not m.get("closed")
        and m.get("active", False)
        for m in markets
    )
    if not tradable:
        return False
    vol = d(ev.get("volume")) or Decimal("0")
    if not (VOL_MIN <= vol <= VOL_MAX):
        return False
    end_dt = parse_end_date(ev.get("endDate"))
    days_val = days_until(end_dt)
    if days_val is None or days_val < MIN_DAYS or days_val > MAX_DAYS:
        return False
    return True


def select_diverse_markets(eligible: list, n_target: int) -> list:
    """Seleciona mercados diversos por (volume_bucket × categoria),
    round-robin para garantir cobertura."""
    by_group: Dict[Tuple[str, str], list] = defaultdict(list)
    for ev in eligible:
        vol = d(ev.get("volume")) or Decimal("0")
        vb = volume_bucket(vol)
        cat = infer_category(ev.get("tags") or [])
        by_group[(vb, cat)].append(ev)
    for key in by_group:
        by_group[key].sort(key=lambda e: -float(d(e.get("volume")) or 0))

    selected: list = []
    group_keys = list(by_group.keys())
    idx_per_group: Dict[Tuple[str, str], int] = defaultdict(int)
    max_rounds = 20
    for _ in range(max_rounds):
        for key in group_keys:
            if len(selected) >= n_target:
                break
            idx = idx_per_group[key]
            if idx < len(by_group[key]):
                selected.append(by_group[key][idx])
                idx_per_group[key] = idx + 1
        if len(selected) >= n_target:
            break
    return selected


async def rediscover_and_mark_inactive(lab: Lab) -> Tuple[int, int]:
    """
    Re-consulta Gamma. Marca como inactive eventos que:
      - não aparecem mais na lista de ativos
      - aparecem mas com closed=True ou algum market não-tradable
    Retorna (n_deactivated, n_checked).

    Não remove do universo (complicaria WS re-sub); só flagga para ignorar
    em compute_snapshot. Bug #26.
    """
    try:
        all_events = await fetch_all_events()
    except Exception as exc:
        lab.log(f"re-discovery FAILED: {type(exc).__name__}: {exc}", level="WARN")
        return (0, 0)

    live_ids = set(str(ev.get("id", "")) for ev in all_events)
    live_by_id = {str(ev.get("id", "")): ev for ev in all_events}

    # Nossos eventos têm o id original salvo? Precisamos mapear idx → event_id
    # Vamos salvar no title_full + usar outra via. Como os slug/title são
    # únicos o suficiente, usamos slug.
    n_deactivated = 0
    n_checked = 0
    for ev in lab.events:
        if not ev.is_active:
            continue
        n_checked += 1
        # Tenta match por slug
        matched = None
        for live in all_events:
            if live.get("slug") == ev.slug:
                matched = live
                break
        if matched is None:
            ev.is_active = False
            for o in ev.outcomes:
                o.is_active = False
            n_deactivated += 1
            continue
        # Se achou mas virou não-tradable, desativa
        if not event_passes_filters(matched):
            # Pode não passar por volume range (já estava no universo), então
            # checa só as flags críticas:
            markets = matched.get("markets") or []
            still_tradable = all(
                m.get("enableOrderBook")
                and m.get("acceptingOrders", False)
                and not m.get("closed")
                and m.get("active", False)
                for m in markets
            ) if markets else False
            if not still_tradable or matched.get("closed"):
                ev.is_active = False
                for o in ev.outcomes:
                    o.is_active = False
                n_deactivated += 1
    return (n_deactivated, n_checked)


# ═════════════════════════════════════════════════════════════════════════════
# WEBSOCKET — message handling with correct price_change semantics
# ═════════════════════════════════════════════════════════════════════════════

def _parse_book_levels(raw_list: Any) -> List[BookLevel]:
    """Parse levels do evento `book`. Formato: [{price, size}, ...]."""
    out: List[BookLevel] = []
    if not isinstance(raw_list, list):
        return out
    for lv in raw_list:
        if not isinstance(lv, dict):
            continue
        p = d(lv.get("price"))
        s = d(lv.get("size"))
        if p is None or s is None or s <= 0 or p <= 0 or p >= 1:
            continue
        out.append(BookLevel(price=p, size=s))
    return out


def process_message(lab: Lab, m: dict, conn_id: int) -> None:
    et = m.get("event_type")
    ts_raw = m.get("timestamp")
    try:
        ts = int(ts_raw) if ts_raw is not None else now_ms()
    except (TypeError, ValueError):
        ts = now_ms()

    lab.conn_stats[conn_id]["events"] += 1
    lab.ws_events_by_type[et or "unknown"] += 1

    if et == "book":
        aid = m.get("asset_id")
        o = lab.token_to_outcome.get(aid)
        if not o:
            return
        bids = _parse_book_levels(m.get("bids"))
        asks = _parse_book_levels(m.get("asks"))
        o.rebuild_from_snapshot(bids, asks, ts)

    elif et == "price_change":
        # Payload real: {asset_id, market, changes: [{price, side, size}], hash, ...}
        # Bug #9 corrigido: aplica deltas ao book local.
        aid = m.get("asset_id")
        o = lab.token_to_outcome.get(aid)
        if not o:
            # Alguns feeds mandam asset_id dentro de cada change; tenta isso.
            changes = m.get("changes") or m.get("price_changes") or []
            if isinstance(changes, list) and changes:
                for ch in changes:
                    if not isinstance(ch, dict):
                        continue
                    inner_aid = ch.get("asset_id")
                    o2 = lab.token_to_outcome.get(inner_aid) if inner_aid else None
                    if o2 is None:
                        continue
                    p = d(ch.get("price"))
                    s = d(ch.get("size"))
                    side = ch.get("side")
                    if p is None or s is None or side is None:
                        continue
                    o2.apply_price_change(side, p, s, ts)
            return
        changes = m.get("changes") or m.get("price_changes") or []
        if not isinstance(changes, list):
            return
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            p = d(ch.get("price"))
            s = d(ch.get("size"))
            side = ch.get("side")
            if p is None or s is None or side is None:
                continue
            o.apply_price_change(side, p, s, ts)

    elif et == "best_bid_ask":
        # Não usamos best_bid_ask para atualizar book (perderíamos níveis
        # profundos). Só um sinal de "algo mudou" — já temos ts do evento.
        # Optamos por IGNORAR explicitamente (o book já é mantido por
        # `book` e `price_change`).
        return

    elif et == "last_trade_price":
        # Não muda estado do book; só info
        return


async def reader_task(lab: Lab, tokens_chunk: List[str], conn_id: int,
                      deadline: float) -> None:
    """Reader WS com reconexão exponencial e log explícito de erros (bug #20/21)."""
    sub_msg = {
        "assets_ids": tokens_chunk,
        "type": "market",
        "custom_feature_enabled": True,
    }
    backoff = 1.0
    BACKOFF_MAX = 60.0
    STABLE_CONNECTION_SEC = 30  # resetar backoff após 30s estável

    while time.time() < deadline and not lab.shutdown_event.is_set():
        connected_at: Optional[float] = None
        try:
            async with websockets.connect(
                CLOB_WS, ping_interval=20, max_size=10 * 1024 * 1024
            ) as ws:
                await ws.send(json.dumps(sub_msg))
                connected_at = time.time()
                # Reset do backoff só DEPOIS de conectar + enviar sub
                backoff = 1.0
                while time.time() < deadline and not lab.shutdown_event.is_set():
                    # Reset do backoff se conexão já está estável
                    if (connected_at is not None
                            and time.time() - connected_at > STABLE_CONNECTION_SEC):
                        backoff = 1.0
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    except asyncio.TimeoutError:
                        continue
                    except ConnectionClosed as exc:
                        lab.log(
                            f"[conn {conn_id}] ConnectionClosed: "
                            f"code={exc.code if hasattr(exc, 'code') else '?'}",
                            level="WARN",
                        )
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    msgs = data if isinstance(data, list) else [data]
                    for msg in msgs:
                        if isinstance(msg, dict):
                            try:
                                process_message(lab, msg, conn_id)
                            except Exception as exc:
                                lab.log(
                                    f"[conn {conn_id}] process_message error: "
                                    f"{type(exc).__name__}: {exc}",
                                    level="WARN",
                                )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            lab.conn_stats[conn_id]["reconnects"] += 1
            lab.log(
                f"[conn {conn_id}] reconnect caused by "
                f"{type(exc).__name__}: {str(exc)[:200]}",
                level="WARN",
            )

        # Shutdown check antes do sleep
        if lab.shutdown_event.is_set() or time.time() >= deadline:
            return
        try:
            await asyncio.wait_for(
                lab.shutdown_event.wait(), timeout=min(backoff, BACKOFF_MAX)
            )
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, BACKOFF_MAX)


# ═════════════════════════════════════════════════════════════════════════════
# SNAPSHOT / DETECT / EDGE DECAY
# ═════════════════════════════════════════════════════════════════════════════

async def snapshot_task(lab: Lab, deadline: float) -> None:
    """
    Gera snapshots só quando há update novo em algum outcome do evento.
    Bug #2/#24 corrigidos: não spamma o histórico com cópias cacheadas.
    """
    # Por evento, guardamos o último `based_on_update_ms` que persistimos
    last_based_on: Dict[int, int] = {}

    while time.time() < deadline and not lab.shutdown_event.is_set():
        try:
            await asyncio.wait_for(lab.shutdown_event.wait(), timeout=0.2)
            return
        except asyncio.TimeoutError:
            pass

        current_now = now_ms()
        for ev in lab.events:
            if not ev.is_active:
                continue
            if not ev.outcomes:
                continue
            # Cheap check: se nada mudou desde o último snapshot deste evento, pula.
            latest_update = max(
                (o.last_update_ms for o in ev.outcomes if o.ever_updated), default=0
            )
            if latest_update == 0:
                continue
            if latest_update == last_based_on.get(ev.idx, -1):
                continue

            snap, reason = compute_snapshot(ev, current_now)
            if snap is None:
                lab.rejection_counts[reason] += 1
                continue
            lab.snapshot_history[ev.idx].append(snap)
            last_based_on[ev.idx] = latest_update


def _find_snapshot_at_or_after(
    history: Deque[BookSnapshot], target_ts: int,
) -> Optional[BookSnapshot]:
    """
    Encontra o primeiro snapshot em `history` cujo `based_on_update_ms > target_ts`.
    Ou seja: um snapshot baseado em update real que ocorreu depois de target_ts.
    Bug #12: só vale se o book realmente atualizou depois da detecção.
    """
    for snap in history:
        if snap.based_on_update_ms > target_ts:
            return snap
    return None


async def measure_edge_decay(lab: Lab, det_id: int, event_idx: int,
                             direction: str, detection_ts_ms: int,
                             detection_based_on_ms: int,
                             target_delay_ms: int) -> None:
    """
    Agenda medição de edge decay para +target_delay_ms após detecção.
    Diferente do v1: só considera válida se houve update REAL do book no
    intervalo. Não mede mais o próprio sleep (bug #11).
    """
    try:
        await asyncio.wait_for(
            lab.shutdown_event.wait(), timeout=target_delay_ms / 1000
        )
        return
    except asyncio.TimeoutError:
        pass

    measured_at = now_ms()
    elapsed = measured_at - detection_ts_ms
    history = lab.snapshot_history.get(event_idx, deque())

    # Snapshot baseado em update posterior à detecção
    fresh_snap = _find_snapshot_at_or_after(history, detection_based_on_ms)

    surviving_gross: Optional[Decimal] = None
    surviving_net: Optional[Decimal] = None
    had_real_update = fresh_snap is not None

    if fresh_snap is not None:
        dev = check_deviation(fresh_snap)
        if dev is not None:
            dir_new, gross_new, net_new = dev
            if dir_new == direction:
                surviving_gross = gross_new
                surviving_net = net_new
            else:
                # direção mudou (ex.: LONG→SHORT) — edge da direção original zerou
                surviving_gross = Decimal("0")
                surviving_net = Decimal("0")
        else:
            surviving_gross = Decimal("0")
            surviving_net = Decimal("0")

    lab.decay_measurements.append(EdgeDecayMeasurement(
        detection_id=det_id,
        target_delay_ms=target_delay_ms,
        measured_at_ms=measured_at,
        elapsed_ms_actual=elapsed,
        had_real_update_post_detection=had_real_update,
        surviving_edge=surviving_gross,
        surviving_net_edge=surviving_net,
    ))

    # Se completou todas as medições desta detecção, libera slot
    measured_for_det = sum(
        1 for m in lab.decay_measurements if m.detection_id == det_id
    )
    if measured_for_det >= len(EDGE_DECAY_TARGETS_MS):
        lab.active_measurements.discard(det_id)


async def detector_task(lab: Lab, deadline: float) -> None:
    """
    Varre eventos a cada DETECTION_INTERVAL_MS. Usa cooldown estendido com
    detecção de reopen: mesma oportunidade (event, direction) só gera nova
    detecção se desvio voltou a zero entre medições (bug #13).

    Também registra close events quando desvio zera (bug #28).
    """
    # helper: registra fechamento se estava aberta
    def _close_opportunity(ev_idx: int, direction: str, at_ms: int) -> None:
        key = (ev_idx, direction)
        state = lab.open_opportunities.get(key)
        if state is None:
            return
        lab.detection_closes.append(DetectionClose(
            detection_id=state["detection_id"],
            opportunity_id=state["opportunity_id"],
            opened_at_ms=state["opened_at_ms"],
            closed_at_ms=at_ms,
            duration_ms=at_ms - state["opened_at_ms"],
            max_gross_edge_seen=state["max_gross_edge"],
        ))
        del lab.open_opportunities[key]

    while time.time() < deadline and not lab.shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                lab.shutdown_event.wait(), timeout=DETECTION_INTERVAL_MS / 1000
            )
            return
        except asyncio.TimeoutError:
            pass

        if len(lab.active_measurements) > MAX_ACTIVE_MEASUREMENTS:
            continue

        current = now_ms()
        for ev in lab.events:
            if not ev.is_active:
                # Fecha qualquer oportunidade que estivesse aberta
                for direction in ("LONG", "SHORT"):
                    _close_opportunity(ev.idx, direction, current)
                continue
            history = lab.snapshot_history.get(ev.idx)
            if not history:
                continue
            snap = history[-1]
            deviation = check_deviation(snap)

            # Para cada direção possível, checa se oportunidade está aberta ou
            # deveria ser fechada.
            for direction in ("LONG", "SHORT"):
                is_active_this_dir = (
                    deviation is not None and deviation[0] == direction
                )
                key = (ev.idx, direction)
                state = lab.open_opportunities.get(key)

                if state is None and not is_active_this_dir:
                    continue  # nada acontecendo
                if state is not None and not is_active_this_dir:
                    # Estava aberta, agora fechou → registra close
                    _close_opportunity(ev.idx, direction, current)
                    continue
                if state is not None and is_active_this_dir:
                    # Ainda aberta; atualiza max_gross
                    gross = deviation[1]  # type: ignore[index]
                    if gross > state["max_gross_edge"]:
                        state["max_gross_edge"] = gross
                    # Respeita cooldown
                    if current - state["opened_at_ms"] < DETECTION_COOLDOWN_MS:
                        continue
                    # Cooldown expirou → re-registra como nova detecção
                    # dentro da mesma oportunidade (não incrementa opp_id)
                    gross, net = deviation[1], deviation[2]  # type: ignore[index]
                    _emit_detection(
                        lab, ev, snap, direction, gross, net,
                        opportunity_id=state["opportunity_id"],
                        is_reopen=False,
                        at_ms=current,
                    )
                    state["opened_at_ms"] = current  # re-arma cooldown
                    continue
                # state is None and is_active_this_dir: oportunidade nova
                gross, net = deviation[1], deviation[2]  # type: ignore[index]
                opp_id = lab.next_opportunity_id()
                det_id = _emit_detection(
                    lab, ev, snap, direction, gross, net,
                    opportunity_id=opp_id,
                    is_reopen=False,
                    at_ms=current,
                )
                lab.open_opportunities[key] = {
                    "detection_id": det_id,
                    "opportunity_id": opp_id,
                    "opened_at_ms": current,
                    "max_gross_edge": gross,
                }


def _emit_detection(lab: Lab, ev: EventState, snap: BookSnapshot,
                    direction: str, gross: Decimal, net: Decimal,
                    opportunity_id: int, is_reopen: bool, at_ms: int) -> int:
    """Cria DetectionRecord, agenda edge_decay measurements, retorna det_id."""
    det_id = lab.next_detection_id()
    rec = DetectionRecord(
        detection_id=det_id,
        opportunity_id=opportunity_id,
        detected_at_ms=at_ms,
        event_idx=ev.idx,
        event_title=ev.title_full,
        category=ev.category,
        volume_bucket=ev.volume_bucket,
        time_bucket_label=time_bucket_hours(float(ev.days_until_resolution) * 24),
        direction=direction,
        initial_sum_ask=snap.sum_ask,
        initial_sum_bid=snap.sum_bid,
        initial_gross_edge=gross,
        fee_load_per_usdc=snap.fee_load_per_usdc,
        initial_net_edge_after_fees=net,
        is_economically_viable=(net >= MIN_NET_EDGE_AFTER_FEES),
        depth_usd_min=snap.depth_usd_min,
        depth_usd_sum=snap.depth_usd_sum,
        depth_bucket=book_depth_bucket_usd(snap.depth_usd_min),
        days_until_resolution=ev.days_until_resolution,
        n_outcomes=ev.n_outcomes,
        is_reopen=is_reopen,
    )
    lab.detections.append(rec)
    for t_ms in EDGE_DECAY_TARGETS_MS:
        asyncio.create_task(
            measure_edge_decay(lab, det_id, ev.idx, direction,
                               at_ms, snap.based_on_update_ms, t_ms)
        )
    lab.active_measurements.add(det_id)
    return det_id


# ═════════════════════════════════════════════════════════════════════════════
# PERSIST INCREMENTAL (bug #15)
# ═════════════════════════════════════════════════════════════════════════════

def _detection_to_row(d: DetectionRecord) -> dict:
    return {
        "detection_id": d.detection_id,
        "opportunity_id": d.opportunity_id,
        "detected_at_ms": d.detected_at_ms,
        "event_idx": d.event_idx,
        "event_title": d.event_title,
        "category": d.category,
        "volume_bucket": d.volume_bucket,
        "time_bucket": d.time_bucket_label,
        "direction": d.direction,
        "initial_sum_ask": float(d.initial_sum_ask),
        "initial_sum_bid": float(d.initial_sum_bid),
        "initial_gross_edge": float(d.initial_gross_edge),
        "fee_load_per_usdc": float(d.fee_load_per_usdc),
        "initial_net_edge_after_fees": float(d.initial_net_edge_after_fees),
        "is_economically_viable": d.is_economically_viable,
        "depth_usd_min": float(d.depth_usd_min),
        "depth_usd_sum": float(d.depth_usd_sum),
        "depth_bucket": d.depth_bucket,
        "days_until_resolution": float(d.days_until_resolution),
        "n_outcomes": d.n_outcomes,
        "is_reopen": d.is_reopen,
    }


def _decay_to_row(m: EdgeDecayMeasurement) -> dict:
    return {
        "detection_id": m.detection_id,
        "target_delay_ms": m.target_delay_ms,
        "measured_at_ms": m.measured_at_ms,
        "elapsed_ms_actual": m.elapsed_ms_actual,
        "had_real_update_post_detection": m.had_real_update_post_detection,
        "surviving_edge": float(m.surviving_edge) if m.surviving_edge is not None else None,
        "surviving_net_edge": float(m.surviving_net_edge) if m.surviving_net_edge is not None else None,
    }


def _close_to_row(c: DetectionClose) -> dict:
    return {
        "detection_id": c.detection_id,
        "opportunity_id": c.opportunity_id,
        "opened_at_ms": c.opened_at_ms,
        "closed_at_ms": c.closed_at_ms,
        "duration_ms": c.duration_ms,
        "max_gross_edge_seen": float(c.max_gross_edge_seen),
    }


def persist_incremental(lab: Lab, force_tag: Optional[str] = None) -> None:
    """
    Persiste APENAS o que foi adicionado desde o último checkpoint.
    Bug #15 corrigido: v1 re-serializava tudo a cada batch (conta triangular).

    Arquivos nomeados por timestamp legível. Análise post-hoc:
        pd.concat([pd.read_parquet(f) for f in glob('detections_*.parquet')])
    """
    if not HAS_PANDAS:
        return

    tag = force_tag or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    new_dets = lab.detections[lab._persisted_detection_idx:]
    new_decays = lab.decay_measurements[lab._persisted_decay_idx:]
    new_closes = lab.detection_closes[lab._persisted_close_idx:]

    # Assert anti-regressão bug #15: batches incrementais não podem repetir ids
    if new_dets:
        ids = [r.detection_id for r in new_dets]
        if len(set(ids)) != len(ids):
            lab.raise_alert(
                "BUG_15_REGRESSION",
                f"IDs duplicados no batch {tag}: {len(ids)} rows, {len(set(ids))} únicos",
            )

    try:
        if new_dets:
            rows = [_detection_to_row(x) for x in new_dets]
            path = lab.output_dir / f"detections_{tag}.parquet"
            pd.DataFrame(rows).to_parquet(path, index=False)
            lab._persisted_detection_idx = len(lab.detections)
        if new_decays:
            rows = [_decay_to_row(x) for x in new_decays]
            path = lab.output_dir / f"decay_{tag}.parquet"
            pd.DataFrame(rows).to_parquet(path, index=False)
            lab._persisted_decay_idx = len(lab.decay_measurements)
        if new_closes:
            rows = [_close_to_row(x) for x in new_closes]
            path = lab.output_dir / f"closes_{tag}.parquet"
            pd.DataFrame(rows).to_parquet(path, index=False)
            lab._persisted_close_idx = len(lab.detection_closes)

        lab.persist_batch_counter += 1
        lab.last_checkpoint_ts = time.time()
        lab.log(
            f"checkpoint {tag}: +{len(new_dets)} det, "
            f"+{len(new_decays)} decay, +{len(new_closes)} closes"
        )
    except Exception as exc:
        lab.raise_alert(
            "PERSIST_FAILED",
            f"persist_incremental exception: {type(exc).__name__}: {exc}",
        )


async def persist_task(lab: Lab, deadline: float) -> None:
    while time.time() < deadline and not lab.shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                lab.shutdown_event.wait(), timeout=CHECKPOINT_INTERVAL_SEC
            )
            return
        except asyncio.TimeoutError:
            pass
        persist_incremental(lab)


# ═════════════════════════════════════════════════════════════════════════════
# STATUS.JSON (amigável pra cat remoto sem TTY)
# ═════════════════════════════════════════════════════════════════════════════

def _percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(len(s) * p)))
    return s[k]


def compute_status(lab: Lab) -> dict:
    now = now_ms()
    elapsed_sec = (time.time() - lab.start_time) if lab.start_time else 0.0

    # Edge stats dos últimos N minutos
    lookback_ms = 60 * 60 * 1000  # 1h rolling
    recent = [d for d in lab.detections if now - d.detected_at_ms <= lookback_ms]
    all_gross = [float(d.initial_gross_edge) for d in lab.detections]
    recent_gross = [float(d.initial_gross_edge) for d in recent]

    # Bucket stats
    depth_buckets = Counter(d.depth_bucket for d in lab.detections)
    total_dets = max(1, len(lab.detections))

    # Checkpoint idade
    cp_age_sec = (
        (time.time() - lab.last_checkpoint_ts) if lab.last_checkpoint_ts else None
    )

    # Re-discovery state
    n_active_events = sum(1 for e in lab.events if e.is_active)
    n_deactivated = len(lab.events) - n_active_events

    # Event type breakdown
    ws_types = dict(lab.ws_events_by_type)

    # Edge decay aggregate: % com update real por target
    decay_stats: Dict[str, dict] = {}
    for target in EDGE_DECAY_TARGETS_MS:
        slice_ = [m for m in lab.decay_measurements if m.target_delay_ms == target]
        if not slice_:
            continue
        with_update = sum(1 for m in slice_ if m.had_real_update_post_detection)
        decay_stats[f"{target}ms"] = {
            "n": len(slice_),
            "had_real_update_pct": round(100 * with_update / len(slice_), 1),
        }

    return {
        "timestamp": utc_iso(),
        "elapsed_hours": round(elapsed_sec / 3600, 2),
        "duration_hours_total": round(lab.duration_sec / 3600, 2),
        "pct_complete": round(100 * elapsed_sec / max(1, lab.duration_sec), 1),

        "detections_total": len(lab.detections),
        "detections_last_hour": len(recent),
        "detections_viable_total": sum(1 for d in lab.detections if d.is_economically_viable),
        "opportunities_opened": lab._next_opportunity_id,
        "opportunities_closed": len(lab.detection_closes),
        "opportunities_open_now": len(lab.open_opportunities),

        "gross_edge_median_pct": round(100 * median(all_gross), 4) if all_gross else None,
        "gross_edge_p90_pct": round(100 * _percentile(all_gross, 0.9), 4) if all_gross else None,
        "gross_edge_p99_pct": round(100 * _percentile(all_gross, 0.99), 4) if all_gross else None,
        "gross_edge_max_pct": round(100 * max(all_gross), 4) if all_gross else None,
        "gross_edge_recent_p90_pct": round(100 * _percentile(recent_gross, 0.9), 4) if recent_gross else None,

        "depth_bucket_distribution_pct": {
            k: round(100 * v / total_dets, 1) for k, v in depth_buckets.items()
        },

        "ws_events_total": sum(s["events"] for s in lab.conn_stats.values()),
        "ws_reconnects_total": sum(s["reconnects"] for s in lab.conn_stats.values()),
        "ws_events_by_type": ws_types,

        "events_universe_total": len(lab.events),
        "events_active": n_active_events,
        "events_deactivated_by_rediscovery": n_deactivated,

        "last_checkpoint_seconds_ago": round(cp_age_sec, 1) if cp_age_sec is not None else None,
        "checkpoint_count": lab.persist_batch_counter,

        "snapshot_rejection_top5": dict(lab.rejection_counts.most_common(5)),
        "edge_decay_stats": decay_stats,

        "alerts_active": sorted(lab.active_alerts.keys()),
    }


async def status_task(lab: Lab, deadline: float) -> None:
    status_path = lab.output_dir / "status.json"
    last_console_log = 0.0
    while time.time() < deadline and not lab.shutdown_event.is_set():
        try:
            await asyncio.wait_for(lab.shutdown_event.wait(), timeout=STATUS_INTERVAL_SEC)
            return
        except asyncio.TimeoutError:
            pass

        status = compute_status(lab)
        try:
            tmp = status_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(status, indent=2))
            tmp.replace(status_path)
        except Exception as exc:
            lab.log(f"status.json write failed: {exc}", level="WARN")

        # Log verboso a cada STATUS_CONSOLE_INTERVAL_SEC
        if time.time() - last_console_log >= STATUS_CONSOLE_INTERVAL_SEC:
            last_console_log = time.time()
            lab.log(
                f"[{status['elapsed_hours']:.2f}h / "
                f"{status['duration_hours_total']:.0f}h] "
                f"det={status['detections_total']} "
                f"viable={status['detections_viable_total']} "
                f"p90_edge={status['gross_edge_p90_pct']}% "
                f"ws={status['ws_events_total']} "
                f"reconn={status['ws_reconnects_total']} "
                f"active_events={status['events_active']}/{status['events_universe_total']}"
            )

        # Sanity checks em tempo real
        check_sanity_alerts(lab, status)


def check_sanity_alerts(lab: Lab, status: dict) -> None:
    """Aciona alertas se padrão de bug do v1 reaparecer."""
    # 1. P90 edge absurdo
    p90 = status.get("gross_edge_p90_pct")
    if p90 is not None and p90 > float(ALERT_P90_EDGE_THRESHOLD) * 100:
        lab.raise_alert(
            "P90_EDGE_SUSPICIOUS",
            f"gross_edge P90={p90}% > {float(ALERT_P90_EDGE_THRESHOLD)*100}% — possível ghost",
        )
    else:
        lab.clear_alert("P90_EDGE_SUSPICIOUS")

    # 2. Concentração de detecções em um único mercado (>50% em 1h)
    now = now_ms()
    lookback_ms = 60 * 60 * 1000
    recent = [d for d in lab.detections if now - d.detected_at_ms <= lookback_ms]
    if len(recent) >= 20:
        per_event = Counter(d.event_idx for d in recent)
        top_idx, top_count = per_event.most_common(1)[0]
        if top_count / len(recent) > float(ALERT_CONCENTRATION_PCT):
            lab.raise_alert(
                "CONCENTRATION_HIGH",
                f"event_idx={top_idx} concentrou {top_count}/{len(recent)} detecções em 1h",
            )
        else:
            lab.clear_alert("CONCENTRATION_HIGH")

    # 3. Dominância de bucket de depth
    dist = status.get("depth_bucket_distribution_pct", {})
    if dist:
        max_bucket, max_pct = max(dist.items(), key=lambda kv: kv[1])
        if max_pct > float(ALERT_BUCKET_DOMINANCE_PCT) * 100:
            lab.raise_alert(
                "DEPTH_BUCKET_DOMINANCE",
                f"bucket {max_bucket} tem {max_pct}% — possível cálculo errado de depth",
            )
        else:
            lab.clear_alert("DEPTH_BUCKET_DOMINANCE")

    # 4. Checkpoint atrasado
    cp_age = status.get("last_checkpoint_seconds_ago")
    if cp_age is not None and cp_age > 2 * CHECKPOINT_INTERVAL_SEC:
        lab.raise_alert(
            "CHECKPOINT_STALE",
            f"último checkpoint há {cp_age}s (esperado ≤{CHECKPOINT_INTERVAL_SEC}s)",
        )
    else:
        lab.clear_alert("CHECKPOINT_STALE")


async def rediscovery_task(lab: Lab, deadline: float) -> None:
    """Re-valida universo a cada REDISCOVERY_INTERVAL_SEC (bug #26)."""
    # Pula a primeira rodada (já veio de fetch inicial)
    while time.time() < deadline and not lab.shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                lab.shutdown_event.wait(), timeout=REDISCOVERY_INTERVAL_SEC
            )
            return
        except asyncio.TimeoutError:
            pass
        n_deact, n_check = await rediscover_and_mark_inactive(lab)
        lab.log(
            f"re-discovery: verificou {n_check} eventos, "
            f"desativou {n_deact} (resolvidos/stopped)"
        )


# ═════════════════════════════════════════════════════════════════════════════
# MAIN — bootstrap + orchestration
# ═════════════════════════════════════════════════════════════════════════════

def _build_events_from_selected(lab: Lab, selected: list) -> None:
    for i, ev_raw in enumerate(selected):
        cat = infer_category(ev_raw.get("tags") or [])
        vol_total = d(ev_raw.get("volume")) or Decimal("0")
        vol_24 = d(ev_raw.get("volume24hr")) or Decimal("0")
        end_dt = parse_end_date(ev_raw.get("endDate"))
        days_val = days_until(end_dt) or Decimal("0")
        title_full = str(ev_raw.get("title", "") or "").strip() or "(no title)"
        slug = str(ev_raw.get("slug", "") or "").strip()

        es = EventState(
            idx=i,
            title_full=title_full,
            category=cat,
            fee_rate=fee_for(cat),
            volume_total=vol_total,
            volume_24h=vol_24,
            outcomes=[],
            days_until_resolution=days_val,
            volume_bucket=volume_bucket(vol_total),
            slug=slug,
        )
        for m in ev_raw.get("markets", []):
            tok_raw = m.get("clobTokenIds") or m.get("clob_token_ids")
            if isinstance(tok_raw, str):
                try:
                    tok_raw = json.loads(tok_raw)
                except Exception:
                    tok_raw = None
            if not tok_raw or len(tok_raw) < 2:
                continue
            question_full = str(m.get("question", "?") or "?")
            os_ = OutcomeState(
                event_idx=i,
                question=question_full,
                yes_token=str(tok_raw[0]),
            )
            es.outcomes.append(os_)
            lab.token_to_outcome[os_.yes_token] = os_
        if len(es.outcomes) >= MIN_OUTCOMES_NEGRISK:
            lab.events.append(es)


def _save_markets_info(lab: Lab) -> None:
    info = []
    for ev in lab.events:
        info.append({
            "idx": ev.idx,
            "slug": ev.slug,
            "title": ev.title_full,
            "category": ev.category,
            "fee_rate": float(ev.fee_rate),
            "volume_total": float(ev.volume_total),
            "volume_24h": float(ev.volume_24h),
            "volume_bucket": ev.volume_bucket,
            "days_until_resolution": float(ev.days_until_resolution),
            "n_outcomes": ev.n_outcomes,
            "outcome_questions": [o.question for o in ev.outcomes],
            "outcome_tokens_yes": [o.yes_token for o in ev.outcomes],
        })
    (lab.output_dir / "markets.json").write_text(json.dumps(info, indent=2))


async def run_simulation(lab: Lab) -> None:
    lab.log(f"=== HFT Sim v2 ===")
    lab.log(f"Duração: {lab.duration_sec/3600:.1f}h")
    lab.log(f"Mercados alvo: {lab.n_markets}")
    lab.log(f"Output: {lab.output_dir}")
    lab.log(f"Checkpoint interval: {CHECKPOINT_INTERVAL_SEC/60:.0f}min")
    lab.log(f"Re-discovery: {REDISCOVERY_INTERVAL_SEC/60:.0f}min")
    lab.log(f"Status.json: {STATUS_INTERVAL_SEC}s")

    # 1. Descoberta
    lab.log("Buscando events da Gamma...")
    try:
        all_events = await fetch_all_events()
    except Exception as exc:
        lab.log(f"FALHA na busca Gamma: {type(exc).__name__}: {exc}", level="ERROR")
        return
    lab.log(f"  {len(all_events)} events totais")

    eligible = [ev for ev in all_events if event_passes_filters(ev)]
    lab.log(f"  {len(eligible)} passaram filtros")

    if len(eligible) < 10:
        lab.log(
            f"ERRO: muito poucos eligíveis ({len(eligible)}). "
            "Relaxa filtros em CONFIG e tenta de novo.",
            level="ERROR",
        )
        return

    selected = select_diverse_markets(eligible, lab.n_markets)
    lab.log(f"  {len(selected)} selecionados")

    # 2. Construir estado
    _build_events_from_selected(lab, selected)
    _save_markets_info(lab)

    if not lab.events:
        lab.log("ERRO: nenhum event válido após build", level="ERROR")
        return

    by_vol = Counter(e.volume_bucket for e in lab.events)
    by_cat = Counter(e.category for e in lab.events)
    lab.log(f"  Por volume: {dict(by_vol)}")
    lab.log(f"  Por categoria (top 5): {dict(by_cat.most_common(5))}")

    all_tokens = list(lab.token_to_outcome.keys())
    chunks = [
        all_tokens[i:i + MAX_TOKENS_PER_CONN]
        for i in range(0, len(all_tokens), MAX_TOKENS_PER_CONN)
    ]
    lab.log(
        f"{len(lab.events)} events, {len(all_tokens)} tokens, "
        f"{len(chunks)} conexões WS"
    )

    # 3. Schedule tasks
    lab.start_time = time.time()
    deadline = lab.start_time + lab.duration_sec

    readers = [
        asyncio.create_task(reader_task(lab, chunk, i, deadline), name=f"reader_{i}")
        for i, chunk in enumerate(chunks)
    ]
    snap_t = asyncio.create_task(snapshot_task(lab, deadline), name="snapshot")
    det_t = asyncio.create_task(detector_task(lab, deadline), name="detector")
    persist_t = asyncio.create_task(persist_task(lab, deadline), name="persist")
    status_t = asyncio.create_task(status_task(lab, deadline), name="status")
    redisc_t = asyncio.create_task(rediscovery_task(lab, deadline), name="rediscovery")

    all_tasks = readers + [snap_t, det_t, persist_t, status_t, redisc_t]

    try:
        await asyncio.gather(*all_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        lab.log("Cancelamento recebido")
    finally:
        # Final flush
        persist_incremental(lab, force_tag="final")
        # Status final
        try:
            final_status = compute_status(lab)
            (lab.output_dir / "status.json").write_text(json.dumps(final_status, indent=2))
        except Exception:
            pass
        lab.log(f"=== Sim finalizada ===")
        lab.log(f"  Detecções totais: {len(lab.detections)}")
        lab.log(f"  Detecções viáveis: "
                f"{sum(1 for d in lab.detections if d.is_economically_viable)}")
        lab.log(f"  Oportunidades abertas: {lab._next_opportunity_id}")
        lab.log(f"  Oportunidades fechadas: {len(lab.detection_closes)}")
        lab.log(f"  Medições decay: {len(lab.decay_measurements)}")
        lab.log(f"  Checkpoints: {lab.persist_batch_counter}")
        lab.log(f"  WS events: {sum(s['events'] for s in lab.conn_stats.values())}")
        lab.log(f"  Reconnects: {sum(s['reconnects'] for s in lab.conn_stats.values())}")
        lab.log(f"  Top rejeições de snapshot: "
                f"{dict(lab.rejection_counts.most_common(10))}")
        lab.log(f"Output em: {lab.output_dir}")


def _install_signal_handlers(lab: Lab, loop: asyncio.AbstractEventLoop) -> None:
    def _handler(sig: int) -> None:
        lab.log(f"Recebido sinal {sig}, shutdown graceful...", level="WARN")
        lab.shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handler, int(sig))
        except NotImplementedError:
            # Windows — fallback
            signal.signal(sig, lambda s, f: lab.shutdown_event.set())


def main() -> int:
    parser = argparse.ArgumentParser(description="HFT Simulation Lab v2")
    parser.add_argument("--hours", type=float, default=DEFAULT_DURATION_HOURS,
                        help=f"Duração em horas (default: {DEFAULT_DURATION_HOURS})")
    parser.add_argument("--markets", type=int, default=DEFAULT_N_MARKETS,
                        help=f"Mercados alvo (default: {DEFAULT_N_MARKETS})")
    parser.add_argument("--output", type=str, default=None,
                        help="Diretório de saída (default: ./output/run_TIMESTAMP)")
    args = parser.parse_args()

    if not HAS_PANDAS:
        print("ABORTANDO: pandas/pyarrow obrigatórios em production mode.",
              file=sys.stderr)
        return 2

    duration_sec = int(args.hours * 3600)
    if args.output:
        output_dir = Path(args.output)
    else:
        run_name = datetime.now(timezone.utc).strftime("run_%Y-%m-%d_%Hh%Mm")
        output_dir = Path("./output") / run_name

    lab = Lab(
        duration_sec=duration_sec,
        n_markets=args.markets,
        output_dir=output_dir,
    )

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _install_signal_handlers(lab, loop)
        loop.run_until_complete(run_simulation(lab))
    except KeyboardInterrupt:
        lab.log("Interrompido pelo usuário", level="WARN")
    finally:
        lab.close()
        try:
            loop.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
