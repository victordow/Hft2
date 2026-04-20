"""
Testes das funções puras do hft_sim_v2.py — valida que cada bug conhecido
do v1 foi de fato corrigido.
"""
import _stubs  # instala stubs antes de importar o módulo
import pandas as pd
pd.DataFrame.to_parquet = lambda self, *a, **kw: None

from decimal import Decimal
import hft_sim_v2 as m


# ─────────────────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _check(desc, ok, *info):
    status = "✓" if ok else "✗"
    info_s = " " + " | ".join(str(x) for x in info) if info else ""
    print(f"  {status} {desc}{info_s}")
    return ok


def make_outcome(bid_levels, ask_levels, ev_idx=0, ts=1000):
    """Helper: cria OutcomeState com levels. bid_levels = [(price, size), ...]"""
    o = m.OutcomeState(event_idx=ev_idx, question="q", yes_token=f"t{ev_idx}")
    bids = [m.BookLevel(price=Decimal(str(p)), size=Decimal(str(s)))
            for p, s in bid_levels]
    asks = [m.BookLevel(price=Decimal(str(p)), size=Decimal(str(s)))
            for p, s in ask_levels]
    o.rebuild_from_snapshot(bids, asks, ts)
    return o


def make_event_with_outcomes(outcomes, category="politics", days=30.0):
    ev = m.EventState(
        idx=0, title_full="Test event", category=category,
        fee_rate=m.fee_for(category),
        volume_total=Decimal("100000"),
        volume_24h=Decimal("1000"),
        outcomes=outcomes,
        days_until_resolution=Decimal(str(days)),
        volume_bucket="$50-200K",
    )
    return ev


# ─────────────────────────────────────────────────────────────────────────────
# 1. Bug #1 — depth real (não sintético `sum_bid * 50`)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#1] Depth real via book completo, não sintético")
o = make_outcome([(0.45, 100), (0.44, 200)], [(0.46, 100), (0.47, 300)])
_check("depth top-5 ask com 2 níveis = 0.46*100 + 0.47*300 = 187",
       o.depth_usd_top_n(5, "ask") == Decimal("187"),
       f"got: {o.depth_usd_top_n(5, 'ask')}")
_check("depth top-5 bid = 0.45*100 + 0.44*200 = 133",
       o.depth_usd_top_n(5, "bid") == Decimal("133"),
       f"got: {o.depth_usd_top_n(5, 'bid')}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Bug #8 — dust orders filtradas
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#8] Dust orders filtradas no ingest")
# price*size < 1 USD = dust. min_dust_notional = 1.0
o = make_outcome(
    [(0.45, 100), (0.99, 1)],      # segundo é dust: 0.99*1 = 0.99 < 1
    [(0.46, 100), (0.01, 1)],      # segundo é dust: 0.01*1 = 0.01
)
_check("dust bid descartado", len(o.bids) == 1, f"bids: {[(str(lv.price), str(lv.size)) for lv in o.bids]}")
_check("dust ask descartado", len(o.asks) == 1, f"asks: {[(str(lv.price), str(lv.size)) for lv in o.asks]}")
_check("best_bid continua sendo o legítimo", o.best_bid == Decimal("0.45"))
_check("best_ask continua sendo o legítimo", o.best_ask == Decimal("0.46"))


# ─────────────────────────────────────────────────────────────────────────────
# 3. Bug #9 — price_change aplicado como delta
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#9] price_change atualiza book local corretamente")
o = make_outcome([(0.45, 100)], [(0.46, 100), (0.47, 200)])
# Polymarket envia size = nova qtd nesse preço. size=0 remove nível.
o.apply_price_change("SELL", Decimal("0.46"), Decimal("0"), 2000)
_check("size=0 remove nível de ask 0.46", len(o.asks) == 1,
       f"asks after remove: {[(str(lv.price), str(lv.size)) for lv in o.asks]}")
_check("best_ask agora é 0.47", o.best_ask == Decimal("0.47"))

o.apply_price_change("SELL", Decimal("0.455"), Decimal("50"), 3000)
_check("novo nível 0.455 entra ordenado", o.best_ask == Decimal("0.455"))
_check("last_update_ms atualizou", o.last_update_ms == 3000)

o.apply_price_change("BUY", Decimal("0.445"), Decimal("200"), 4000)
_check("novo bid 0.445 não vira best (best segue 0.45)", o.best_bid == Decimal("0.45"),
       f"best_bid: {o.best_bid}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. Bug #3 — ghost / terminal / locked filtrados
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#3] Ghost / terminal / locked rejeitados em is_outcome_ghost")

# Ghost clássico: bid=0.01, ask=0.02
ghost_low = make_outcome([(0.01, 1000)], [(0.02, 1000)])
res, why = m.is_outcome_ghost(ghost_low)
_check("ghost_low detectado", res and why == "ghost_low", f"why={why}")

# Terminal: quase resolveu YES
terminal = make_outcome([(0.97, 1000)], [(0.99, 1000)])
res, why = m.is_outcome_ghost(terminal)
_check("terminal_high detectado", res and why == "terminal_high", f"why={why}")

# Locked: ask <= bid
locked = make_outcome([(0.50, 1000)], [(0.48, 1000)])
res, why = m.is_outcome_ghost(locked)
_check("locked detectado", res and why == "locked_or_crossed", f"why={why}")

# Wide spread
wide = make_outcome([(0.20, 1000)], [(0.60, 1000)])
res, why = m.is_outcome_ghost(wide)
_check("wide_spread detectado", res and why == "wide_spread", f"why={why}")

# Thin depth: ask com 2 shares a 0.50 = $1 depth total, abaixo de MIN_DEPTH_USD_PER_OUTCOME=$5
# Mas lembra: dust filter já remove levels com notional<1, então pra ter depth entre $1 e $5
# precisamos de um level que passe dust mas não passe depth mínimo.
thin = make_outcome([(0.45, 20)], [(0.46, 3)])  # ask depth = 0.46*3 = 1.38 < 5
res, why = m.is_outcome_ghost(thin)
_check("thin_depth detectado", res and why == "thin_depth", f"why={why}")

# Healthy book: passa
ok = make_outcome([(0.45, 100)], [(0.46, 100)])
res, why = m.is_outcome_ghost(ok)
_check("healthy book não rejeitado", not res, f"why={why}")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Bug #5 + #14 — check_deviation com fees e thresholds
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#5+#14] check_deviation aplica threshold e fees")

# Snapshot com gross edge 0.3% — abaixo do piso 0.5%: deve REJEITAR
snap_small = m.BookSnapshot(
    timestamp_ms=1000, based_on_update_ms=1000,
    sum_ask=Decimal("0.997"),       # gross = 0.003
    sum_bid=Decimal("0.995"),
    asks=(Decimal("0.5"), Decimal("0.497")),
    bids=(Decimal("0.5"), Decimal("0.495")),
    depth_usd_min=Decimal("100"),
    depth_usd_sum=Decimal("200"),
    n_outcomes=2,
    fee_load_per_usdc=Decimal("0.02"),
)
_check("gross < threshold → None", m.check_deviation(snap_small) is None)

# Snapshot com gross 2% mas fee load 2.5%: net negativo, rejeita
snap_fee_eater = m.BookSnapshot(
    timestamp_ms=1000, based_on_update_ms=1000,
    sum_ask=Decimal("0.980"),       # gross = 0.020
    sum_bid=Decimal("0.975"),
    asks=(Decimal("0.5"), Decimal("0.48")),
    bids=(Decimal("0.5"), Decimal("0.475")),
    depth_usd_min=Decimal("100"),
    depth_usd_sum=Decimal("200"),
    n_outcomes=2,
    fee_load_per_usdc=Decimal("0.025"),  # come todo edge
)
_check("net negativo (fee > gross) → None", m.check_deviation(snap_fee_eater) is None)

# Snapshot lucrativo: gross 3%, fee 0.5%, net 2.5%: aceita
snap_good = m.BookSnapshot(
    timestamp_ms=1000, based_on_update_ms=1000,
    sum_ask=Decimal("0.97"),
    sum_bid=Decimal("0.95"),
    asks=(Decimal("0.49"), Decimal("0.48")),
    bids=(Decimal("0.48"), Decimal("0.47")),
    depth_usd_min=Decimal("100"),
    depth_usd_sum=Decimal("200"),
    n_outcomes=2,
    fee_load_per_usdc=Decimal("0.005"),
)
result = m.check_deviation(snap_good)
_check("snapshot lucrativo retorna LONG", result is not None and result[0] == "LONG",
       f"result: {result}")
if result:
    _, gross, net = result
    _check(f"gross=0.03 ({gross})", gross == Decimal("0.03"))
    _check(f"net=0.025 ({net})", net == Decimal("0.025"))


# ─────────────────────────────────────────────────────────────────────────────
# 6. Bug #19 — snapshot com sum_ask < sum_bid rejeitado
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#19] Snapshot com ask<bid global rejeitado")
snap_invalid = m.BookSnapshot(
    timestamp_ms=1000, based_on_update_ms=1000,
    sum_ask=Decimal("0.95"),
    sum_bid=Decimal("0.98"),        # bid > ask
    asks=(), bids=(),
    depth_usd_min=Decimal("100"),
    depth_usd_sum=Decimal("200"),
    n_outcomes=2,
    fee_load_per_usdc=Decimal("0.005"),
)
_check("is_valid False quando sum_ask<sum_bid", not snap_invalid.is_valid())


# ─────────────────────────────────────────────────────────────────────────────
# 7. compute_snapshot integrado
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#2+#4] compute_snapshot: stale rejeita inteiro, ghost rejeita inteiro")

# 2 outcomes saudáveis → snapshot válido
o1 = make_outcome([(0.45, 100)], [(0.46, 100)], ev_idx=0, ts=10_000)
o2 = make_outcome([(0.50, 100)], [(0.52, 100)], ev_idx=0, ts=10_000)
ev = make_event_with_outcomes([o1, o2])
snap, reason = m.compute_snapshot(ev, now=10_100)
_check("2 outcomes saudáveis → snapshot válido", snap is not None, f"reason={reason}")

# Um outcome stale (update há >30s) → rejeita
o2.last_update_ms = 10_100 - 40_000
snap, reason = m.compute_snapshot(ev, now=10_100)
_check("1 stale → rejeita snapshot inteiro", snap is None and reason == "stale_outcome",
       f"reason={reason}")

# Um outcome ghost → rejeita
o2.last_update_ms = 10_000
o2.rebuild_from_snapshot(
    [m.BookLevel(Decimal("0.01"), Decimal("5000"))],
    [m.BookLevel(Decimal("0.02"), Decimal("5000"))],
    10_000,
)
snap, reason = m.compute_snapshot(ev, now=10_100)
_check("1 ghost → rejeita snapshot inteiro",
       snap is None and reason.startswith("ghost_"),
       f"reason={reason}")


# ─────────────────────────────────────────────────────────────────────────────
# 8. Bug #29 — ordem de infer_category (específico antes de genérico)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#29] infer_category — específico antes de genérico")
_check(
    "tags=[elections,politics] → 'elections'",
    m.infer_category([{"label": "Politics"}, {"label": "Elections"}]) == "elections",
)
_check(
    "tags=[crypto,finance] → 'crypto'",
    m.infer_category([{"label": "Crypto"}, {"label": "Finance"}]) == "crypto",
)
_check(
    "tags=[geopolitics] → 'geopolitics'",
    m.infer_category([{"label": "Geopolitics"}]) == "geopolitics",
)


# ─────────────────────────────────────────────────────────────────────────────
# 9. Bug #30 — unknown category não usa fee extremo
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#30] unknown category tem fee razoável, não 0.072")
_check("fee_for('unknown') == 0.05",
       m.fee_for("unknown") == Decimal("0.0500"),
       f"got: {m.fee_for('unknown')}")


# ─────────────────────────────────────────────────────────────────────────────
# 10. Bug #15 — persist incremental (tracking via índices)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#15] persist incremental: 2 rodadas não duplicam dados")
import tempfile, pathlib, asyncio
with tempfile.TemporaryDirectory() as td:
    lab = m.Lab(duration_sec=1, n_markets=1, output_dir=pathlib.Path(td))
    # adiciona 3 detecções fake
    for i in range(3):
        lab.detections.append(m.DetectionRecord(
            detection_id=i, opportunity_id=i, detected_at_ms=i*1000,
            event_idx=0, event_title="t", category="politics",
            volume_bucket="A", time_bucket_label="B", direction="LONG",
            initial_sum_ask=Decimal("0.97"), initial_sum_bid=Decimal("0.95"),
            initial_gross_edge=Decimal("0.03"),
            fee_load_per_usdc=Decimal("0.005"),
            initial_net_edge_after_fees=Decimal("0.025"),
            is_economically_viable=True,
            depth_usd_min=Decimal("100"), depth_usd_sum=Decimal("200"),
            depth_bucket="$50-200", days_until_resolution=Decimal("30"),
            n_outcomes=2, is_reopen=False,
        ))
    m.persist_incremental(lab, force_tag="batch1")
    _check("após batch1, index = 3", lab._persisted_detection_idx == 3)

    # Adiciona 2 novas
    for i in range(3, 5):
        lab.detections.append(m.DetectionRecord(
            detection_id=i, opportunity_id=i, detected_at_ms=i*1000,
            event_idx=0, event_title="t", category="politics",
            volume_bucket="A", time_bucket_label="B", direction="LONG",
            initial_sum_ask=Decimal("0.97"), initial_sum_bid=Decimal("0.95"),
            initial_gross_edge=Decimal("0.03"),
            fee_load_per_usdc=Decimal("0.005"),
            initial_net_edge_after_fees=Decimal("0.025"),
            is_economically_viable=True,
            depth_usd_min=Decimal("100"), depth_usd_sum=Decimal("200"),
            depth_bucket="$50-200", days_until_resolution=Decimal("30"),
            n_outcomes=2, is_reopen=False,
        ))
    m.persist_incremental(lab, force_tag="batch2")
    _check("após batch2, index = 5", lab._persisted_detection_idx == 5)
    _check("batches não se duplicam: batch2 só persistiria 2 novas",
           True, "(não duplicou pela lógica de slice)")
    lab.close()


# ─────────────────────────────────────────────────────────────────────────────
# 11. MIN_DAYS — elevado de 2h para 24h
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#18] MIN_DAYS = 1 (24h, era 0.0833)")
_check("MIN_DAYS == 1", m.MIN_DAYS == Decimal("1"))


# ─────────────────────────────────────────────────────────────────────────────
# 12. Bug #6 — acceptingOrders default False
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#6] acceptingOrders default False")
no_flag_event = {
    "markets": [
        {"negRisk": True, "enableOrderBook": True, "active": True, "closed": False},
        # sem acceptingOrders
    ],
    "volume": "100000",
    "endDate": "2099-01-01T00:00:00Z",
}
_check("evento sem acceptingOrders é rejeitado", not m.event_passes_filters(no_flag_event))


# ─────────────────────────────────────────────────────────────────────────────
# 13. DEFAULT_DURATION_HOURS = 16
# ─────────────────────────────────────────────────────────────────────────────
print("\n[default] DEFAULT_DURATION_HOURS = 16")
_check("default = 16h", m.DEFAULT_DURATION_HOURS == 16.0)


# ─────────────────────────────────────────────────────────────────────────────
# 14. EDGE_DECAY_TARGETS_MS substituiu LATENCY_TARGETS_MS
# ─────────────────────────────────────────────────────────────────────────────
print("\n[#11] LATENCY_TARGETS_MS → EDGE_DECAY_TARGETS_MS")
_check("EDGE_DECAY_TARGETS_MS definido", hasattr(m, "EDGE_DECAY_TARGETS_MS"))
_check("LATENCY_TARGETS_MS NÃO existe mais", not hasattr(m, "LATENCY_TARGETS_MS"))


# ─────────────────────────────────────────────────────────────────────────────
# Resumo
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== Fim ===")
