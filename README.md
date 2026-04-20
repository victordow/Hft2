# HFT Sim v2 — rodar no Hetzner + Terminus

## Deploy rápido

```bash
# 1. Copia o arquivo pra VPS (do celular via SFTP/SCP, ou git pull)
scp hft_sim_v2.py user@vps:/home/user/hft/

# 2. Instala deps (uma vez só)
ssh user@vps
cd ~/hft
pip3 install --user httpx websockets pandas pyarrow

# 3. Roda dentro de tmux (sobrevive ao disconnect do SSH)
tmux new -s hft
python3 hft_sim_v2.py --hours 16

# 4. Detach: Ctrl+B, depois D. Fecha o Terminus tranquilo.

# 5. Amanhã:
ssh user@vps
tmux attach -t hft         # ou: tmux ls
```

## Monitoramento sem TTY (pelo celular)

O v2 grava `status.json` no output dir a cada 30s. Pra conferir progresso de
qualquer lugar:

```bash
ssh user@vps "cat ~/hft/output/run_*/status.json" | head -60
```

Campos importantes:
- `elapsed_hours` / `pct_complete` — quanto já rodou
- `detections_total` / `detections_viable_total` — total e quantas são viáveis
  (depois de fees e thresholds)
- `gross_edge_p90_pct` — se estiver acima de 10%, alerta dispara
- `depth_bucket_distribution_pct` — se um bucket tem >90%, alerta dispara
- `alerts_active` — lista de alertas em aberto
- `snapshot_rejection_top5` — por que snapshots estão sendo rejeitados
  (stale_outcome, ghost_ghost_low, etc.)
- `edge_decay_stats` — % de medições com update real pós-detecção

Se `alerts_active` tiver qualquer coisa, não é automaticamente ruim — é um flag
pra você investigar depois no parquet.

## Análise pós-run (amanhã)

```python
import pandas as pd
from glob import glob

# Junta todos os checkpoints incrementais
dets = pd.concat([pd.read_parquet(f) for f in glob('output/run_*/detections_*.parquet')])
decay = pd.concat([pd.read_parquet(f) for f in glob('output/run_*/decay_*.parquet')])
closes = pd.concat([pd.read_parquet(f) for f in glob('output/run_*/closes_*.parquet')])

# Sanity: não deve haver detection_ids duplicados em `dets`
assert dets['detection_id'].is_unique, "BUG #15 voltou — detection_id duplicado"

# Distribuição de net edge
print(dets['initial_net_edge_after_fees'].describe(percentiles=[0.5, 0.9, 0.99]))

# Detecções viáveis por categoria
print(dets[dets['is_economically_viable']].groupby('category').size())

# Edge decay por target delay
print(decay.groupby('target_delay_ms')['surviving_net_edge'].describe())

# Vida média das oportunidades
print(closes['duration_ms'].describe())
```

## O que mudou do v1 pro v2 (resumo)

| # | Bug v1 | Correção v2 |
|---|---|---|
| 1 | `total_book_depth_usd = sum_bid × 50` (sempre ~$50) | Depth real via book completo: `sum(price × size)` nos top-5 asks |
| 2 | `snapshot_task` repete cópias cacheadas | Só grava quando `max(outcome.last_update_ms)` muda |
| 3 | Ghost/terminal books não filtrados | `is_outcome_ghost()` com 6 critérios por outcome |
| 4 | `IMPUTED_TAIL_PROB` mascarava stale | Qualquer stale rejeita snapshot inteiro |
| 5 | `check_deviation` sem threshold, sem fee | Threshold gross ≥ 0.5%, net ≥ 0.2% após fees |
| 6 | `acceptingOrders` default True | Agora default False |
| 8 | `best_ask` ignorava `size` | Dust (`price × size < $1`) filtrado no ingest |
| 9 | `price_change` lia campos que não existem | Aplica delta ao book local (BUY/SELL, size=0 remove) |
| 11 | `actual_delay_ms` media o próprio `sleep()` | Métrica removida; substituída por `edge_decay` contra update real |
| 12 | `book_available=True` se existia snapshot cacheado | Só True se snapshot baseado em update pós-detecção |
| 13 | Cooldown de 3s: mesma oportunidade virava 1200 detecções | 10min + detecção de reopen via close event |
| 14 | `DetectionRecord` só tinha gross edge | Agora tem `fee_load_per_usdc`, `initial_net_edge`, `is_economically_viable` |
| 15 | `persist_batch` reserializava TUDO (conta triangular) | Persist incremental via índice; `assert` anti-regressão |
| 18 | `MIN_DAYS = 2h` (fábrica de ghost) | Agora 24h |
| 19 | Não checava `sum_ask ≥ sum_bid` | `BookSnapshot.is_valid()` rejeita |
| 20 | Reconnects sem log de causa | Log explícito com `type(exc).__name__` |
| 21 | Backoff reset só na conexão; cap 5s na sleep | Reset após 30s estável; cap 60s |
| 22 | `shutdown = True` boolean, sleeps longas | `asyncio.Event` pra shutdown graceful rápido |
| 23 | Histórico 10s | 80s (cobre edge_decay 5s + folga) |
| 25 | Truncava `title[:60]` | Guarda título completo (`title_full`) |
| 26 | Universo fixo por 2h | Re-discovery a cada 30min, marca resolvidos |
| 28 | Não registrava quando oportunidade fechava | `DetectionClose` com duration + max edge |
| 29 | Ordem de categorias genérica antes de específica | `elections > politics`, etc. |
| 30 | `unknown` default = 7.2% (inflava fee) | Agora 5% |

## Sanity alerts em tempo real

Todos aparecem em `alerts_active` e em `lab.log`:

- `P90_EDGE_SUSPICIOUS`: P90 > 10% em qualquer hora → verificar se ghost
  filter quebrou
- `CONCENTRATION_HIGH`: >50% das detecções em 1h são do mesmo `event_idx` →
  possível bug persistente naquele mercado
- `DEPTH_BUCKET_DOMINANCE`: >90% das detecções em 1 bucket de depth → cálculo
  de depth pode estar errado de novo
- `CHECKPOINT_STALE`: última persistência há mais que 2× intervalo esperado
- `BUG_15_REGRESSION`: ids duplicados num batch incremental (não devia
  acontecer nunca; se acontecer, sei imediatamente)
- `PERSIST_FAILED`: exceção ao escrever parquet

## Estrutura do output

```
output/run_2026-04-20_04h30m/
├── lab.log                          # log humano (tail -f pra ver em vivo)
├── status.json                      # resumo live, sobrescrito a cada 30s
├── markets.json                     # universo inicial (80 events, com tokens)
├── detections_2026-04-20T04-45-00.parquet    # 1º checkpoint (15min)
├── detections_2026-04-20T05-00-00.parquet    # 2º checkpoint
├── ...
├── detections_final.parquet         # flush final (inclui últimos <15min)
├── decay_*.parquet                  # medições de edge decay
└── closes_*.parquet                 # eventos de fechamento de oportunidade
```

Para 16h: ~64 checkpoints de detecções + idem de decay + idem de closes. 
Arquivos pequenos (< 1MB cada normalmente). `ls` fica ok.

## Se quiser matar no meio

`Ctrl+C` dentro do tmux, ou `kill -TERM <pid>` de outro terminal. O handler
graceful dispara o shutdown: tasks terminam, último checkpoint é forçado com
tag `final`, status.json é atualizado uma última vez.
