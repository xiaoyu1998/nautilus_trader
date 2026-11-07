# nautilus-txflow (MVP stub)

Public market-data stub for [TxFlow](https://docs.txflow.com/api.md) USDC perpetuals.

| | |
|--|--|
| REST | `POST https://rpc.txflow.com/info` (`x-api-version: 2026-08-16`) |
| WS | `wss://rpc.txflow.com/ws` |
| Meta | `perpMeta` |
| BBO | WS channel `bbo` (`coin` = numeric instrument `index` as string) |
| Order book (deltas) | WS channel `l2BookDepth` → Nautilus `OrderBookDeltas` (snapshot + delta; managed cache book) |
| Order book (full snap) | WS channel `l2Book` (not wired in MVP; use `l2BookDepth`) |
| Funding | `marketTicker.fundingRate` (percent / settlement; usually hourly) |
| Default taker | VIP0 `userCross` **0.045% = 450 ppm** |

Full pairwise edge monitor lives in `/home/yucili/work/trading/cross-exchange` (`md_watch`).

```bash
export HTTPS_PROXY=http://127.0.0.1:8118 HTTP_PROXY=http://127.0.0.1:8118
cargo run -p nautilus-txflow --bin txflow-list-markets
```
