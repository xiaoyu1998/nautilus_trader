# nautilus-predict-fun

[NautilusTrader](https://nautilustrader.io) adapter skeleton for [Predict.fun](https://predict.fun).

MVP focus: **discovery + REST orderbook + WebSocket orderbook / wallet events** against the public **testnet** API.
EIP-712 order signing and full execution settlement are still stubbed.

Docs: <https://dev.predict.fun/>

## Endpoints (defaults)

| Surface | URL | Auth |
| ------- | --- | ---- |
| REST testnet (default) | `https://api-testnet.predict.fun` | None |
| REST mainnet | `https://api.predict.fun` | `x-api-key` |
| WS (testnet & mainnet) | `wss://ws.predict.fun/ws` | Public topics work without key in practice; JWT for `predictWalletEvents`. Override with `PREDICT_FUN_WS_URL`. |

## Environment

| Variable | Meaning |
| -------- | ------- |
| `PREDICT_FUN_ENV` | `testnet` (default) or `mainnet` |
| `PREDICT_FUN_HTTP_URL` | Override REST base URL |
| `PREDICT_FUN_WS_URL` | Override WS URL |
| `PREDICT_FUN_API_KEY` | API key (`x-api-key`); required for mainnet WS/REST |
| `PREDICT_FUN_JWT` | Wallet JWT for `predictWalletEvents/{jwt}` (ws-exec) |
| `PREDICT_FUN_MARKET_ID` | Default market id for ws-data when no CLI arg (overrides BTC 5m auto-pick) |

Do **not** commit API keys, JWTs, or private keys.

## Sample bins

> **WebSocket note:** Docs publish `wss://ws.predict.fun/ws` (there is currently no resolving `ws-testnet` host). Connections without `PREDICT_FUN_API_KEY` can open and ack subscriptions / heartbeats, but orderbook snapshots/updates typically require a valid API key. Invalid keys get HTTP `401` on handshake.

Run from the repo root:

```sh
# List open categories (events) and nested markets (testnet)
cargo run -p nautilus-predict-fun --bin predict-fun-event-discovery

# Search / filter markets (CLI query; falls back to client-side filter if /v1/search fails)
cargo run -p nautilus-predict-fun --bin predict-fun-search-markets -- bnb

# REST orderbook snapshot (optional market id arg)
cargo run -p nautilus-predict-fun --bin predict-fun-orderbook
cargo run -p nautilus-predict-fun --bin predict-fun-orderbook -- 437

# Fetch auth message to sign (no private key used)
cargo run -p nautilus-predict-fun --bin predict-fun-auth-message

# WebSocket orderbook stream (CTRL+C to stop)
# Default: current live BTC 5m window (slug unix nearest to / containing now)
cargo run -p nautilus-predict-fun --bin predict-fun-ws-data
cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- --btc-5m
cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- 437
# Or pass a category slug / predict.fun market URL:
cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- btc-updown-5m-1792848300
cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- \
  https://predict.fun/market/btc-updown-5m-1792848300

# WebSocket wallet / order events (requires PREDICT_FUN_JWT)
PREDICT_FUN_JWT='...' cargo run -p nautilus-predict-fun --bin predict-fun-ws-exec
```

Mainnet BTC 5m example:

```sh
PREDICT_FUN_ENV=mainnet PREDICT_FUN_API_KEY=... \
  cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- --btc-5m
```

### BTC 5m discovery

Rolling **Bitcoin Up or Down (5 minutes)** windows use category slugs like
`btc-updown-5m-<unixTs>` (window start unix seconds, length 300s). The HTTP helper
walks `GET /v1/categories?status=OPEN` (pagination), filters that prefix, and
picks the **current live** window: prefer slug where `start <= now < start+300`,
otherwise the OPEN window whose start is nearest to now. It does **not** pick the
farthest-future OPEN listing. You can also pass that slug or a
`https://predict.fun/market/<slug>` URL on the `predict-fun-ws-data` CLI.
`/v1/markets?categorySlug=` is tried for exact slug resolve, with category walk
as fallback.

### Obtaining `PREDICT_FUN_JWT`

1. Run `predict-fun-auth-message` and copy the message.
2. Sign it with your wallet.
3. `POST /v1/auth` (see [docs](https://dev.predict.fun/)) to receive a JWT.
4. `export PREDICT_FUN_JWT='...'` — EIP-712 auth signing is not wired in this MVP yet.

## MVP status

| Area | Status |
| ---- | ------ |
| HTTP client (categories, markets, orderbook, search, auth message) | Implemented |
| BTC 5m / slug-prefix category discovery helper | Implemented |
| Sample discovery / search / orderbook / auth bins | Implemented |
| WebSocket orderbook (`predict-fun-ws-data`, default BTC 5m) | Implemented |
| WebSocket wallet events (`predict-fun-ws-exec`, JWT via env) | Implemented |
| InstrumentProvider / BinaryOption mapping | Stub / TODO |
| EIP-712 signing + place/cancel orders | Stub / TODO |
| Execution client / settlement | Stub / TODO |

## License

LGPL-3.0-only — see the repository `LICENSE`.

## BTC 5m live selection

`--btc-5m` / default market discovery uses **sample-bin** helper
`bin/btc_5m.rs` (`select_live_btc_5m_market`), not the adapter library or HTTP
client. It builds `btc-updown-5m-<floor(now/300)*300>` and calls
`GET /v1/categories/{slug}`. Live markets typically have `status=REGISTERED` +
`tradingStatus=OPEN`.

## Market point rewards (PP)

Market objects from `GET /v1/markets`, `GET /v1/markets/{id}`, and nested
`markets` under categories include:

```json
"rewards": {
  "current": { "hourlyRate": 3000, "startsAt": "...", "endsAt": "..." },
  "schedule": [ { "hourlyRate": 3000, "startsAt": "...", "endsAt": "..." } ]
}
```

- `rewards.current.hourlyRate` — Predict Points per hour while the period is active (or `null` if none accruing).
- `rewards.schedule` — past/future periods for the market.
- Live BTC 5m windows often use `hourlyRate=3000` for the 5-minute span (~250 PP for the full window).

`Market::rewards_summary()` / `current_hourly_rate()` expose this in the adapter;
`farming` and `predict-fun-ws-data` log it when selecting a market.
