# nautilus-arcus

Arcus perpetual futures adapter for Nautilus Trader (MVP).

## Scope

- **Data**: REST `GET /v1/markets`, `GET /v1/bbo`; WS `bbo` → `QuoteTick`; WS `l2Orderbook` → `OrderBookDeltas` (full snapshots)
- **Execution**: Ed25519-signed REST `placeOrder` / `cancelOrder`; WS `orders` + `userFills` for lifecycle
- Instrument ids: `{TICKER}-PERP.ARCUS` (e.g. `ZEC-USD-PERP.ARCUS`)

Not in this MVP: incremental `l2OrderbookUpdates`, modify-order, mass status / full reconciliation.

## Smoke bins

```sh
# List online markets + ZEC BBO (needs network; proxy via HTTPS_PROXY if required)
cargo run -p nautilus-arcus --bin arcus-list-markets
cargo run -p nautilus-arcus --bin arcus-list-markets -- testnet

# Stream BBO
cargo run -p nautilus-arcus --bin arcus-bbo-watch -- --market ZEC-USD --max-updates 5
```

## Execution env

| Variable | Meaning |
|----------|---------|
| `ARCUS_API_SECRET` | Ed25519 private key hex (32 bytes) |
| `ARCUS_ADDRESS` | Master wallet `0x…` |
| `ARCUS_ACCOUNT_INDEX` | Subaccount index (default 0) |

Register the matching public key via Arcus `POST /v1/createApiKey` (EIP-712) before live orders.
