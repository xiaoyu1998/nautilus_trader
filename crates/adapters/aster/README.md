# nautilus-aster

AsterDEX **USDT-M perpetual** public market-data adapter (MVP).

**Scope:** list trading perps via REST + stream BBO (`bookTicker`). No orders / auth.

## Endpoints

| Kind | URL |
|------|-----|
| REST base | `https://fapi.asterdex.com` |
| Exchange info | `GET /fapi/v1/exchangeInfo` |
| Book ticker | `GET /fapi/v1/ticker/bookTicker` |
| Premium / funding | `GET /fapi/v1/premiumIndex` |
| WS base | `wss://fstream.asterdex.com` |
| Single BBO | `/ws/<symbol>@bookTicker` (symbol lowercase) |
| All BBO | `/ws/!bookTicker` |

API shape is Binance-futures compatible ([docs](https://docs.asterdex.com/for-developers/aster-api/api-documentation)).

## Layout

```
src/
  lib.rs
  common/     urls + consts
  http/       exchangeInfo + bookTicker + premiumIndex
  websocket/  bookTicker streams
bin/
  list_markets.rs
  bbo_watch.rs
```

## Run

```bash
cd /home/yucili/work/trading/nautilus_trader
export HTTPS_PROXY=http://127.0.0.1:8118   # if needed
export HTTP_PROXY=http://127.0.0.1:8118

cargo run -p nautilus-aster --bin aster-list-markets
cargo run -p nautilus-aster --bin aster-bbo-watch -- --symbol BTCUSDT
```
