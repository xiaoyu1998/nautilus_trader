# lightbot-strategies

Trading strategies for the LightBot platform.

This crate contains various trading strategies organized by category:

- **momentum**: Momentum-based strategies (e.g., EMA cross)
- **base**: Base utilities and helper modules

## Structure

Each strategy is implemented as a library module and has a corresponding binary entry point in `src/bin/`:

- Strategy implementation: `src/momentum/ema_cross.rs`
- Binary entry point: `src/bin/ema_cross.rs`

## Running Strategies

```bash
# Run EMA cross strategy
cargo run --bin strategy-ema-cross

# Build all strategies
cargo build --bins
```

## Adding New Strategies

1. Create the strategy module in the appropriate category directory (e.g., `src/momentum/`)
2. Implement the `Strategy` trait from `lightbot-trading`
3. Create a binary entry point in `src/bin/`
4. Add the binary to `Cargo.toml` under `[[bin]]`

