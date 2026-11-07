//! Aster data client configuration.

use nautilus_model::instruments::InstrumentAny;

/// Live data-client config (public market data MVP).
#[derive(Debug, Clone, Default)]
pub struct AsterDataClientConfig {
    /// Instruments published on connect (and used for subscribe symbol maps).
    pub seed_instruments: Vec<InstrumentAny>,
}

impl AsterDataClientConfig {
    #[must_use]
    pub fn with_seed_instruments(mut self, instruments: Vec<InstrumentAny>) -> Self {
        self.seed_instruments = instruments;
        self
    }
}
