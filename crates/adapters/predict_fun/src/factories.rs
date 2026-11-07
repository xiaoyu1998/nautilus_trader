// -------------------------------------------------------------------------------------------------
//! Factory for Predict.fun live data clients.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{
    common::consts::PREDICT_FUN,
    config::PredictFunDataClientConfig,
    data::PredictFunDataClient,
};

impl ClientConfig for PredictFunDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Predict.fun data clients.
#[derive(Debug, Clone, Default)]
pub struct PredictFunDataClientFactory;

impl PredictFunDataClientFactory {
    /// Creates a new factory.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for PredictFunDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let cfg = config
            .as_any()
            .downcast_ref::<PredictFunDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for PredictFunDataClientFactory. Expected PredictFunDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let client = PredictFunDataClient::new(client_id, cfg)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        PREDICT_FUN
    }

    fn config_type(&self) -> &'static str {
        "PredictFunDataClientConfig"
    }
}
