//! Factory for Aster live data clients.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{
    common::consts::ASTER,
    config::AsterDataClientConfig,
    data::AsterDataClient,
};

impl ClientConfig for AsterDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Aster data clients.
#[derive(Debug, Clone, Default)]
pub struct AsterDataClientFactory;

impl AsterDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for AsterDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let cfg = config
            .as_any()
            .downcast_ref::<AsterDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for AsterDataClientFactory. Expected AsterDataClientConfig, was {config:?}",
                )
            })?
            .clone();
        let client = AsterDataClient::new(ClientId::from(name), cfg)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ASTER
    }

    fn config_type(&self) -> &'static str {
        "AsterDataClientConfig"
    }
}
