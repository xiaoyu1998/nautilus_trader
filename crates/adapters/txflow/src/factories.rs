//! Factory for TxFlow live data clients.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{
    common::consts::TXFLOW,
    config::TxFlowDataClientConfig,
    data::TxFlowDataClient,
};

impl ClientConfig for TxFlowDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct TxFlowDataClientFactory;

impl TxFlowDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for TxFlowDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let cfg = config
            .as_any()
            .downcast_ref::<TxFlowDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for TxFlowDataClientFactory. Expected TxFlowDataClientConfig, was {config:?}",
                )
            })?
            .clone();
        let client = TxFlowDataClient::new(ClientId::from(name), cfg)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        TXFLOW
    }

    fn config_type(&self) -> &'static str {
        "TxFlowDataClientConfig"
    }
}
