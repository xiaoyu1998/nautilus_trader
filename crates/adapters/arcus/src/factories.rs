//! Factories for Arcus live data and execution clients.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, TraderId},
};

use crate::{
    common::consts::ARCUS,
    config::{ArcusDataClientConfig, ArcusExecutionClientConfig},
    data::ArcusDataClient,
    execution::ArcusExecutionClient,
};

impl ClientConfig for ArcusDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for ArcusExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Arcus data clients.
#[derive(Debug, Clone, Default)]
pub struct ArcusDataClientFactory;

impl ArcusDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for ArcusDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let cfg = config
            .as_any()
            .downcast_ref::<ArcusDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for ArcusDataClientFactory. Expected ArcusDataClientConfig, was {config:?}",
                )
            })?
            .clone();
        let client = ArcusDataClient::new(ClientId::from(name), cfg)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ARCUS
    }

    fn config_type(&self) -> &'static str {
        "ArcusDataClientConfig"
    }
}

/// Factory for creating Arcus execution clients.
#[derive(Debug, Clone, Default)]
pub struct ArcusExecutionClientFactory;

impl ArcusExecutionClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for ArcusExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let arcus_config = config
            .as_any()
            .downcast_ref::<ArcusExecutionClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for ArcusExecutionClientFactory. Expected ArcusExecutionClientConfig, was {config:?}",
                )
            })?
            .clone();

        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            *crate::common::consts::ARCUS_VENUE,
            OmsType::Netting,
            arcus_config.account_id,
            AccountType::Margin,
            None,
            cache,
        );

        let client = ArcusExecutionClient::new(core, arcus_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ARCUS
    }

    fn config_type(&self) -> &'static str {
        "ArcusExecutionClientConfig"
    }
}
