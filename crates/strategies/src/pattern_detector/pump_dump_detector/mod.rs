mod action;
mod constants;
mod data_actor;
pub mod dashboard;
pub mod trap_patterns;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use nautilus_common::actor::DataActor;
use nautilus_core::nanos::DurationNanos;
use nautilus_model::{
    data::{Bar, BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::{ClientId, InstrumentId},
};
use nautilus_trading::{nautilus_strategy, StrategyConfig, StrategyCore};
use tokio::sync::mpsc;

use constants::{BAR_PERIOD_MINUTES, POOL_POLL_INTERVAL_NS, POOL_POLL_TIMER_NAME};
use trap_patterns::{
    BearishTrapBars, DeathCrossBarScan, GoldenCrossBarScan,
};

pub const EPS: f64 = 1e-12;

#[derive(Debug, Clone)]
pub enum PumpDumpDetectorPoolCommand {
    Add { instrument_id: String },
    Remove { instrument_id: String },
}

#[derive(Debug, Clone)]
pub struct PatternSignalDto {
    pub pattern_type: String,
    pub strength: f64,
    pub first_pivot_ts_event_ns: u64,
    pub second_pivot_ts_event_ns: u64,
}

#[derive(Debug, Clone)]
pub struct PumpDumpDetectorBarChunk {
    pub instrument_id: String,
    pub bar: (u64, f64, f64, f64, f64, f64),
    pub patterns: Vec<PatternSignalDto>,
}

#[derive(Debug)]
pub struct PumpDumpDetectorStrategy {
    core: StrategyCore,
    data_client_id: ClientId,
    pool_cmd_queue: Option<Arc<Mutex<VecDeque<PumpDumpDetectorPoolCommand>>>>,
    bar_tx: Option<mpsc::UnboundedSender<PumpDumpDetectorBarChunk>>,
    pool: HashSet<InstrumentId>,
    golden_cross_by_instrument: HashMap<InstrumentId, GoldenCrossBarScan>,
    death_cross_by_instrument: HashMap<InstrumentId, DeathCrossBarScan>,
}

impl PumpDumpDetectorStrategy {
    #[must_use]
    pub fn new(
        config: StrategyConfig,
        data_client_id: ClientId,
        pool_cmd_queue: Option<Arc<Mutex<VecDeque<PumpDumpDetectorPoolCommand>>>>,
        bar_tx: Option<mpsc::UnboundedSender<PumpDumpDetectorBarChunk>>,
    ) -> Self {
        Self {
            core: StrategyCore::new(config),
            data_client_id,
            pool_cmd_queue,
            bar_tx,
            pool: HashSet::new(),
            golden_cross_by_instrument: HashMap::new(),
            death_cross_by_instrument: HashMap::new(),
        }
    }

    fn bar_type_for(&self, instrument_id: &InstrumentId) -> BarType {
        let bar_spec = BarSpecification::new(
            BAR_PERIOD_MINUTES,
            BarAggregation::Minute,
            PriceType::Last,
        );
        BarType::new(
            instrument_id.clone(),
            bar_spec,
            AggregationSource::External,
        )
    }

    fn subscribe_market(&mut self, instrument_id: &InstrumentId) {
        let bar_type = self.bar_type_for(instrument_id);
        DataActor::subscribe_bars(self, bar_type, Some(self.data_client_id.clone()), None);
        log::info!("PumpDumpDetector subscribed 1m bars for {}", instrument_id);
    }

    fn unsubscribe_market(&mut self, instrument_id: &InstrumentId) {
        let bar_type = self.bar_type_for(instrument_id);
        DataActor::unsubscribe_bars(self, bar_type, Some(self.data_client_id.clone()), None);
        self.golden_cross_by_instrument.remove(instrument_id);
        self.death_cross_by_instrument.remove(instrument_id);
        log::info!("PumpDumpDetector unsubscribed 1m bars for {}", instrument_id);
    }

    pub(super) fn process_pool_commands(&mut self) {
        let Some(q) = &self.pool_cmd_queue else {
            return;
        };
        let mut commands = Vec::new();
        {
            let mut guard = q.lock().expect("pool queue lock");
            while let Some(c) = guard.pop_front() {
                commands.push(c);
            }
        }
        for cmd in commands {
            match cmd {
                PumpDumpDetectorPoolCommand::Add { instrument_id } => {
                    let instrument_id = InstrumentId::from(instrument_id.as_str());
                    if self.pool.insert(instrument_id.clone()) {
                        self.subscribe_market(&instrument_id);
                    }
                }
                PumpDumpDetectorPoolCommand::Remove { instrument_id } => {
                    let instrument_id = InstrumentId::from(instrument_id.as_str());
                    if self.pool.remove(&instrument_id) {
                        self.unsubscribe_market(&instrument_id);
                    }
                }
            }
        }
    }

    fn start_pool_poll_timer(&mut self) {
        if self.pool_cmd_queue.is_none() {
            return;
        }
        if let Err(e) = self.clock().set_timer_ns(
            POOL_POLL_TIMER_NAME,
            DurationNanos::new(POOL_POLL_INTERVAL_NS),
            None,
            None,
            None,
            None,
            None,
        ) {
            log::error!("PumpDumpDetector pool poll timer failed: {e}");
        }
    }

    fn map_golden_cross(det: BearishTrapBars) -> PatternSignalDto {
        PatternSignalDto {
            pattern_type: "golden_cross".to_string(),
            strength: det.strength,
            first_pivot_ts_event_ns: det.first_pivot_ts_event_ns,
            second_pivot_ts_event_ns: det.second_pivot_ts_event_ns,
        }
    }

    fn map_death_cross(det: BearishTrapBars) -> PatternSignalDto {
        PatternSignalDto {
            pattern_type: "death_cross".to_string(),
            strength: det.strength,
            first_pivot_ts_event_ns: det.first_pivot_ts_event_ns,
            second_pivot_ts_event_ns: det.second_pivot_ts_event_ns,
        }
    }

    fn forward_bar_to_dashboard(&mut self, bar: &Bar, patterns: Vec<PatternSignalDto>) {
        let Some(ref tx) = self.bar_tx else {
            return;
        };
        let instrument_id = bar.instrument_id();
        let _ = tx.send(PumpDumpDetectorBarChunk {
            instrument_id: instrument_id.to_string(),
            bar: (
                bar.ts_event.as_u64(),
                bar.open.as_f64(),
                bar.high.as_f64(),
                bar.low.as_f64(),
                bar.close.as_f64(),
                bar.volume.as_f64(),
            ),
            patterns,
        });
    }

    pub(super) fn handle_bar(&mut self, bar: &Bar) {
        let instrument_id = bar.instrument_id();
        let golden_cross_detector = self
            .golden_cross_by_instrument
            .entry(instrument_id.clone())
            .or_insert_with(GoldenCrossBarScan::new_default);
        let death_cross_detector = self
            .death_cross_by_instrument
            .entry(instrument_id.clone())
            .or_insert_with(DeathCrossBarScan::new_default);

        let mut patterns = Vec::new();
        let ts = bar.ts_event.as_u64();
        let open = bar.open.as_f64();
        let high = bar.high.as_f64();
        let low = bar.low.as_f64();
        let close = bar.close.as_f64();
        let volume = bar.volume.as_f64();

        if let Some(det) = golden_cross_detector.update(ts, open, high, low, close, volume) {
            patterns.push(Self::map_golden_cross(det));
        }
        if let Some(det) = death_cross_detector.update(ts, open, high, low, close, volume) {
            patterns.push(Self::map_death_cross(det));
        }

        self.forward_bar_to_dashboard(bar, patterns);
    }
}

nautilus_strategy!(PumpDumpDetectorStrategy);
