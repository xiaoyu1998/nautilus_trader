use nautilus_common::actor::DataActor;
use nautilus_common::timer::TimeEvent;
use nautilus_model::data::Bar;
use ustr::Ustr;

use super::{constants, PumpDumpDetectorStrategy};

impl DataActor for PumpDumpDetectorStrategy {
    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.handle_bar(bar);
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        for bar in bars {
            self.handle_bar(bar);
        }
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if event.name == Ustr::from(constants::POOL_POLL_TIMER_NAME) {
            self.process_pool_commands();
        }
        Ok(())
    }

    fn on_start(&mut self) -> anyhow::Result<()> {
        let strategy_id = self.actor_id();
        log::info!("Starting PumpDumpDetectorStrategy {strategy_id}");
        self.process_pool_commands();
        self.start_pool_poll_timer();
        log::info!("PumpDumpDetectorStrategy started: {strategy_id}");
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        if self.pool_cmd_queue.is_some() {
            self.clock().cancel_timer(constants::POOL_POLL_TIMER_NAME);
        }
        let strategy_id = self.actor_id();
        log::info!("PumpDumpDetectorStrategy stopped: {strategy_id}");
        Ok(())
    }
}
