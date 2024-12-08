//! Implements a reporter whose role is to transmit reports from sources, sinks, and schedulers to
//! the UserInterface coroutine.
use log::debug;

use crate::flows::sink::PacketSinkReport;
use crate::flows::source::PacketSourceReport;
use crate::schedulers::SchedulerReport;

#[derive(Clone, Debug)]
pub enum Report {
    PacketSourceReport(PacketSourceReport),
    SchedulerReport(SchedulerReport),
    PacketSinkReport(PacketSinkReport),
}

#[derive(Debug, PartialEq)]
pub enum ReportTiming {
    InProgress,
    Final,
}

enum ElementType {
    Source,
    Scheduler,
    Sink,
}

pub struct Reporter {
    pub report_interval: f64,
}

impl Reporter {
    pub fn new(report_interval: f64) -> Reporter {
        Reporter { report_interval }
    }

    pub fn report(report: Report, timing: ReportTiming) {
        debug!("Sending a report with timing.");
    }
}
