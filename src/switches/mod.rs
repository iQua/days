pub mod switch;

use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum SchedulingDiscipline {
    DRR,
    FIFO,
    SP,
    VirtualClock,
    WFQ,
    WRR,
}
