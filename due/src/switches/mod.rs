pub mod switch;

use serde;
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum SchedulingDiscipline {
    DRR,
    FIFO,
    WFQ,
}
