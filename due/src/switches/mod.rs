pub mod switch;
use serde;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum SchedulingDiscipline {
    DRR,
    FIFO,
}
