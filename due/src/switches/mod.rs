pub mod switch;
use serde;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum SchedulingDiscipline {
    DRR,
    FIFO,
}

// impl<'de> Deserialize<'de> for SchedulingDiscipline {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         let discipline = String::deserialize(deserializer)?;
//         println!("discipline: {}", discipline);
//         match discipline.as_str() {
//             "FIFO" => Ok(SchedulingDiscipline::FIFO),
//             "DRR" => Ok(SchedulingDiscipline::DRR),
//             _ => Err(serde::de::Error::custom("Invalid scheduling discipline")),
//         }
//     }
// }
