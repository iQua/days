pub mod build;
pub mod drop;
pub mod drr;
pub mod flow;
pub mod packet;
pub mod port;
pub mod route;
pub mod sink;
pub mod source;
pub mod switch;
pub mod topo;

use serde;
use serde::Deserialize;

use crate::endpoints::sink::PacketSink;
use crate::endpoints::source::PacketSource;

#[derive(Debug)]
pub enum EndPoint {
    PacketSource(PacketSource),
    PacketSink(PacketSink),
}

impl EndPoint {
    pub fn id(&self) -> usize {
        match self {
            EndPoint::PacketSource(source) => source.id(),
            EndPoint::PacketSink(sink) => sink.id(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum SchedulingDiscipline {
    DRR,
    FIFO,
}
