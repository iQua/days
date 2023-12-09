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

use asynchronix::model::{Model, Output};

use crate::endpoints::drr::DRRServer;
use crate::endpoints::packet::Packet;
use crate::endpoints::port::Port;
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

pub enum Scheduler {
    DRRServer(DRRServer),
    Port(Port),
}

impl Scheduler {
    pub fn output(&self) -> Output<Packet> {
        match self {
            Scheduler::DRRServer(drr_server) => drr_server.output,
            Scheduler::Port(port) => port.output,
        }
    }

    pub fn scheduler(&self) -> impl Model {
        match self {
            Scheduler::DRRServer(drr_server) => drr_server,
            Scheduler::Port(port) => port,
        }
    }
}
