pub mod drop;
pub mod drr;
pub mod packet;
pub mod port;
pub mod sink;
pub mod source;
pub mod switch;

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
