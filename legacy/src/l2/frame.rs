//! Link-layer frame types.

use crate::flows::packet::Packet;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum LinkFrame {
    Data(Packet),
    #[cfg(feature = "l2_pfc")]
    Pfc(super::pfc::PfcFrame),
}

impl LinkFrame {
    pub fn size_bytes(&self) -> usize {
        match self {
            LinkFrame::Data(packet) => packet.size,
            #[cfg(feature = "l2_pfc")]
            LinkFrame::Pfc(frame) => frame.size_bytes(),
        }
    }

    pub fn time(&self) -> f64 {
        match self {
            LinkFrame::Data(packet) => packet.time,
            #[cfg(feature = "l2_pfc")]
            LinkFrame::Pfc(frame) => frame.time,
        }
    }

    pub fn set_time(&mut self, time: f64) {
        match self {
            LinkFrame::Data(packet) => {
                packet.time = time;
            }
            #[cfg(feature = "l2_pfc")]
            LinkFrame::Pfc(frame) => {
                frame.time = time;
            }
        }
    }
}

impl From<Packet> for LinkFrame {
    fn from(packet: Packet) -> Self {
        LinkFrame::Data(packet)
    }
}
