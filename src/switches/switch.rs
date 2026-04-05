//! Implements a packet switch with a demultiplexer based on flow classes.

use std::collections::HashMap;

use log::debug;
use tracing::instrument;

use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::flows::packet::Packet;
use crate::next_switch_id;

pub struct PacketSwitch {
    switch_id: usize,

    /// locally maintained simulation time
    pub time: f64,

    /// the number of packets received by the switch
    packets_received: usize,
    /// the flow information base (FIB) of the switch
    /// flow_id -> switch_id
    pub fib: HashMap<usize, usize>,
    /// the reverse flow information base (FIB) of the switch, used by TCP
    /// flow_id -> switch_id
    pub r_fib: HashMap<usize, usize>,

    /// senders for sending inbound packets to outbound ports
    /// switch_id -> outputs to downstream schedulers or endpoints
    pub outputs: HashMap<usize, Output<Packet>>,
}

impl PacketSwitch {
    pub fn new(fib: HashMap<usize, usize>, r_fib: HashMap<usize, usize>) -> PacketSwitch {
        // the senders from the demultiplexer to ports inside the switch
        let mut outputs = HashMap::new();

        for switch_id in fib.values() {
            if !outputs.contains_key(switch_id) {
                outputs.insert(*switch_id, Output::default());
            }
        }

        PacketSwitch {
            switch_id: next_switch_id(),
            fib,
            r_fib,
            packets_received: 0,
            outputs,
            time: 0.0, // Initialize local simulation time
        }
    }

    pub fn id(&self) -> usize {
        self.switch_id
    }

    pub fn set_fib(&mut self, flow_id: usize, next_id: usize) {
        self.fib.insert(flow_id, next_id);
    }

    pub fn set_r_fib(&mut self, flow_id: usize, next_id: usize) {
        self.r_fib.insert(flow_id, next_id);
    }

    /// Runtime-safe FIB installation entrypoint for external simulators.
    pub async fn install_fib(&mut self, entry: (usize, usize), _cx: &Context<Self>) {
        self.set_fib(entry.0, entry.1);
    }

    /// Runtime-safe reverse-FIB installation entrypoint for external simulators.
    pub async fn install_r_fib(&mut self, entry: (usize, usize), _cx: &Context<Self>) {
        self.set_r_fib(entry.0, entry.1);
    }

    #[instrument(skip(self, _cx))]
    pub async fn packet_received(&mut self, packet: Packet, _cx: &Context<Self>) {
        #[cfg(feature = "test")]
        {
            let global_time = _cx
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();
            let local_time = self.time;

            // makes sure that the current simulation time can be correctly retrieved from
            // the packet itself
            assert!(
                packet.time <= global_time + 1e-7,
                "Timing mismatch for flow {} packet {}: packet.time = {}, global_time = {}",
                packet.flow_id,
                packet.packet_id,
                packet.time,
                global_time
            );

            // makes sure that the simulation advances in time
            assert!((global_time - local_time).abs() <= 1e-7 || global_time > local_time);
        }

        self.time = _cx
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();

        if packet.ack.is_none() && packet.control.is_none() {
            self.packets_received += 1;

            debug!(
                "PacketSwitch {} received packet {} ({} bytes) from flow {} at time {:.3}. \
                        {} packets received.",
                self.switch_id,
                packet.packet_id,
                packet.size,
                packet.flow_id,
                self.time,
                self.packets_received
            );

            // forwards packets that are not acknowledgments to their corresponding downstream elements
            let Some(switch_id) = self.fib.get(&packet.flow_id).copied() else {
                debug!(
                    "PacketSwitch {} missing FIB entry for flow {}",
                    self.switch_id, packet.flow_id
                );
                return;
            };

            if let Some(output) = self.outputs.get_mut(&switch_id) {
                output.send(packet).await;
            }
        } else {
            debug!(
                "PacketSwitch {} received control packet {} ({} bytes) from flow {} at time {:.3}.",
                self.switch_id, packet.packet_id, packet.size, packet.flow_id, self.time,
            );

            // forwards acknowledgment packets to their corresponding upstream elements
            let Some(switch_id) = self.r_fib.get(&packet.flow_id).copied() else {
                debug!(
                    "PacketSwitch {} missing reverse FIB entry for flow {}",
                    self.switch_id, packet.flow_id
                );
                return;
            };

            if let Some(output) = self.outputs.get_mut(&switch_id) {
                output.send(packet).await;
            }
        }
    }
}

impl Model for PacketSwitch {
    type Env = ();
}
