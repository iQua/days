//! Implements a packet switch with a demultiplexer based on flow classes.

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::flows::progress::Report;
use crate::next_switch_id;

#[derive(Clone, Debug)]
pub struct PacketSwitchReport {
    pub id: u32,
    /// the start time of this report interval
    pub start_time: f64,
    /// the end time of this report interval
    pub end_time: f64,
    /// the number of received packets in this report interval
    pub received_packets: u32,
    pub dropped_packets: u32,
    pub forwarded_packets: u32,
    pub queue_length: u32,
    /// the size of received packets in this report interval
    pub received_sizes: u32,
    pub forwarded_sizes: u32,
    pub throughput_mean: f64,
    /// the mean of queueing delays of the packets
    pub queueing_delay_mean: f64,
}

impl PacketSwitchReport {
    pub fn new(id: u32, start_time: f64) -> Self {
        PacketSwitchReport {
            id,
            start_time,
            end_time: 0.0,
            received_packets: 0,
            dropped_packets: 0,
            forwarded_packets: 0,
            queue_length: 0,
            received_sizes: 0,
            forwarded_sizes: 0,
            throughput_mean: 0.0,
            queueing_delay_mean: 0.0,
        }
    }

    pub fn receive_update(&mut self, packet: &Packet) {
        self.received_packets += 1;
        self.received_sizes += packet.size as u32;
    }

    pub fn forward_update(&mut self, packet: &Packet) {
        self.forwarded_packets += 1;
        self.forwarded_sizes += packet.size as u32;
    }

    pub fn drop_update(&mut self, packet: &Packet) {
        self.dropped_packets += 1;
    }
}

pub struct PacketSwitch {
    switch_id: usize,
    /// the number of packets received by the switch
    packets_received: usize,
    /// the flow information base (FIB) of the switch
    /// flow_id -> switch_id
    fib: HashMap<usize, usize>,
    /// the reverse flow information base (FIB) of the switch, used by TCP
    /// flow_id -> switch_id
    r_fib: HashMap<usize, usize>,

    /// senders for sending inbound packets to outbound ports
    /// switch_id -> outputs to downstream schedulers or endpoints
    pub outputs: HashMap<usize, Output<Packet>>,

    /// the report of a report interval
    pub report: PacketSwitchReport,
    /// the interval of sending a periodic report to the progress coroutine
    report_interval: f64,
    /// the sender for sedning reports
    pub report_output: Output<Report>,
}

impl std::fmt::Display for PacketSwitch {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PacketSwitch {}", self.id())
    }
}

impl PacketSwitch {
    pub fn new(
        fib: HashMap<usize, usize>,
        r_fib: HashMap<usize, usize>,
        report_interval: f64,
    ) -> PacketSwitch {
        let switch_id = next_switch_id();
        let switch_name = format!("PacketSwitch {switch_id}");

        // the senders from the demultiplexer to ports inside the switch
        let mut outputs = HashMap::new();

        for switch_id in fib.values() {
            if !outputs.contains_key(switch_id) {
                outputs.insert(*switch_id, Output::default());
            }
        }

        PacketSwitch {
            switch_id,
            fib,
            r_fib,
            packets_received: 0,
            outputs,
            report: PacketSwitchReport::new(switch_id as u32, 0.0),
            report_interval,
            report_output: Output::default(),
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

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();

        if packet.ack.is_none() {
            self.packets_received += 1;

            self.report.receive_update(&packet);

            debug!(
                "PacketSwitch {} received packet {} ({} bytes) from flow {} at time {:.3}. \
                {} packets received.",
                self.switch_id,
                packet.packet_id,
                packet.size,
                packet.flow_id,
                now,
                self.packets_received
            );

            // forwards packets that are not acknowledgment to their
            // corresponding downstream elements
            let switch_id = self.fib[&packet.flow_id];

            if let Some(output) = self.outputs.get_mut(&switch_id) {
                output.send(packet).await;
            }
        } else {
            debug!(
                "PacketSwitch {} received ack of packet {} ({} bytes) from flow {} at time {:.3}.",
                self.switch_id, packet.packet_id, packet.size, packet.flow_id, now,
            );

            // forwards acknowledgment packets to their corresponding upstream
            // elements
            let switch_id = self.r_fib[&packet.flow_id];

            if let Some(output) = self.outputs.get_mut(&switch_id) {
                output.send(packet).await;
            }
        }
    }

    /// Sends a perioid report of current statistics to the progress coroutine.
    fn send_report<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            self.report.end_time = scheduler
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();

            let report = Report::PacketSwitchReport(self.report.clone());

            self.report_output.send(report).await;

            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.report_interval),
                    Self::send_report,
                    (),
                )
                .unwrap();
        }
    }
}

impl Model for PacketSwitch {
    fn init(
        self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.report_interval),
                    Self::send_report,
                    (),
                )
                .unwrap();

            self.into()
        })
    }
}
