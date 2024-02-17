//! Implements a packet switch with a demultiplexer based on flow classes.

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::flows::progress::Report;
use crate::flows::statistics::RandomVar;
use crate::next_switch_id;

#[derive(Clone, Debug)]
pub struct PacketSwitchStatistics {
    switch_name: String,
    /// the arrival times of the packets
    arrival_times: RandomVar,
    /// the last arrival time
    last_arrival_time: f64,
    /// the inter-arrival times of the packets
    inter_arrival_times: RandomVar,
    /// the one-way end-to-end delays of the packets
    one_way_delays: RandomVar,
    /// the total time spent waiting in queues
    queueing_delays: RandomVar,
    /// the size of the packets
    packet_sizes: RandomVar,
}

impl Display for PacketSwitchStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} recorded statistics: \n\
            Arrival times: {:#.3} \n\
            Inter-arrival times: {:#.3} \n\
            One-way delays: {:#.3} \n\
            Queueing delays: {:#.3} \n\
            Packet sizes: {:#.3} \n",
            self.switch_name,
            self.arrival_times,
            self.inter_arrival_times,
            self.one_way_delays,
            self.queueing_delays,
            self.packet_sizes,
        )
    }
}

impl PacketSwitchStatistics {
    pub fn new(switch_name: String) -> Self {
        PacketSwitchStatistics {
            switch_name,
            arrival_times: RandomVar::new(),
            last_arrival_time: 0.0,
            inter_arrival_times: RandomVar::new(),
            one_way_delays: RandomVar::new(),
            queueing_delays: RandomVar::new(),
            packet_sizes: RandomVar::new(),
        }
    }

    pub fn update(&mut self, packet: &Packet, now: f64) {
        self.arrival_times.tabulate(now);
        self.inter_arrival_times
            .tabulate(now - self.last_arrival_time);
        self.last_arrival_time = now;
        self.one_way_delays.tabulate(now - packet.creation_time);
        self.queueing_delays.tabulate(packet.queueing_delay);
        self.packet_sizes.tabulate(packet.size as u32);
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
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        if packet.ack.is_none() {
            self.packets_received += 1;

            debug!(
                "PacketSwitch {} received packet {} ({} bytes) from flow {} at time {:.3}. \
                {} packets received.",
                self.switch_id,
                packet.packet_id,
                packet.size,
                packet.flow_id,
                arrival_time,
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
                self.switch_id, packet.packet_id, packet.size, packet.flow_id, arrival_time,
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
            let name = format!("{self}");
            self.report_output
                .send(Report {
                    name,
                    finished: false,
                })
                .await;

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
