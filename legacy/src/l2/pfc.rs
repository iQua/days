//! Priority-based Flow Control (PFC) frame and egress gate.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use serde::{Deserialize, Serialize};

use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::flows::packet::Packet;
use crate::l2::frame::LinkFrame;
use crate::utils::logger::{CsvLogger, Report, ReportTiming};
use crate::utils::time::{quantize_after, quantize_time};

const NUM_PRIORITIES: usize = 8;
const PAUSE_QUANTA_BITS: f64 = 512.0;
const PFC_FRAME_SIZE_BYTES: usize = 64;

#[cfg(feature = "lean")]
fn to_ns(time_s: f64) -> u64 {
    (time_s.max(0.0) * 1e9).round() as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PfcFrame {
    pub time: f64,
    pub sender_id: u64,
    pub receiver_id: u64,
    pub pfc_frame_id: u64,
    /// Bitmask of enabled priorities (bit i => priority i affected).
    pub class_enable: u8,
    /// Pause quanta per priority.
    pub pause_quanta: [u16; NUM_PRIORITIES],
}

#[derive(Debug, Clone)]
pub struct PfcConfig {
    pub xoff: [usize; NUM_PRIORITIES],
    pub xon: [usize; NUM_PRIORITIES],
    pub pause_quanta: [u16; NUM_PRIORITIES],
    /// Per-priority ingress buffer capacity in bytes (0 for unlimited).
    pub buffer_capacity: [usize; NUM_PRIORITIES],
    /// Optional pause refresh interval in seconds.
    pub refresh_interval: Option<f64>,
    /// Optional drain retry interval in seconds.
    pub drain_interval: Option<f64>,
}

pub use days::utils::logger::PfcPortReport;

impl PfcFrame {
    pub fn new(
        time: f64,
        sender_id: u64,
        receiver_id: u64,
        pfc_frame_id: u64,
        class_enable: u8,
        pause_quanta: [u16; NUM_PRIORITIES],
    ) -> Self {
        Self {
            time,
            sender_id,
            receiver_id,
            pfc_frame_id,
            class_enable,
            pause_quanta,
        }
    }

    pub fn size_bytes(&self) -> usize {
        PFC_FRAME_SIZE_BYTES
    }

    pub fn enabled(&self, priority: usize) -> bool {
        (self.class_enable & (1 << priority)) != 0
    }

    pub fn pause_duration(&self, priority: usize, rate_bps: f64) -> f64 {
        if rate_bps <= 0.0 {
            return 0.0;
        }
        let quanta = self.pause_quanta[priority] as f64;
        (quanta * PAUSE_QUANTA_BITS) / rate_bps
    }
}

pub struct PfcIngressPort {
    port_id: usize,
    peer_gate_id: usize,
    /// locally maintained simulation time
    pub time: f64,
    /// configuration parameters
    config: PfcConfig,
    /// per-priority queue occupancy in bytes
    occupancy: [usize; NUM_PRIORITIES],
    /// per-priority pause state (true if XOFF asserted)
    pause_active: [bool; NUM_PRIORITIES],
    /// per-priority refresh schedule
    refresh_scheduled_at: [Option<f64>; NUM_PRIORITIES],
    /// scheduled drain retry time, if any
    drain_scheduled_at: Option<f64>,
    /// per-priority queues
    queues: Vec<VecDeque<Packet>>,
    /// closure indicating whether a packet can be forwarded downstream
    can_forward: Arc<dyn Fn(&Packet) -> bool + Send + Sync>,

    pub output: Output<Packet>,
    pub pfc_output: Output<PfcFrame>,

    report_start_time: f64,
    total_occupancy: usize,
    max_occupancy: usize,
    pause_frames_sent: usize,
    resume_frames_sent: usize,
    dropped_packets: usize,
}

impl PfcIngressPort {
    const REFRESH_SID: SchedulableId<Self, usize> = SchedulableId::__from_decorated(0);
    const DRAIN_RETRY_SID: SchedulableId<Self, f64> = SchedulableId::__from_decorated(1);
    const LOG_REPORT_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(2);

    pub fn new(
        port_id: usize,
        peer_gate_id: usize,
        config: PfcConfig,
        can_forward: Arc<dyn Fn(&Packet) -> bool + Send + Sync>,
    ) -> Self {
        Self {
            port_id,
            peer_gate_id,
            time: 0.0,
            config,
            occupancy: [0; NUM_PRIORITIES],
            pause_active: [false; NUM_PRIORITIES],
            refresh_scheduled_at: [None; NUM_PRIORITIES],
            drain_scheduled_at: None,
            queues: (0..NUM_PRIORITIES).map(|_| VecDeque::new()).collect(),
            can_forward,
            output: Output::default(),
            pfc_output: Output::default(),
            report_start_time: 0.0,
            total_occupancy: 0,
            max_occupancy: 0,
            pause_frames_sent: 0,
            resume_frames_sent: 0,
            dropped_packets: 0,
        }
    }

    pub fn id(&self) -> usize {
        self.port_id
    }

    async fn send_pfc(&mut self, now: f64, priority: usize, pause_quanta: u16) {
        #[cfg(feature = "lean")]
        let pfc_frame_id = CsvLogger::next_pfc_frame_id();
        #[cfg(not(feature = "lean"))]
        let pfc_frame_id = 0;

        let mut quanta = [0u16; NUM_PRIORITIES];
        quanta[priority] = pause_quanta;
        let frame = PfcFrame::new(
            now,
            self.port_id as u64,
            self.peer_gate_id as u64,
            pfc_frame_id,
            1 << priority,
            quanta,
        );

        #[cfg(feature = "lean")]
        {
            let queue_occupancy_bytes = self.occupancy[priority] as u64;
            let xoff_threshold_bytes = self.config.xoff[priority] as u64;
            let xon_threshold_bytes = self.config.xon[priority] as u64;
            let buffer_capacity_bytes = self.config.buffer_capacity[priority] as u64;
            let refresh_interval_ns = self.config.refresh_interval.map(to_ns);
            let drain_interval_ns = self.config.drain_interval.map(to_ns);

            let event = crate::utils::logger::PfcEventRow {
                time_ns: to_ns(now),
                event_id: CsvLogger::next_pfc_event_id(),
                kind: crate::utils::logger::PfcEventKind::PfcSent,
                sender_id: frame.sender_id,
                receiver_id: frame.receiver_id,
                priority: priority as u8,
                pfc_frame_id: frame.pfc_frame_id,
                class_enable: frame.class_enable,
                pause_quanta,
                queue_occupancy_bytes: Some(queue_occupancy_bytes),
                xoff_threshold_bytes: Some(xoff_threshold_bytes),
                xon_threshold_bytes: Some(xon_threshold_bytes),
                buffer_capacity_bytes: Some(buffer_capacity_bytes),
                refresh_interval_ns,
                drain_interval_ns,
            };
            CsvLogger::try_log_report(Report::PfcEventRow(event), ReportTiming::InProgress);
        }

        if pause_quanta > 0 {
            self.pause_frames_sent += 1;
        } else {
            self.resume_frames_sent += 1;
        }
        self.pfc_output.send(frame).await;
    }

    fn schedule_refresh(&mut self, now: f64, priority: usize, cx: &Context<Self>) {
        if let Some(interval) = self.config.refresh_interval {
            let refresh_at = quantize_after(now, interval);
            let should_schedule = match self.refresh_scheduled_at[priority] {
                Some(existing) => refresh_at < existing - f64::EPSILON,
                None => true,
            };
            if should_schedule {
                self.refresh_scheduled_at[priority] = Some(refresh_at);
                cx.schedule_event(
                    Duration::from_secs_f64((refresh_at - now).max(0.0)),
                    &Self::REFRESH_SID,
                    priority,
                )
                .unwrap();
            }
        }
    }

    async fn assert_pause(&mut self, now: f64, priority: usize, cx: &Context<Self>) {
        let now = quantize_time(now);
        if !self.pause_active[priority] {
            self.pause_active[priority] = true;
        }
        let pause_quanta = self.config.pause_quanta[priority];
        if pause_quanta > 0 {
            self.send_pfc(now, priority, pause_quanta).await;
            self.schedule_refresh(now, priority, cx);
        }
    }

    async fn clear_pause(&mut self, now: f64, priority: usize) {
        let now = quantize_time(now);
        if self.pause_active[priority] {
            self.pause_active[priority] = false;
            self.refresh_scheduled_at[priority] = None;
            self.send_pfc(now, priority, 0).await;
        }
    }

    async fn handle_packet(&mut self, packet: Packet, now: f64, cx: &Context<Self>) {
        let now = quantize_time(now);
        let priority = packet.priority as usize;
        let cap = self.config.buffer_capacity[priority];
        if cap > 0 && self.occupancy[priority] + packet.size > cap {
            self.dropped_packets += 1;
            debug!(
                "PfcIngressPort {} dropped packet {} from flow {} (priority {}) at time {:.3}.",
                self.port_id, packet.packet_id, packet.flow_id, priority, now
            );
            return;
        }

        self.occupancy[priority] += packet.size;
        self.total_occupancy += packet.size;
        if self.total_occupancy > self.max_occupancy {
            self.max_occupancy = self.total_occupancy;
        }
        self.queues[priority].push_back(packet);

        if self.occupancy[priority] >= self.config.xoff[priority] {
            self.assert_pause(now, priority, cx).await;
        }

        self.drain(now, cx).await;

        if self.pause_active[priority] && self.occupancy[priority] <= self.config.xon[priority] {
            self.clear_pause(now, priority).await;
        }
    }

    async fn drain(&mut self, now: f64, cx: &Context<Self>) {
        let now = quantize_time(now);
        self.time = now;
        let mut needs_retry = false;
        for priority in 0..NUM_PRIORITIES {
            while let Some(front) = self.queues[priority].front() {
                if !(self.can_forward)(front) {
                    needs_retry = true;
                    break;
                }
                let mut packet = self.queues[priority].pop_front().unwrap();
                self.occupancy[priority] -= packet.size;
                self.total_occupancy -= packet.size;
                packet.time = now;
                self.output.send(packet).await;
            }
        }

        if needs_retry {
            self.schedule_drain_retry(now, cx);
        }
    }

    fn schedule_drain_retry(&mut self, now: f64, cx: &Context<Self>) {
        let now = quantize_time(now);
        let interval = self.config.drain_interval.unwrap_or(1e-6);
        let retry_at = quantize_after(now, interval);
        let should_schedule = match self.drain_scheduled_at {
            Some(existing) => retry_at < existing - f64::EPSILON,
            None => true,
        };
        if should_schedule {
            self.drain_scheduled_at = Some(retry_at);
            cx.schedule_event(
                Duration::from_secs_f64((retry_at - now).max(0.0)),
                &Self::DRAIN_RETRY_SID,
                retry_at,
            )
            .unwrap();
        }
    }

    pub async fn frame_received(&mut self, frame: LinkFrame, cx: &Context<Self>) {
        #[cfg(feature = "test")]
        {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
            assert!(
                (frame.time() - global_time).abs() <= 1e-7,
                "Timing mismatch: frame.time = {}, global_time = {}",
                frame.time(),
                global_time
            );
        }

        let now = quantize_time(frame.time());
        match frame {
            LinkFrame::Data(packet) => self.handle_packet(packet, now, cx).await,
            #[cfg(feature = "l2_pfc")]
            LinkFrame::Pfc(_) => {
                debug!(
                    "PfcIngressPort {} ignoring inbound PFC frame.",
                    self.port_id
                );
            }
        }
    }

    pub async fn packet_received(&mut self, packet: Packet, cx: &Context<Self>) {
        self.frame_received(LinkFrame::Data(packet), cx).await;
    }

    async fn refresh(&mut self, priority: usize, cx: &Context<Self>) {
        let now = quantize_time(cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64());
        self.refresh_scheduled_at[priority] = None;
        if self.pause_active[priority] {
            self.assert_pause(now, priority, cx).await;
        }
    }

    async fn drain_retry(&mut self, now: f64, cx: &Context<Self>) {
        self.drain_scheduled_at = None;
        self.drain(now, cx).await;
    }

    fn prepare_report(&self, now: f64) -> PfcPortReport {
        PfcPortReport {
            id: self.port_id,
            start_time: self.report_start_time,
            end_time: now,
            pause_frames: self.pause_frames_sent,
            resume_frames: self.resume_frames_sent,
            dropped_packets: self.dropped_packets,
            max_occupancy: self.max_occupancy,
            current_occupancy: self.total_occupancy,
        }
    }

    fn reset_stats(&mut self, now: f64) {
        self.report_start_time = now;
        self.pause_frames_sent = 0;
        self.resume_frames_sent = 0;
        self.dropped_packets = 0;
        self.max_occupancy = self.total_occupancy;
    }

    async fn log_report(&mut self, _: (), cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        let report = self.prepare_report(now);
        CsvLogger::log_report(Report::PfcPortReport(report), ReportTiming::InProgress);
        self.reset_stats(now);
    }
}

impl Model for PfcIngressPort {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::refresh));
        registry.add(cx.register_schedulable(Self::drain_retry));
        registry.add(cx.register_schedulable(Self::log_report));
        registry
    }

    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        let report_interval = CsvLogger::get_instance().get_report_interval();

        if report_interval < f64::MAX {
            cx.schedule_periodic_event(
                Duration::from_secs_f64(report_interval),
                Duration::from_secs_f64(report_interval),
                &Self::LOG_REPORT_SID,
                (),
            )
            .unwrap();
        }

        self.into()
    }
}

pub struct PfcEgressGate {
    gate_id: usize,
    /// locally maintained simulation time
    pub time: f64,
    /// link rate in bits per second
    rate: f64,
    /// per-priority pause deadlines
    paused_until: [f64; NUM_PRIORITIES],
    /// per-priority queues
    queues: Vec<VecDeque<LinkFrame>>,
    /// next priority to consider (round-robin)
    next_prio: usize,
    /// next scheduled resume time, if any
    resume_scheduled_at: Option<f64>,

    pub output: Output<LinkFrame>,
}

impl PfcEgressGate {
    const RESUME_SID: SchedulableId<Self, f64> = SchedulableId::__from_decorated(0);

    pub fn new(gate_id: usize, rate: f64) -> Self {
        Self {
            gate_id,
            time: 0.0,
            rate,
            paused_until: [0.0; NUM_PRIORITIES],
            queues: (0..NUM_PRIORITIES).map(|_| VecDeque::new()).collect(),
            next_prio: 0,
            resume_scheduled_at: None,
            output: Output::default(),
        }
    }

    pub fn id(&self) -> usize {
        self.gate_id
    }

    fn enqueue(&mut self, frame: LinkFrame) {
        match &frame {
            LinkFrame::Data(packet) => {
                let prio = packet.priority as usize;
                self.queues[prio].push_back(frame);
            }
            #[cfg(feature = "l2_pfc")]
            LinkFrame::Pfc(_) => {
                debug!(
                    "PfcEgressGate {} ignoring inbound PFC frame on data path.",
                    self.gate_id
                );
            }
        }
    }

    fn next_ready_priority(&mut self, now: f64) -> Option<usize> {
        for offset in 0..NUM_PRIORITIES {
            let prio = (self.next_prio + offset) % NUM_PRIORITIES;
            if !self.queues[prio].is_empty() && now >= self.paused_until[prio] {
                self.next_prio = (prio + 1) % NUM_PRIORITIES;
                return Some(prio);
            }
        }
        None
    }

    fn pop_ready(&mut self, now: f64) -> Option<LinkFrame> {
        let prio = self.next_ready_priority(now)?;
        let mut frame = self.queues[prio].pop_front()?;
        frame.set_time(now);
        Some(frame)
    }

    fn next_resume_time(&self, now: f64) -> Option<f64> {
        self.queues
            .iter()
            .enumerate()
            .filter_map(|(prio, queue)| {
                if queue.is_empty() {
                    None
                } else {
                    let resume_at = self.paused_until[prio];
                    (resume_at > now).then_some(resume_at)
                }
            })
            .min_by(|a, b| a.partial_cmp(b).unwrap())
    }

    fn apply_pfc(&mut self, frame: &PfcFrame) {
        let now = quantize_time(frame.time);
        for prio in 0..NUM_PRIORITIES {
            if frame.enabled(prio) {
                let pause_quanta = frame.pause_quanta[prio];
                if pause_quanta == 0 {
                    self.paused_until[prio] = now;
                } else {
                    let pause = frame.pause_duration(prio, self.rate);
                    let pause_until = quantize_after(now, pause);
                    self.paused_until[prio] = self.paused_until[prio].max(pause_until);
                }
            }
        }
    }

    async fn drain_ready(&mut self, now: f64) {
        let now = quantize_time(now);
        self.time = now;
        while let Some(frame) = self.pop_ready(now) {
            self.output.send(frame).await;
        }
    }

    fn schedule_resume(&mut self, now: f64, cx: &Context<Self>) {
        let now = quantize_time(now);
        if let Some(resume_at_raw) = self.next_resume_time(now) {
            let resume_at = quantize_time(resume_at_raw);
            let should_schedule = match self.resume_scheduled_at {
                Some(existing) => resume_at < existing - f64::EPSILON,
                None => true,
            };
            if should_schedule {
                self.resume_scheduled_at = Some(resume_at);
                cx.schedule_event(
                    Duration::from_secs_f64((resume_at - now).max(0.0)),
                    &Self::RESUME_SID,
                    resume_at,
                )
                .unwrap();
            }
        } else {
            self.resume_scheduled_at = None;
        }
    }

    pub async fn frame_received(&mut self, frame: LinkFrame, cx: &Context<Self>) {
        #[cfg(feature = "test")]
        {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
            assert!(
                (frame.time() - global_time).abs() <= 1e-7,
                "Timing mismatch: frame.time = {}, global_time = {}",
                frame.time(),
                global_time
            );
        }

        let now = frame.time();
        self.enqueue(frame);
        self.drain_ready(now).await;
        self.schedule_resume(now, cx);
    }

    pub async fn packet_received(&mut self, packet: Packet, cx: &Context<Self>) {
        self.frame_received(LinkFrame::Data(packet), cx).await;
    }

    pub async fn pfc_received(&mut self, frame: PfcFrame, cx: &Context<Self>) {
        #[cfg(feature = "test")]
        {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
            assert!(
                (frame.time - global_time).abs() <= 1e-7,
                "Timing mismatch: frame.time = {}, global_time = {}",
                frame.time,
                global_time
            );
        }

        #[cfg(feature = "lean")]
        {
            for priority in 0..NUM_PRIORITIES {
                if !frame.enabled(priority) {
                    continue;
                }

                let event = crate::utils::logger::PfcEventRow {
                    time_ns: to_ns(frame.time),
                    event_id: CsvLogger::next_pfc_event_id(),
                    kind: crate::utils::logger::PfcEventKind::PfcRecv,
                    sender_id: frame.sender_id,
                    receiver_id: self.gate_id as u64,
                    priority: priority as u8,
                    pfc_frame_id: frame.pfc_frame_id,
                    class_enable: frame.class_enable,
                    pause_quanta: frame.pause_quanta[priority],
                    queue_occupancy_bytes: None,
                    xoff_threshold_bytes: None,
                    xon_threshold_bytes: None,
                    buffer_capacity_bytes: None,
                    refresh_interval_ns: None,
                    drain_interval_ns: None,
                };
                CsvLogger::try_log_report(Report::PfcEventRow(event), ReportTiming::InProgress);
            }
        }

        let now = quantize_time(frame.time);
        self.apply_pfc(&frame);
        self.drain_ready(now).await;
        self.schedule_resume(now, cx);
    }

    async fn resume(&mut self, now: f64, cx: &Context<Self>) {
        let now = quantize_time(now);
        self.resume_scheduled_at = None;
        self.drain_ready(now).await;
        self.schedule_resume(now, cx);
    }

    #[cfg(test)]
    pub fn test_enqueue(&mut self, frame: LinkFrame) {
        self.enqueue(frame);
    }

    #[cfg(test)]
    pub fn test_apply_pfc(&mut self, frame: PfcFrame) {
        self.apply_pfc(&frame);
    }

    #[cfg(test)]
    pub fn test_pop_ready(&mut self, now: f64) -> Option<LinkFrame> {
        self.pop_ready(now)
    }
}

impl Model for PfcEgressGate {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::resume));
        registry
    }
}

#[cfg(all(test, feature = "l2_pfc"))]
mod tests {
    use super::*;
    use crate::flows::packet::Packet;

    #[test]
    fn test_pfc_pause_blocks_priority() {
        let rate_bps = 1e9;
        let mut gate = PfcEgressGate::new(0, rate_bps);

        let mut packet = Packet::new(1500, 1, 0, 0.0);
        packet.set_priority(3);
        gate.test_enqueue(LinkFrame::Data(packet));

        let mut quanta = [0u16; NUM_PRIORITIES];
        quanta[3] = 10;
        let pfc = PfcFrame::new(0.0, 0, 0, 0, 1 << 3, quanta);

        gate.test_apply_pfc(pfc.clone());

        assert!(gate.test_pop_ready(0.0).is_none());

        let resume_at = pfc.pause_duration(3, rate_bps);
        let frame = gate
            .test_pop_ready(resume_at)
            .expect("frame should be ready");
        assert!((frame.time() - resume_at).abs() <= 1e-9);
    }

    #[test]
    fn test_pfc_resume_clears_pause() {
        let rate_bps = 1e9;
        let mut gate = PfcEgressGate::new(0, rate_bps);

        let mut packet = Packet::new(1200, 1, 0, 0.0);
        packet.set_priority(2);
        gate.test_enqueue(LinkFrame::Data(packet));

        let mut quanta = [0u16; NUM_PRIORITIES];
        quanta[2] = 100;
        let pfc_pause = PfcFrame::new(0.0, 0, 0, 0, 1 << 2, quanta);
        gate.test_apply_pfc(pfc_pause);

        let pfc_resume = PfcFrame::new(0.5, 0, 0, 0, 1 << 2, [0u16; NUM_PRIORITIES]);
        gate.test_apply_pfc(pfc_resume);

        let frame = gate
            .test_pop_ready(0.5)
            .expect("frame should be released after resume");
        assert!((frame.time() - 0.5).abs() <= 1e-9);
    }

    #[test]
    fn test_pause_duration_zero_rate() {
        let mut quanta = [0u16; NUM_PRIORITIES];
        quanta[1] = 5;
        let pfc = PfcFrame::new(0.0, 0, 0, 0, 1 << 1, quanta);

        assert_eq!(pfc.pause_duration(1, 0.0), 0.0);
        assert_eq!(pfc.pause_duration(1, -10.0), 0.0);
    }

    #[test]
    fn test_pfc_pause_is_priority_scoped() {
        let rate_bps = 1e9;
        let mut gate = PfcEgressGate::new(0, rate_bps);

        let mut p3 = Packet::new(1500, 1, 0, 0.0);
        p3.set_priority(3);
        gate.test_enqueue(LinkFrame::Data(p3));

        let mut p5 = Packet::new(1500, 2, 0, 0.0);
        p5.set_priority(5);
        gate.test_enqueue(LinkFrame::Data(p5));

        let mut quanta = [0u16; NUM_PRIORITIES];
        quanta[3] = 10;
        let pfc = PfcFrame::new(0.0, 0, 0, 0, 1 << 3, quanta);
        gate.test_apply_pfc(pfc);

        let frame = gate
            .test_pop_ready(0.0)
            .expect("unpaused priority should pass");
        match frame {
            LinkFrame::Data(packet) => assert_eq!(packet.priority, 5),
            _ => panic!("expected data frame"),
        }
    }

    #[test]
    fn test_pfc_pause_extends_on_longer_quanta() {
        let rate_bps = 1e9;
        let mut gate = PfcEgressGate::new(0, rate_bps);

        let mut packet = Packet::new(1500, 1, 0, 0.0);
        packet.set_priority(1);
        gate.test_enqueue(LinkFrame::Data(packet));

        let mut quanta = [0u16; NUM_PRIORITIES];
        quanta[1] = 10;
        let pfc_short = PfcFrame::new(0.0, 0, 0, 0, 1 << 1, quanta);
        gate.test_apply_pfc(pfc_short.clone());

        let mut quanta = [0u16; NUM_PRIORITIES];
        quanta[1] = 20;
        let pfc_long = PfcFrame::new(0.1, 0, 0, 1, 1 << 1, quanta);
        gate.test_apply_pfc(pfc_long.clone());

        let resume_short = pfc_short.pause_duration(1, rate_bps);
        assert!(gate.test_pop_ready(resume_short).is_none());

        let resume_long = pfc_long.pause_duration(1, rate_bps) + 0.1;
        let frame = gate
            .test_pop_ready(resume_long)
            .expect("frame should be ready after extended pause");
        assert!((frame.time() - resume_long).abs() <= 1e-9);
    }
}
