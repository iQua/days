#[derive(Clone, Copy, Debug)]
pub struct PacketSourceReport {
    pub id: usize,
    /// the start time of this report interval
    pub start_time: f64,
    /// the end time of this report interval
    pub end_time: f64,
}

impl PacketSourceReport {
    pub fn new(id: usize) -> Self {
        PacketSourceReport {
            id,
            start_time: 0.0,
            end_time: 1.0,
        }
    }
}

fn main() {
    let a = 1;
    println!("{:?}", a);

    let b = 1;
    println!("{:?}", b);
}

fn log_report_1() -> PacketSourceReport {
    PacketSourceReport::new(1)
}

fn log_report_2() -> PacketSourceReport {
    PacketSourceReport::new(2)
}
