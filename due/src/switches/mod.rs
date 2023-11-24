pub mod switch;

#[derive(Clone)]
pub enum SchedulingDiscipline {
    DRR,
    FIFO,
}
