use rand::Rng;
use rand_distr::Distribution;
use sim::Time;

pub struct FixedDistribution(pub Time);

impl Distribution<Time> for FixedDistribution {
    fn sample<R: Rng + ?Sized>(&self, _: &mut R) -> Time {
        self.0
    }
}