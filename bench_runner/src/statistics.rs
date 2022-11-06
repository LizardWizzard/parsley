//! Simple statistical functions adopted from
//! https://rust-lang-nursery.github.io/rust-cookbook/science/mathematics/statistics.html

use num_traits as num;
use std::iter::Sum;

pub fn mean<'a, T>(data: &'a [T]) -> Option<f64>
where
    T: Sum<&'a T>,
    T: num::NumCast,
{
    let sum: f64 = num::cast(data.iter().sum::<T>()).expect("failed to convert");
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::statistics::mean;

    #[test]
    fn test_mean() {
        assert_eq!(mean(&[1, 2, 3, 4]).unwrap(), 2.5);
    }
}
