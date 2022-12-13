// This is a separate crate because it is more convenient to do it this way.
// Benches that heavily utilize async are unwelcome instandard benchmarking mechanism.
// Using separate binaries in parsley_durable_log crate causes dependencies
// to be merged together for library and benches. It can be split by using features,
// but it is inconvenient to pass them every time.
// Also ideally I wanted to have dev dependecy on workspace-wise test_utils crate.
// Which means I need to have a dev dependency and a non dev dependency gated by a feature
// So ended up with separate create because its way simpler.
use histogram::Histogram;

pub fn display_histogram(name: &'static str, h: Histogram, is_duration: bool) {
    let dur = if is_duration { ":dur" } else { "" };

    println!("{name}.min{dur}={}", h.minimum().unwrap().to_string());
    println!("{name}.max{dur}={}", h.maximum().unwrap().to_string());
    println!("{name}.stddev{dur}={}", h.stddev().unwrap().to_string());
    println!("{name}.mean{dur}={}", h.mean().unwrap().to_string());
    for percentile in (10..90).step_by(10).chain([95, 99].into_iter()) {
        println!(
            "{name}.p{}{dur}={}",
            percentile,
            h.percentile(percentile as f64).unwrap().to_string()
        );
    }
    println!(
        "{name}.p{}{dur}={}",
        99.9,
        h.percentile(99.9 as f64).unwrap().to_string()
    );
}
