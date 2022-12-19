use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::{Debug, Display},
    fs,
    io::{self, BufRead, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use cli_table::{format::Justify, print_stdout, Cell, Style, Table};
use pico_args::Arguments;

use crate::{error::error, statistics};

// --- target
// | - key1
//      | - 1 run
//      | - 2 run
// | - key2
//      | - 1 run
//      | - 2 run

pub struct RunArgs {
    // Target command to run
    target: String,
    // Directory to store output in
    output_dir: PathBuf,
    // Iterations to run for
    num_iterations: NonZeroUsize,
    // Benchmark code name
    key: String,
    // Whether to show summary after run
    show_summary: bool,
}

impl RunArgs {
    pub fn parse(args: &mut Arguments) -> Result<Self, io::Error> {
        let target = args.value_from_str("--target").map_err(error)?;
        let output_dir = args.value_from_str("--output-dir").map_err(error)?;
        let num_iterations = args.value_from_str("--iterations").map_err(error)?;
        let key = args.value_from_str("--key").map_err(error)?;
        let show_summary = args.contains("--show-summary");

        Ok(Self {
            target,
            output_dir,
            num_iterations: NonZeroUsize::new(num_iterations)
                .ok_or(error("num iterations is zero".to_string()))?,
            key,
            show_summary,
        })
    }
}

#[derive(Default)]
pub struct IterationResult {
    // Probably can be avoided by using a btree map with a key containing
    // counter and a key with a comparator using only counter to compare
    // It will be more code, it is not critical so use simpler option with
    // separate vec with keys in proper order
    keys_in_order: Vec<String>,
    values: HashMap<String, u64>,
}

impl IterationResult {
    fn record(&mut self, key: String, value: u64) -> Result<(), io::Error> {
        match self.values.entry(key.clone()) {
            Entry::Occupied(o) => Err(error(format!(
                "got duplicate key {} in result map",
                o.key()
            ))),
            Entry::Vacant(v) => {
                v.insert(value);
                self.keys_in_order.push(key.to_owned());
                Ok(())
            }
        }
    }

    pub fn from_output(output: &[u8]) -> Result<IterationResult, io::Error> {
        let mut result = IterationResult::default();

        // TODO skip lines that are not in format. warn about those
        // use some prefix like `record:` or something
        for line in output.lines() {
            let line = line?;
            let mut split = line.split('=');
            let err = || error(format!("failed to parse output line: \"{}\"", line));
            let key = split.next().ok_or_else(err)?;
            let value: u64 = split.next().ok_or_else(err)?.parse().map_err(|_| err())?;

            result.record(key.to_string(), value)?;
        }
        Ok(result)
    }
}

// Transform into durations?
// test with "echo foo=123"
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value {
    Abs(f64),
    Dur(Duration),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Abs(a) => Display::fmt(a, f),
            Value::Dur(d) => Debug::fmt(d, f),
        }
    }
}

#[derive(Debug)]
pub struct ValueSummary {
    pub key: String,
    pub min: Value,
    pub max: Value,
    pub mean: Value,
}

#[derive(Default, Debug)]
pub struct RunSummary {
    pub data: Vec<ValueSummary>,
}

impl RunSummary {
    pub fn to_stdout(&self) -> io::Result<()> {
        if self.data.is_empty() {
            println!("No summary can be shown because there was no output");
        }

        let mut data = vec![];
        for value in &self.data {
            data.push([
                (&value.key).cell(),
                value.min.cell().justify(Justify::Right),
                value.max.cell().justify(Justify::Right),
                value.mean.cell().justify(Justify::Right),
            ])
        }

        let table = data
            .table()
            .title(vec![
                "key".cell().bold(true),
                "min".cell().bold(true),
                "max".cell().bold(true),
                "mean".cell().bold(true),
            ])
            .bold(true);

        print_stdout(table)
    }
}

// bool indicates whether value should be treated as duration
fn parse_key(k: &str) -> (&str, bool) {
    if let Some(k) = k.strip_suffix(":dur") {
        (k, true)
    } else {
        (k, false)
    }
}

#[derive(Default)]
pub struct RunResult {
    data: Vec<IterationResult>,
}

impl RunResult {
    pub fn from_dir(dir: &Path) -> io::Result<Self> {
        let mut iteration_results = vec![];
        for dir_entry in fs::read_dir(&dir)? {
            let path = dir_entry?.path();
            iteration_results.push(IterationResult::from_output(&fs::read(path)?)?);
        }

        if iteration_results.is_empty() {
            return Err(error("no results found in key dir"));
        }

        Ok(Self {
            data: iteration_results,
        })
    }

    pub fn summary(&self) -> io::Result<RunSummary> {
        assert!(!self.data.is_empty());

        let keys = &self.data[0].keys_in_order;

        for iteration_result in &self.data {
            if &iteration_result.keys_in_order != keys {
                return Err(error(format!(
                    "runs have different keys: expected {:?} got {:?}",
                    keys, iteration_result.keys_in_order
                )));
            }
        }

        let mut summary = RunSummary::default();

        let mut values = Vec::with_capacity(keys.len());
        for raw_key in keys {
            let (key, is_duration) = parse_key(raw_key);

            values.clear();
            for iteration_result in &self.data {
                let value = iteration_result.values[raw_key];
                values.push(value)
            }

            // inefficient, minmax, not too many experiments, so should be fine
            // unwrap is ok since we assert for non emptiness
            let min = *values.iter().min().unwrap();
            let max = *values.iter().max().unwrap();
            let mean = statistics::mean(&values).unwrap();
            if is_duration {
                summary.data.push(ValueSummary {
                    key: key.to_string(),
                    min: Value::Dur(Duration::from_micros(min)),
                    max: Value::Dur(Duration::from_micros(max)),
                    mean: Value::Dur(Duration::from_secs_f64(mean / (10usize.pow(6)) as f64)),
                });
            } else {
                summary.data.push(ValueSummary {
                    key: key.to_string(),
                    min: Value::Abs(min as f64),
                    max: Value::Abs(max as f64),
                    mean: Value::Abs(mean),
                });
            }
        }

        Ok(summary)
    }
}

fn record_single_run(
    key_dir: &Path,
    run_id: usize,
    command: &mut Command,
) -> Result<IterationResult, io::Error> {
    let output = command.output()?;
    if !output.status.success() {
        return Err(error(format!(
            "Run failed. Stdout: {} Stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    let result_fname = key_dir.join(run_id.to_string());
    fs::write(result_fname, &output.stdout)?;
    IterationResult::from_output(&output.stdout)
}

// TODO provide stat about divergence between runs (stddev?)
pub fn run(args: RunArgs) -> Result<(), io::Error> {
    println!("Benchmarking: {}", args.target);

    let mut target = args.target.split(' ');
    let bin = target
        .next()
        .ok_or_else(|| error(format!("specified target is invalid: failed to get binary")))
        .map(|bin| Path::new(bin))?;

    if !bin.exists() {
        return Err(error(format!("{} doesnt exist", bin.display())));
    }

    let bin_args = target.collect::<Vec<&str>>();

    fs::create_dir_all(&args.output_dir)?;

    let key_dir = args.output_dir.join(args.key);
    if key_dir.exists() {
        fs::remove_dir_all(&key_dir)?
    }
    fs::create_dir_all(&key_dir)?;

    let mut command = Command::new(&bin);
    command.args(&bin_args);

    let mut result = RunResult::default();

    let run_t0 = Instant::now();
    for run_id in 0..args.num_iterations.get() {
        let iteration_t0 = Instant::now();

        print!("Running iteration {}...", run_id);
        io::stdout().flush().expect("stdout flush");

        let iteration_result = record_single_run(&key_dir, run_id, &mut command)?;

        result.data.push(iteration_result);

        println!("{:?}", iteration_t0.elapsed());
        io::stdout().flush().expect("stdout flush");
    }
    println!("Done in {:?}", run_t0.elapsed());

    if args.show_summary {
        let summary = result.summary()?;
        summary.to_stdout()?;
    }
    Ok(())
}
