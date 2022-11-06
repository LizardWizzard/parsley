use std::{io, path::PathBuf, time::Duration};

use cli_table::{format::Justify, print_stdout, Cell, Style, Table};
use pico_args::Arguments;

use crate::{
    error::error,
    run::{RunResult, Value},
};

pub struct CompareArgs {
    output_dir: PathBuf,
    base_key: String,
    other_key: String,
}

impl CompareArgs {
    pub fn parse(args: &mut Arguments) -> Result<Self, io::Error> {
        let output_dir = args.value_from_str("--output-dir").map_err(error)?;
        let base_key = args.value_from_str("--base-key").map_err(error)?;
        let other_key = args.value_from_str("--other-key").map_err(error)?;
        Ok(Self {
            output_dir,
            base_key,
            other_key,
        })
    }
}

fn format_diff(base: &Value, other: &Value) -> io::Result<String> {
    // TODO calculate diff as percent from the original value
    if base == other {
        return Ok(format!("{other}"));
    }

    match (base, other) {
        (Value::Abs(a), Value::Abs(b)) => {
            let diff = a - b;
            let ratio = diff / a * 100.; // as percent
            let sign = if diff.is_sign_positive() { "+" } else { "" };

            Ok(format!("{b} {sign}{diff:.2} {sign}({ratio:.2}%)"))
        }
        (Value::Abs(a), Value::Dur(b)) => Err(error(format!(
            "value for key are of different type {:?} {:?}",
            a, b
        ))),
        (Value::Dur(a), Value::Abs(b)) => Err(error(format!(
            "value for key are of different type {:?} {:?}",
            a, b
        ))),
        (Value::Dur(a), Value::Dur(b)) => {
            let (sign, diff_micros) = if a > b {
                // sign = -
                // a - 100; b - 90;
                // diff = 10; ratio = 0.1
                ("-", (*a - *b).as_micros())
            } else {
                // sign = +
                // a = 100; b = 110
                // diff = 10; ratio = 0.1
                ("+", (*b - *a).as_micros())
            };

            let ratio = diff_micros as f64 / a.as_micros() as f64;
            let diff_duration = Duration::from_micros(diff_micros.try_into().expect("bad micros"));

            Ok(format!(
                "{b:?} {sign}{diff_duration:?} {sign}({ratio:.2}%)",
                ratio = ratio * 100.
            ))
        }
    }
}

pub fn compare(args: CompareArgs) -> io::Result<()> {
    if !args.output_dir.exists() {
        return Err(error("output dir doesnt exist"));
    }

    let base_key_path = args.output_dir.join(args.base_key);

    if !base_key_path.exists() {
        return Err(error("base key dir doesnt exist"));
    }

    let other_key_path = args.output_dir.join(args.other_key);

    if !other_key_path.exists() {
        return Err(error("other key dir doesnt exist"));
    }

    let base_summary = RunResult::from_dir(&base_key_path)?.summary()?;
    let other_summary = RunResult::from_dir(&other_key_path)?.summary()?;

    let mut data = vec![];

    for (base, other) in base_summary.data.iter().zip(other_summary.data.iter()) {
        if base.key != other.key {
            return Err(error(format!(
                "runs produced different keys: {} != {}",
                base.key, other.key
            )));
        }
        data.push([
            (&base.key).cell(),
            base.min.cell().justify(Justify::Right),
            format_diff(&base.min, &other.min)?
                .cell()
                .justify(Justify::Right),
            base.max.cell().justify(Justify::Right),
            format_diff(&base.max, &other.max)?
                .cell()
                .justify(Justify::Right),
            base.mean.cell().justify(Justify::Right),
            format_diff(&base.mean, &other.mean)?
                .cell()
                .justify(Justify::Right),
        ])
    }

    let table = data
        .table()
        .title(vec![
            "key".cell().bold(true),
            "min (base)".cell().bold(true),
            "min (other)".cell().bold(true),
            "max (base)".cell().bold(true),
            "max (other)".cell().bold(true),
            "mean (base)".cell().bold(true),
            "mean (other)".cell().bold(true),
        ])
        .bold(true);

    print_stdout(table)
}
