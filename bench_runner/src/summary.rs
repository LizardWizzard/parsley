use std::{io, path::PathBuf};

use pico_args::Arguments;

use crate::{error::error, run::RunResult};

pub struct SummaryArgs {
    output_dir: PathBuf,
    key: String,
}

impl SummaryArgs {
    pub fn parse(args: &mut Arguments) -> Result<Self, io::Error> {
        let output_dir = args.value_from_str("--output-dir").map_err(error)?;
        let key = args.value_from_str("--key").map_err(error)?;
        Ok(Self { output_dir, key })
    }
}

pub fn summary(args: SummaryArgs) -> io::Result<()> {
    if !args.output_dir.exists() {
        return Err(error("output dir doesnt exist"));
    }

    let key_path = args.output_dir.join(args.key);

    if !key_path.exists() {
        return Err(error("key dir doesnt exist"));
    }

    RunResult::from_dir(&key_path)?.summary()?.to_stdout()?;

    Ok(())
}
