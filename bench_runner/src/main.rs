// run specified bench executable,
// store stdout output in specified folder with run id, (run specified number of times)
// report - use statistics acrross several runs
// compare - compare several named benchmarks

// so the ideal workflow: run some benchmark - runs 3 times, (maybe show current result with distribution analysis etc)
// output is plased into <bench_name><user supplied name e g "before"
// run another benchmark (maybe the same one but with different params or with changed code)
// run in compare mode to check what is the difference
// use drop caches in between the runs

use std::io;

use compare::CompareArgs;
use run::RunArgs;
use summary::SummaryArgs;

mod compare;
mod error;
mod run;
mod statistics;
mod summary;

use crate::error::error;

enum Args {
    Run(RunArgs),
    Summary(SummaryArgs),
    Compare(CompareArgs),
}

impl Args {
    fn parse() -> Result<Self, io::Error> {
        let mut pargs = pico_args::Arguments::from_env();

        // TODO help
        // Help has a higher priority and should be handled separately.
        // if pargs.contains(["-h", "--help"]) {
        //     print!("{}", HELP);
        //     std::process::exit(0);
        // }

        let subcommand = pargs
            .subcommand()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .ok_or_else(|| {
                error(String::from(
                    "expected one of the following subcommands: run",
                ))
            })?;

        let args = match subcommand.as_str() {
            "run" => Args::Run(RunArgs::parse(&mut pargs)?),
            "sum" => Args::Summary(SummaryArgs::parse(&mut pargs)?),
            "cmp" => Args::Compare(CompareArgs::parse(&mut pargs)?),
            unknown => {
                return Err(error(format!(
                    "got unknown subcommand {} expected one of: run, sum, cmp",
                    unknown
                )))
            }
        };

        // It's up to the caller what to do with the remaining arguments.
        let remaining = pargs.finish();
        if !remaining.is_empty() {
            return Err(error(format!("Error: unknown arguments: {:?}.", remaining)));
        }
        Ok(args)
    }
}

fn main() -> io::Result<()> {
    match Args::parse()? {
        Args::Run(args) => run::run(args),
        Args::Summary(args) => summary::summary(args),
        Args::Compare(args) => compare::compare(args),
    }
}
