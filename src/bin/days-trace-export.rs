use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(ValueEnum, Debug, Clone, Copy)]
enum ExportFormat {
    /// Export as newline-delimited JSON (lossless).
    Ndjson,
    /// Export as a generated `.tla` module defining `Trace == << ... >>` (rescaled for TLC).
    Tla,
}

#[derive(Parser, Debug)]
#[command(name = "days-trace-export")]
struct Cli {
    /// Input `*_events.csv` file produced by Days.
    #[arg(long)]
    input: PathBuf,

    /// Output file path.
    ///
    /// Defaults:
    /// - `--format ndjson`: `--input` with extension replaced by `.ndjson`
    /// - `--format tla`: `<dirname(--input)>/<module_name>.tla`
    #[arg(long)]
    output: Option<PathBuf>,

    /// Export format.
    #[arg(long, value_enum, default_value_t = ExportFormat::Ndjson)]
    format: ExportFormat,

    /// TLA module name when `--format tla` is used.
    #[arg(long, default_value = "TraceData")]
    module_name: String,

    /// Disable canonical sorting by `(time_ns, event_id)` before exporting.
    #[arg(long, default_value_t = false)]
    no_sort: bool,
}

fn main() {
    let cli = Cli::parse();

    let output = cli.output.unwrap_or_else(|| match cli.format {
        ExportFormat::Ndjson => days::utils::trace_export::default_ndjson_output_path(&cli.input),
        ExportFormat::Tla => cli
            .input
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(format!("{}.tla", cli.module_name)),
    });

    let result = match cli.format {
        ExportFormat::Ndjson => {
            days::utils::trace_export::export_csv_to_ndjson(&cli.input, &output, !cli.no_sort)
        }
        ExportFormat::Tla => days::utils::trace_export::export_csv_to_tla_trace_module(
            &cli.input,
            &output,
            !cli.no_sort,
            &cli.module_name,
        ),
    };

    if let Err(e) = result {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
