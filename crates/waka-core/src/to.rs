use std::{io::Write, path::PathBuf};

use anyhow::anyhow;
use csvs_convert::{
    DescribeOptions, Options, csvs_to_ods_with_options, csvs_to_parquet_with_options,
    csvs_to_postgres_with_options, csvs_to_sqlite_with_options, csvs_to_xlsx_with_options,
    make_datapackage,
};
use serde::Deserialize;
use tracing::debug;

use crate::{
    config::{self, Delimiter},
    util,
    util::process_input,
};

#[allow(dead_code)]
#[derive(Deserialize)]
struct Args {
    cmd_postgres:       bool,
    arg_postgres:       Option<String>,
    cmd_sqlite:         bool,
    arg_sqlite:         Option<String>,
    cmd_parquet:        bool,
    arg_parquet:        Option<String>,
    cmd_xlsx:           bool,
    arg_xlsx:           Option<String>,
    cmd_ods:            bool,
    arg_ods:            Option<String>,
    cmd_datapackage:    bool,
    arg_datapackage:    Option<String>,
    arg_input:          Vec<PathBuf>,
    flag_delimiter:     Option<Delimiter>,
    flag_schema:        Option<String>,
    flag_separator:     Option<String>,
    flag_all_strings:   bool,
    flag_dump:          bool,
    flag_drop:          bool,
    flag_evolve:        bool,
    flag_stats:         bool,
    flag_stats_csv:     Option<String>,
    flag_jobs:          Option<usize>,
    flag_print_package: bool,
    flag_quiet:         bool,
    flag_pipe:          bool,
}

static EMPTY_STDIN_ERRMSG: &str =
    "No data on stdin. Need to add connection string as first argument then the input CSVs";

pub fn run(argv: &[&str]) -> anyhow::Result<()> {
    let args: Args = util::get_args("", argv)?;
    tracing::debug!("'to' command running");
    let mut options = Options::builder()
        .delimiter(args.flag_delimiter.map(config::Delimiter::as_byte))
        .schema(args.flag_schema.unwrap_or_default())
        .seperator(args.flag_separator.unwrap_or_else(|| " ".into()))
        .all_strings(args.flag_all_strings)
        .evolve(args.flag_evolve)
        .stats(args.flag_stats)
        .pipe(args.flag_pipe)
        .stats_csv(args.flag_stats_csv.unwrap_or_default())
        .drop(args.flag_drop)
        .threads(util::njobs(args.flag_jobs))
        .build();

    let output;
    let mut arg_input = args.arg_input.clone();
    let tmpdir = tempfile::tempdir()?;

    if args.cmd_postgres {
        debug!("converting to PostgreSQL");
        arg_input = process_input(arg_input, &tmpdir, EMPTY_STDIN_ERRMSG)?;
        if args.flag_dump {
            options.dump_file = args.arg_postgres.expect("checked above");
            output = csvs_to_postgres_with_options(String::new(), arg_input, options)?;
        } else {
            output = csvs_to_postgres_with_options(
                args.arg_postgres.expect("checked above"),
                arg_input,
                options,
            )?;
        }
        debug!("conversion to PostgreSQL complete");
    } else if args.cmd_sqlite {
        debug!("converting to SQLite");
        arg_input = process_input(arg_input, &tmpdir, EMPTY_STDIN_ERRMSG)?;
        if args.flag_dump {
            options.dump_file = args.arg_sqlite.expect("checked above");
            output = csvs_to_sqlite_with_options(String::new(), arg_input, options)?;
        } else {
            output = csvs_to_sqlite_with_options(
                args.arg_sqlite.expect("checked above"),
                arg_input,
                options,
            )?;
        }
        debug!("conversion to SQLite complete");
    } else if args.cmd_parquet {
        debug!("converting to Parquet");
        arg_input = process_input(arg_input, &tmpdir, EMPTY_STDIN_ERRMSG)?;
        output = csvs_to_parquet_with_options(
            args.arg_parquet.expect("checked above"),
            arg_input,
            options,
        )?;
        debug!("conversion to Parquet complete");
    } else if args.cmd_xlsx {
        debug!("converting to Excel XLSX");
        arg_input = process_input(arg_input, &tmpdir, EMPTY_STDIN_ERRMSG)?;

        output =
            csvs_to_xlsx_with_options(args.arg_xlsx.expect("checked above"), arg_input, options)?;
        debug!("conversion to Excel XLSX complete");
    } else if args.cmd_ods {
        debug!("converting to ODS");
        arg_input = process_input(arg_input, &tmpdir, EMPTY_STDIN_ERRMSG)?;

        output =
            csvs_to_ods_with_options(args.arg_ods.expect("checked above"), arg_input, options)?;
        debug!("conversion to ODS complete");
    } else if args.cmd_datapackage {
        debug!("creating Data Package");
        arg_input = process_input(arg_input, &tmpdir, EMPTY_STDIN_ERRMSG)?;

        let describe_options = DescribeOptions::builder()
            .delimiter(options.delimiter)
            .stats(options.stats)
            .threads(options.threads)
            .stats_csv(options.stats_csv);
        output = make_datapackage(arg_input, PathBuf::new(), &describe_options.build())?;
        let file = std::fs::File::create(args.arg_datapackage.expect("checked above"))?;
        serde_json::to_writer_pretty(file, &output)?;
        debug!("Data Package complete");
    } else {
        return Err(anyhow!(
            "Need to supply either xlsx, ods, parquet, postgres, sqlite, datapackage as subcommand"
        ));
    }

    if args.flag_print_package {
        println!(
            "{}",
            serde_json::to_string_pretty(&output).expect("values should be serializable")
        );
    } else if !args.flag_quiet && !args.flag_dump {
        let empty_array = vec![];
        for resource in output["resources"].as_array().unwrap_or(&empty_array) {
            let mut stdout = std::io::stdout();
            writeln!(&mut stdout)?;
            if args.flag_pipe {
                writeln!(
                    &mut stdout,
                    "Table '{}'",
                    resource["name"].as_str().unwrap_or("")
                )?;
            } else {
                writeln!(
                    &mut stdout,
                    "Table '{}' ({} rows)",
                    resource["name"].as_str().unwrap_or(""),
                    resource["row_count"].as_i64().unwrap_or(0)
                )?;
            }

            writeln!(&mut stdout)?;

            let mut tabwriter = tabwriter::TabWriter::new(stdout);

            if args.flag_pipe {
                writeln!(
                    &mut tabwriter,
                    "{}",
                    ["Field Name", "Field Type"].join("\t")
                )?;
            } else {
                writeln!(
                    &mut tabwriter,
                    "{}",
                    ["Field Name", "Field Type", "Field Format"].join("\t")
                )?;
            }

            for field in resource["schema"]["fields"]
                .as_array()
                .unwrap_or(&empty_array)
            {
                writeln!(
                    &mut tabwriter,
                    "{}",
                    [
                        field["name"].as_str().unwrap_or(""),
                        field["type"].as_str().unwrap_or(""),
                        field["format"].as_str().unwrap_or("")
                    ]
                    .join("\t")
                )?;
            }
            tabwriter.flush()?;
        }
        let mut stdout = std::io::stdout();
        writeln!(&mut stdout)?;
    }

    Ok(())
}
