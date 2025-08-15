use std::{
    collections::HashMap,
    fs,
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
};

use cached::{SizedCache, proc_macro::cached};
use dynfmt2::Format;
use foldhash::fast::RandomState;
use geosuggest_core::{
    Engine, EngineData,
    index::{ArchivedCitiesRecord as CitiesRecord, ArchivedCountryRecord as CountryRecord},
    storage,
};
use geosuggest_utils::{IndexUpdater, IndexUpdaterSettings, SourceItem};
use indicatif::{ProgressBar, ProgressDrawTarget};
use log::info;
use phf::phf_map;
use rayon::{
    iter::{IndexedParallelIterator, ParallelIterator},
    prelude::IntoParallelRefIterator,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use tempfile::tempdir;
use url::Url;
use util::expand_tilde;
use uuid::Uuid;

use crate::{
    CliResult,
    clitypes::CliError,
    config::{Config, Delimiter},
    regex_oncelock,
    select::SelectColumns,
    util,
    util::replace_column_value,
};

// Cached regex patterns used throughout the geocode module
// Using module-level statics for better performance
static ADMIN1_CODE_REGEX: fn() -> &'static Regex = || regex_oncelock!(r"^[A-Z]{2}\.[A-Z0-9]{1,8}$");

static LOCATION_REGEX: fn() -> &'static Regex =
    || regex_oncelock!(r"(?-u)([+-]?(?:\d+\.?\d*|\.\d+)),\s*([+-]?(?:\d+\.?\d*|\.\d+))");

static FORMATSTR_REGEX: fn() -> &'static Regex = || regex_oncelock!(r"\{(?P<key>\w+)\}");

#[derive(Deserialize)]
struct Args {
    arg_column:          String,
    arg_location:        String,
    cmd_suggest:         bool,
    cmd_suggestnow:      bool,
    cmd_reverse:         bool,
    cmd_reversenow:      bool,
    cmd_countryinfo:     bool,
    cmd_countryinfonow:  bool,
    cmd_iplookup:        bool,
    cmd_iplookupnow:     bool,
    cmd_index_check:     bool,
    cmd_index_update:    bool,
    cmd_index_load:      bool,
    cmd_index_reset:     bool,
    arg_input:           Option<String>,
    arg_index_file:      Option<String>,
    flag_rename:         Option<String>,
    flag_country:        Option<String>,
    flag_min_score:      Option<f32>,
    flag_admin1:         Option<String>,
    flag_k_weight:       Option<f32>,
    flag_formatstr:      String,
    flag_language:       String,
    flag_invalid_result: Option<String>,
    flag_batch:          usize,
    flag_timeout:        u16,
    flag_cache_dir:      String,
    flag_languages:      String,
    flag_cities_url:     String,
    flag_force:          bool,
    flag_jobs:           Option<usize>,
    flag_new_column:     Option<String>,
    flag_output:         Option<String>,
    flag_delimiter:      Option<Delimiter>,
    flag_progressbar:    bool,
}

#[derive(Clone, Debug)]
struct Admin1Filter {
    admin1_string: String,
    is_code:       bool,
}

#[derive(Clone)]
struct NamesLang {
    cityname:    String,
    admin1name:  String,
    admin2name:  String,
    countryname: String,
}

static QSV_VERSION: &str = env!("CARGO_PKG_VERSION");
static DEFAULT_GEOCODE_INDEX_FILENAME: &str =
    concat!("qsv-", env!("CARGO_PKG_VERSION"), "-geocode-index.rkyv");
static GEOIP2_FILENAME: &str = "GeoLite2-City.mmdb";

static DEFAULT_CITIES_NAMES_URL: &str =
    "https://download.geonames.org/export/dump/alternateNamesV2.zip";
static DEFAULT_CITIES_NAMES_FILENAME: &str = "alternateNamesV2.txt";
static DEFAULT_COUNTRY_INFO_URL: &str = "https://download.geonames.org/export/dump/countryInfo.txt";
static DEFAULT_ADMIN1_CODES_URL: &str =
    "https://download.geonames.org/export/dump/admin1CodesASCII.txt";
static DEFAULT_ADMIN2_CODES_URL: &str = "https://download.geonames.org/export/dump/admin2Codes.txt";

// we use a compile time static perfect hash map for US state FIPS codes
static US_STATES_FIPS_CODES: phf::Map<&'static str, &'static str> = phf_map! {
    "AK" => "02",
    "AL" => "01",
    "AR" => "05",
    "AZ" => "04",
    "CA" => "06",
    "CO" => "08",
    "CT" => "09",
    "DC" => "11",
    "DE" => "10",
    "FL" => "12",
    "GA" => "13",
    "HI" => "15",
    "IA" => "19",
    "ID" => "16",
    "IL" => "17",
    "IN" => "18",
    "KS" => "20",
    "KY" => "21",
    "LA" => "22",
    "MA" => "25",
    "MD" => "24",
    "ME" => "23",
    "MI" => "26",
    "MN" => "27",
    "MO" => "29",
    "MS" => "28",
    "MT" => "30",
    "NC" => "37",
    "ND" => "38",
    "NE" => "31",
    "NH" => "33",
    "NJ" => "34",
    "NM" => "35",
    "NV" => "32",
    "NY" => "36",
    "OH" => "39",
    "OK" => "40",
    "OR" => "41",
    "PA" => "42",
    "RI" => "44",
    "SC" => "45",
    "SD" => "46",
    "TN" => "47",
    "TX" => "48",
    "UT" => "49",
    "VT" => "50",
    "VA" => "51",
    "WA" => "53",
    "WI" => "55",
    "WV" => "54",
    "WY" => "56",
    // the following are territories
    // and are not included in the default index
    // leaving them here for reference
    // "AS" => "60",
    // "GU" => "66",
    // "MP" => "69",
    // "PR" => "72",
    // "UM" => "74",
    // "VI" => "78",
};

// max number of entries in LRU cache
static CACHE_SIZE: usize = 2_000_000;
// max number of entries in fallback LRU cache if we can't allocate CACHE_SIZE
static FALLBACK_CACHE_SIZE: usize = CACHE_SIZE / 4;

static INVALID_DYNFMT: &str = "Invalid dynfmt template.";
static INVALID_COUNTRY_CODE: &str = "Invalid country code.";

// when suggesting with --admin1, how many suggestions to fetch from the engine
// before filtering by admin1
static SUGGEST_ADMIN1_LIMIT: usize = 10;

// valid column values for %dyncols
// when adding new columns, make sure to maintain the sort order
// otherwise, the dyncols check will fail as it uses binary search
static SORTED_VALID_DYNCOLS: [&str; 28] = [
    "admin1",
    "admin2",
    "area",
    "capital",
    "continent",
    "country",
    "country_geonameid",
    "country_name",
    "country_population",
    "currency_code",
    "currency_name",
    "equivalent_fips_code",
    "fips",
    "id",
    "iso3",
    "languages",
    "latitude",
    "longitude",
    "name",
    "neighbours",
    "phone",
    "population",
    "postal_code_format",
    "postal_code_regex",
    "timezone",
    "tld",
    "us_county_fips_code",
    "us_state_fips_code",
];

// dyncols populated sentinel value
static DYNCOLS_POPULATED: &str = "_POPULATED";

// valid subcommands
#[derive(Clone, Copy, PartialEq)]
enum GeocodeSubCmd {
    Suggest,
    SuggestNow,
    Reverse,
    ReverseNow,
    CountryInfo,
    CountryInfoNow,
    Iplookup,
    IplookupNow,
    IndexCheck,
    IndexUpdate,
    IndexLoad,
    IndexReset,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let mut args: Args = util::get_args(USAGE, argv)?;

    if args.flag_new_column.is_some() && args.flag_rename.is_some() {
        return fail_incorrectusage_clierror!(
            "Cannot use --new-column and --rename at the same time."
        );
    }

    if args.flag_new_column.is_some() && args.flag_formatstr.starts_with("%dyncols:") {
        return fail_incorrectusage_clierror!(
            "Cannot use --new-column with the '%dyncols:' --formatstr option."
        );
    }

    // if args.flag_cities_url is a number and is 500, 1000, 5000 or 15000,
    // its a geonames cities file ID and convert it to a URL
    // we do this as a convenience shortcut for users
    if args.flag_cities_url.parse::<u16>().is_ok() {
        let cities_id = args.flag_cities_url;
        // ensure its a valid cities_id - 500, 1000, 5000 or 15000
        if cities_id != "500" && cities_id != "1000" && cities_id != "5000" && cities_id != "15000"
        {
            return fail_incorrectusage_clierror!(
                "Invalid --cities-url: {cities_id} - must be one of 500, 1000, 5000 or 15000"
            );
        }
        args.flag_cities_url =
            format!("https://download.geonames.org/export/dump/cities{cities_id}.zip");
    }

    if let Err(err) = Url::parse(&args.flag_cities_url) {
        return fail_incorrectusage_clierror!(
            "Invalid --cities-url: {url} - {err}",
            url = args.flag_cities_url,
            err = err
        );
    }

    // we need to use tokio runtime as geosuggest uses async
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(geocode_main(args))?;

    Ok(())
}

// main async geocode function that does the actual work
async fn geocode_main(args: Args) -> CliResult<()> {
    let mut index_cmd = false;
    let mut now_cmd = false;
    let mut iplookup_cmd = false;
    let geocode_cmd = if args.cmd_suggest {
        GeocodeSubCmd::Suggest
    } else if args.cmd_reverse {
        GeocodeSubCmd::Reverse
    } else if args.cmd_countryinfo {
        GeocodeSubCmd::CountryInfo
    } else if args.cmd_suggestnow {
        now_cmd = true;
        GeocodeSubCmd::SuggestNow
    } else if args.cmd_reversenow {
        now_cmd = true;
        GeocodeSubCmd::ReverseNow
    } else if args.cmd_countryinfonow {
        now_cmd = true;
        GeocodeSubCmd::CountryInfoNow
    } else if args.cmd_iplookup {
        iplookup_cmd = true;
        GeocodeSubCmd::Iplookup
    } else if args.cmd_iplookupnow {
        now_cmd = true;
        iplookup_cmd = true;
        GeocodeSubCmd::IplookupNow
    } else if args.cmd_index_check {
        index_cmd = true;
        GeocodeSubCmd::IndexCheck
    } else if args.cmd_index_update {
        index_cmd = true;
        GeocodeSubCmd::IndexUpdate
    } else if args.cmd_index_load {
        index_cmd = true;
        GeocodeSubCmd::IndexLoad
    } else if args.cmd_index_reset {
        index_cmd = true;
        GeocodeSubCmd::IndexReset
    } else {
        // should not happen as docopt won't allow it
        unreachable!();
    };

    // setup cache directory
    let geocode_cache_dir = if let Ok(cache_dir) = std::env::var("QSV_CACHE_DIR") {
        // if QSV_CACHE_DIR env var is set, check if it exists. If it doesn't, create it.
        if cache_dir.starts_with('~') {
            // QSV_CACHE_DIR starts with ~, expand it
            // safety: we know it starts with ~, so it should be safe to unwrap
            expand_tilde(&cache_dir).unwrap()
        } else {
            PathBuf::from(cache_dir)
        }
    } else {
        // QSV_CACHE_DIR env var is not set, use args.flag_cache_dir
        // first check if it starts with ~, expand it
        if args.flag_cache_dir.starts_with('~') {
            // safety: we know it starts with ~, so it should be safe to unwrap
            expand_tilde(&args.flag_cache_dir).unwrap()
        } else {
            PathBuf::from(&args.flag_cache_dir)
        }
    };
    if !Path::new(&geocode_cache_dir).exists() {
        fs::create_dir_all(&geocode_cache_dir)?;
    }

    info!("Using cache directory: {}", geocode_cache_dir.display());

    let geocode_index_filename = std::env::var("QSV_GEOCODE_INDEX_FILENAME")
        .unwrap_or_else(|_| DEFAULT_GEOCODE_INDEX_FILENAME.to_string());
    let active_geocode_index_file =
        format!("{}/{}", geocode_cache_dir.display(), geocode_index_filename);
    let geocode_index_file = args
        .arg_index_file
        .clone()
        .unwrap_or_else(|| active_geocode_index_file.clone());
    let geoip2_filename = std::env::var("QSV_GEOIP2_FILENAME")
        .unwrap_or_else(|_| format!("{}/{}", geocode_cache_dir.display(), GEOIP2_FILENAME));

    // create a TempDir for the one record CSV we're creating if we're doing a Now command
    // we're doing this at this scope so the TempDir is automatically dropped after we're done
    let tempdir = tempfile::Builder::new().prefix("qsv-geocode").tempdir()?;

    // we're doing a SuggestNow, ReverseNow or CountryInfoNow - create a one record CSV in tempdir
    // with one column named "Location" and the passed location value and use it as the input
    let input = if now_cmd {
        let tempdir_path = tempdir.path().to_string_lossy().to_string();
        let temp_csv_path = format!("{}/{}.csv", tempdir_path, Uuid::new_v4());
        let temp_csv_path = Path::new(&temp_csv_path);
        let mut temp_csv_wtr = csv::WriterBuilder::new().from_path(temp_csv_path)?;
        temp_csv_wtr.write_record(["Location"])?;
        temp_csv_wtr.write_record([&args.arg_location])?;
        temp_csv_wtr.flush()?;
        Some(temp_csv_path.to_string_lossy().to_string())
    } else {
        args.arg_input
    };

    let rconfig = Config::new(input.as_ref())
        .delimiter(args.flag_delimiter)
        .select(SelectColumns::parse(&args.arg_column)?);

    #[cfg(feature = "datapusher_plus")]
    #[allow(unused_variables)]
    let show_progress = false;

    // prep progress bar
    #[cfg(any(feature = "feature_capable", feature = "lite"))]
    let show_progress =
        (args.flag_progressbar || util::get_envvar_flag("QSV_PROGRESSBAR")) && !rconfig.is_stdin();

    let progress = ProgressBar::with_draw_target(None, ProgressDrawTarget::stderr_with_hz(5));
    if show_progress {
        util::prep_progress(&progress, util::count_rows(&rconfig)?);
    } else {
        progress.set_draw_target(ProgressDrawTarget::hidden());
    }

    if index_cmd {
        // cities_filename is derived from the cities_url
        // the filename is the last component of the URL with a .txt extension
        // e.g. https://download.geonames.org/export/dump/cities15000.zip -> cities15000.txt
        let cities_filename = args
            .flag_cities_url
            .split('/')
            .next_back()
            .unwrap()
            .replace(".zip", ".txt");

        // setup languages
        let languages_vec: Vec<&str> = args.flag_languages.split(',').map(AsRef::as_ref).collect();

        info!("geocode_index_file: {geocode_index_file} Languages: {languages_vec:?}");

        let indexupdater_settings = IndexUpdaterSettings {
            http_timeout_ms:  util::timeout_secs(args.flag_timeout)? * 1000,
            cities:           SourceItem {
                url:      &args.flag_cities_url,
                filename: &cities_filename,
            },
            names:            Some(SourceItem {
                url:      DEFAULT_CITIES_NAMES_URL,
                filename: DEFAULT_CITIES_NAMES_FILENAME,
            }),
            countries_url:    Some(DEFAULT_COUNTRY_INFO_URL),
            admin1_codes_url: Some(DEFAULT_ADMIN1_CODES_URL),
            admin2_codes_url: Some(DEFAULT_ADMIN2_CODES_URL),
            filter_languages: languages_vec.clone(),
        };

        let updater = IndexUpdater::new(indexupdater_settings.clone())
            .map_err(|_| CliError::Other("Error initializing IndexUpdater".to_string()))?;

        let index_storage = storage::Storage::new();

        match geocode_cmd {
            // check if Geoname index needs to be updated from the Geonames website
            // also returns the index file metadata as JSON
            GeocodeSubCmd::IndexCheck => {
                winfo!("Checking main Geonames website for updates...");
                check_index_file(&geocode_index_file)?;

                let metadata = index_storage
                    .read_metadata(geocode_index_file)
                    .map_err(|e| format!("index-check error: {e}"))?;

                let index_metadata_json = match serde_json::to_string_pretty(&metadata) {
                    Ok(json) => json,
                    Err(e) => {
                        let json_error = json!({
                            "errors": [{
                                "title": "Cannot serialize index metadata to JSON",
                                "detail": e.to_string()
                            }]
                        });
                        format!("{json_error}")
                    },
                };

                let created_at =
                    util::format_systemtime(metadata.as_ref().unwrap().created_at, "%+");
                eprintln!("Created at: {created_at}");

                match metadata {
                    Some(m)
                        if updater.has_updates(&m).await.map_err(|_| {
                            CliError::Network("Geonames update check failed.".to_string())
                        })? =>
                    {
                        winfo!(
                            "Updates available at Geonames.org. Use `qsv geocode index-update` to \
                             update/rebuild the index.\nPlease use this judiciously as Geonames \
                             is a free service.\n"
                        );
                    },
                    Some(_) => {
                        winfo!("Geonames index up-to-date.\n");
                    },
                    None => return fail_incorrectusage_clierror!("Invalid Geonames index file."),
                }

                // print to stdout the index metadata as JSON
                // so users can redirect stdout to a JSON file if desired
                println!("{index_metadata_json}");
            },
            GeocodeSubCmd::IndexUpdate => {
                // update/rebuild Geonames index from Geonames website
                // will only update if there are changes unless --force is specified
                check_index_file(&geocode_index_file)?;

                if args.flag_force {
                    display_rebuild_instructions(
                        &args.flag_cities_url,
                        &cities_filename,
                        &args.flag_languages,
                        &geocode_index_file,
                    );
                } else {
                    winfo!("Checking main Geonames website for updates...");
                    let metadata = index_storage
                        .read_metadata(geocode_index_file.clone())
                        .map_err(|e| format!("index-update error: {e}"))?;

                    if updater.has_updates(&metadata.unwrap()).await.map_err(|_| {
                        CliError::Network("Geonames update check failed.".to_string())
                    })? {
                        winfo!("Updates available at Geonames.org.");
                        display_rebuild_instructions(
                            &args.flag_cities_url,
                            &cities_filename,
                            &args.flag_languages,
                            &geocode_index_file,
                        );
                    } else {
                        winfo!("Skipping update. Geonames index is up-to-date.");
                    }
                }
            },
            GeocodeSubCmd::IndexLoad => {
                // load alternate geocode index file
                if let Some(index_file) = args.arg_index_file {
                    winfo!("Validating alternate Geonames index: {index_file}...");
                    check_index_file(&index_file)?;

                    let engine_data =
                        load_engine_data(index_file.clone().into(), &progress).await?;
                    // we successfully loaded the alternate geocode index file, so its valid
                    // copy it to the default geocode index file

                    if engine_data.metadata.is_some() {
                        let _ =
                            index_storage.dump_to(active_geocode_index_file.clone(), &engine_data);
                        winfo!(
                            "Valid Geonames index file {index_file} successfully copied to \
                             {active_geocode_index_file}. It will be used from now on or until \
                             you reset/rebuild it.",
                        );
                    } else {
                        return fail_incorrectusage_clierror!(
                            "Alternate Geonames index file {index_file} is invalid.",
                        );
                    }
                } else {
                    return fail_incorrectusage_clierror!(
                        "No alternate Geonames index file specified."
                    );
                }
            },
            GeocodeSubCmd::IndexReset => {
                // reset geocode index by deleting the current local copy
                // and downloading the default geocode index for the current qsv version
                winfo!("Resetting Geonames index to default: {geocode_index_file}...");
                fs::remove_file(&geocode_index_file)?;
                load_engine_data(geocode_index_file.clone().into(), &progress).await?;
                winfo!("Default Geonames index file successfully reset to {QSV_VERSION} release.");
            },
            // index_cmd is true, so we should never get a non-index subcommand
            _ => unreachable!(),
        }
        return Ok(());
    }

    // we're not doing an index subcommand, so we're doing a suggest/now, reverse/now,
    // countryinfo/now or iplookup/now subcommand. Load the current local Geonames index
    let mut engine_data = load_engine_data(geocode_index_file.clone().into(), &progress).await?;
    if iplookup_cmd {
        // load the GeoIP2 database
        engine_data
            .load_geoip2(geoip2_filename.clone())
            .map_err(|e| {
                CliError::Other(format!(
                    r#"Error loading GeoIP2 database "{geoip2_filename}": {e}"#
                ))
            })?;
    }

    let engine = engine_data
        .as_engine()
        .map_err(|e| CliError::Other(format!("Error initializing Engine: {e}")))?;

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(args.flag_output.as_ref())
        .quote_style(
            // if we're doing a now subcommand with JSON output, we don't want the CSV writer
            // to close quote the output as it will produce invalid JSON
            if now_cmd && (args.flag_formatstr == "%json" || args.flag_formatstr == "%pretty-json")
            {
                csv::QuoteStyle::Never
            } else {
                csv::QuoteStyle::Necessary
            },
        )
        .writer()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;
    let column_index = *sel.iter().next().unwrap();

    let mut headers = rdr.headers()?.clone();

    if let Some(new_name) = args.flag_rename {
        let new_col_names = util::ColumnNameParser::new(&new_name).parse()?;
        if new_col_names.len() != sel.len() {
            return fail_incorrectusage_clierror!(
                "Number of new columns does not match input column selection."
            );
        }
        for (i, col_index) in sel.iter().enumerate() {
            headers = replace_column_value(&headers, *col_index, &new_col_names[i]);
        }
    }

    // setup output headers
    if let Some(new_column) = &args.flag_new_column {
        headers.push_field(new_column);
    }

    // if formatstr starts with "%dyncols:"", then we're using dynfmt to add columns.
    // To add columns, we enclose in curly braces a key:value pair for each column with
    // the key being the desired column name and the value being the CityRecord field
    // we want to add to the CSV
    // e.g. "%dyncols: {city_col:name}, {state_col:admin1}, {country_col:country}"
    // will add three columns to the CSV named city_col, state_col and country_col.

    // first, parse the formatstr to get the column names and values in parallel vectors
    let mut column_names = Vec::new();
    let mut column_values = Vec::new();
    // dyncols_len is the number of columns we're adding in dyncols mode
    // it also doubles as a flag to indicate if we're using dyncols mode
    // i.e. if dyncols_len > 0, we're using dyncols mode; 0 we're not
    let dyncols_len = if args.flag_formatstr.starts_with("%dyncols:") {
        for column in args.flag_formatstr[9..].split(',') {
            let column = column.trim();
            let column_key_value: Vec<&str> = column.split(':').collect();
            if column_key_value.len() == 2 {
                column_names.push(column_key_value[0].trim_matches('{'));
                column_values.push(column_key_value[1].trim_matches('}'));
            }
        }

        // now, validate the column values
        // the valid column values are in SORTED_VALID_DYNCOLS
        for column_value in &column_values {
            if SORTED_VALID_DYNCOLS.binary_search(column_value).is_err() {
                return fail_incorrectusage_clierror!(
                    "Invalid column value: {column_value}. Valid values are: \
                     {SORTED_VALID_DYNCOLS:?}"
                );
            }
        }

        // its valid, add the columns to the CSV headers
        for column in column_names {
            headers.push_field(column);
        }
        column_values.len() as u8
    } else {
        0_u8
    };

    // now, write the headers to the output CSV, unless its a now subcommand with JSON output
    if !(now_cmd && (args.flag_formatstr == "%json" || args.flag_formatstr == "%pretty-json")) {
        wtr.write_record(&headers)?;
    }

    // setup admin1 filter for Suggest/Now
    let mut admin1_code_prefix = String::new();
    let mut admin1_same_prefix = true;
    let mut flag_country = args.flag_country.clone();
    let admin1_filter_list = match geocode_cmd {
        GeocodeSubCmd::Suggest | GeocodeSubCmd::SuggestNow => {
            // admin1 filter: if all uppercase, search for admin1 code, else, search for admin1 name
            // see https://download.geonames.org/export/dump/admin1CodesASCII.txt for valid codes
            if let Some(admin1_list) = args.flag_admin1.clone() {
                // this regex matches admin1 codes (e.g. US.NY, JP.40, CN.23, HK.NYL, GG.6417214)
                let admin1_code_re = ADMIN1_CODE_REGEX();
                let admin1_list_work = Some(
                    admin1_list
                        .split(',')
                        .map(|s| {
                            let temp_s = s.trim();
                            let is_code_flag = admin1_code_re.is_match(temp_s);
                            Admin1Filter {
                                admin1_string: if is_code_flag {
                                    if admin1_same_prefix {
                                        // check if all admin1 codes have the same prefix
                                        if admin1_code_prefix.is_empty() {
                                            // first admin1 code, so set the prefix
                                            admin1_code_prefix = temp_s[0..3].to_string();
                                        } else if admin1_code_prefix != temp_s[0..3] {
                                            // admin1 codes have different prefixes, so we can't
                                            // infer the country from the admin1 code
                                            admin1_same_prefix = false;
                                        }
                                    }
                                    temp_s.to_string()
                                } else {
                                    // its an admin1 name, lowercase it
                                    // so we can do case-insensitive starts_with() comparisons
                                    temp_s.to_lowercase()
                                },
                                is_code:       is_code_flag,
                            }
                        })
                        .collect::<Vec<Admin1Filter>>(),
                );

                // if admin1 is set, country must also be set
                // however, if all admin1 codes have the same prefix, we can infer the country from
                // the admin1 codes. Otherwise, we can't infer the country from the
                // admin1 code, so we error out.
                if args.flag_admin1.is_some() && flag_country.is_none() {
                    if !admin1_code_prefix.is_empty() && admin1_same_prefix {
                        admin1_code_prefix.pop(); // remove the dot
                        flag_country = Some(admin1_code_prefix);
                    } else {
                        return fail_incorrectusage_clierror!(
                            "If --admin1 is set, --country must also be set unless admin1 codes \
                             are used with a common country prefix (e.g. US.CA,US.NY,US.OH, etc)."
                        );
                    }
                }
                admin1_list_work
            } else {
                None
            }
        },
        _ => {
            // reverse/now and countryinfo/now subcommands don't support admin1 filter
            if args.flag_admin1.is_some() {
                return fail_incorrectusage_clierror!(
                    "reverse/reversenow & countryinfo subcommands do not support the --admin1 \
                     filter option."
                );
            }
            None
        },
    }; // end setup admin1 filters

    // setup country filter - both suggest/now and reverse/now support country filters
    // countryinfo/now subcommands ignores the country filter
    let country_filter_list = flag_country.map(|country_list| {
        country_list
            .split(',')
            .map(|s| s.trim().to_ascii_uppercase())
            .collect::<Vec<String>>()
    });

    log::debug!("country_filter_list: {country_filter_list:?}");
    log::debug!("admin1_filter_list: {admin1_filter_list:?}");

    // amortize memory allocation by reusing record
    #[allow(unused_assignments)]
    let mut batch_record = csv::StringRecord::new();

    // reuse batch buffers
    let batchsize: usize = if args.flag_batch == 0 {
        std::cmp::max(1000, util::count_rows(&rconfig)? as usize)
    } else {
        args.flag_batch
    };
    let mut batch = Vec::with_capacity(batchsize);
    let mut batch_results = Vec::with_capacity(batchsize);

    util::njobs(args.flag_jobs);

    let invalid_result = args.flag_invalid_result.unwrap_or_default();

    let min_score = args.flag_min_score;
    let k_weight = args.flag_k_weight;

    // main loop to read CSV and construct batches for parallel processing.
    // each batch is processed via Rayon parallel iterator.
    // loop exits when batch is empty.
    'batch_loop: loop {
        for _ in 0..batchsize {
            match rdr.read_record(&mut batch_record) {
                Ok(has_data) => {
                    if has_data {
                        batch.push(std::mem::take(&mut batch_record));
                    } else {
                        // nothing else to add to batch
                        break;
                    }
                },
                Err(e) => {
                    return fail_clierror!("Error reading file: {e}");
                },
            }
        }

        if batch.is_empty() {
            // break out of infinite loop when at EOF
            break 'batch_loop;
        }

        // do actual apply command via Rayon parallel iterator
        batch
            .par_iter()
            .map(|record_item| {
                let mut record = record_item.clone();
                let mut cell = record[column_index].to_owned();
                if cell.is_empty() {
                    // cell to geocode is empty. If in dyncols mode, we need to add empty columns.
                    // Otherwise, we leave the row untouched.
                    if dyncols_len > 0 {
                        add_fields(&mut record, "", dyncols_len);
                    }
                } else if geocode_cmd == GeocodeSubCmd::CountryInfo
                    || geocode_cmd == GeocodeSubCmd::CountryInfoNow
                {
                    // we're doing a countryinfo or countryinfonow subcommand
                    cell = get_countryinfo(
                        &engine,
                        &cell.to_ascii_uppercase(),
                        &args.flag_language,
                        &args.flag_formatstr,
                    )
                    .unwrap_or(cell);
                } else if dyncols_len > 0 {
                    // we're in dyncols mode, so use search_index_NO_CACHE fn
                    // as we need to inject the column values into each row of the output csv
                    // so we can't use the cache
                    let search_results = search_index_no_cache(
                        &engine,
                        geocode_cmd,
                        &cell,
                        &args.flag_formatstr,
                        &args.flag_language,
                        min_score,
                        k_weight,
                        country_filter_list.as_ref(),
                        admin1_filter_list.as_ref(),
                        &column_values,
                        &mut record,
                    );

                    // if search_results.is_some but we don't get the DYNCOLS_POPULATED
                    // sentinel value or its None, then we have an invalid result
                    let invalid = if let Some(res) = search_results {
                        res != DYNCOLS_POPULATED
                    } else {
                        true
                    };
                    if invalid {
                        if invalid_result.is_empty() {
                            // --invalid-result is not set, so add empty columns
                            add_fields(&mut record, "", dyncols_len);
                        } else {
                            // --invalid-result is set
                            add_fields(&mut record, &invalid_result, dyncols_len);
                        }
                    }
                } else {
                    // not in dyncols mode so call the CACHED search_index fn
                    // as we want to take advantage of the cache
                    let search_result = search_index(
                        &engine,
                        geocode_cmd,
                        &cell,
                        &args.flag_formatstr,
                        &args.flag_language,
                        min_score,
                        k_weight,
                        country_filter_list.as_ref(),
                        admin1_filter_list.as_ref(),
                        &column_values,
                        &mut record,
                    );

                    if let Some(geocoded_result) = search_result {
                        // we have a valid geocode result, so use that
                        cell = geocoded_result;
                    } else {
                        // we have an invalid geocode result
                        if !invalid_result.is_empty() {
                            // --invalid-result is set, so use that instead
                            // otherwise, we leave cell untouched.
                            cell.clone_from(&invalid_result);
                        }
                    }
                }
                // }
                if args.flag_new_column.is_some() {
                    record.push_field(&cell);
                } else {
                    record = replace_column_value(&record, column_index, &cell);
                }

                record
            })
            .collect_into_vec(&mut batch_results);

        // rayon collect() guarantees original order, so we can just append results each batch
        for result_record in &batch_results {
            wtr.write_record(result_record)?;
        }

        if show_progress {
            progress.inc(batch.len() as u64);
        }

        batch.clear();
    } // end batch loop

    if show_progress {
        // the geocode result cache is NOT used in dyncols mode,
        // so update the cache info only when dyncols_len == 0
        if dyncols_len == 0 {
            util::update_cache_info!(progress, SEARCH_INDEX);
        }
        util::finish_progress(&progress);
    }
    Ok(wtr.flush()?)
}

/// Display instructions for rebuilding the Geonames index using the geosuggest crate directly
fn display_rebuild_instructions(
    cities_url: &str,
    cities_filename: &str,
    languages: &str,
    geocode_index_file: &str,
) {
    winfo!(
        r#"To rebuild the index, use the geosuggest crate directly:

git clone https://github.com/estin/geosuggest.git
cd geosuggest
cargo run -p geosuggest-utils --bin geosuggest-build-index --release --features=cli,tracing -- \
    from-urls \
    --cities-url {cities_url} \
    --cities-filename {cities_filename} \
    --languages {languages} \
    --output {geocode_index_file}"#,
        cities_url = cities_url,
        cities_filename = cities_filename,
        languages = languages,
        geocode_index_file = geocode_index_file,
    );
}

/// check if index_file exists and ends with a .rkyv extension
fn check_index_file(index_file: &str) -> CliResult<()> {
    // check if index_file is a u16 with the values 500, 1000, 5000 or 15000
    // if it is, return OK
    if let Ok(i) = index_file.parse::<u16>()
        && (i == 500 || i == 1000 || i == 5000 || i == 15000)
    {
        return Ok(());
    }

    if !std::path::Path::new(index_file)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("rkyv"))
    {
        return fail_incorrectusage_clierror!(
            "Alternate Geonames index file {index_file} does not have a .rkyv extension."
        );
    }
    // check if index_file exist
    if !Path::new(index_file).exists() {
        return fail_incorrectusage_clierror!(
            "Alternate Geonames index file {index_file} does not exist."
        );
    }

    winfo!("Valid: {index_file}");
    Ok(())
}

/// load_engine_data loads the Geonames index file into memory
/// if the index file does not exist, it will download the default index file
/// from the qsv GitHub repo. For convenience, if geocode_index_file is 500, 1000, 5000 or 15000,
/// it will download the desired index file from the qsv GitHub repo.
async fn load_engine_data(
    geocode_index_file: PathBuf,
    progressbar: &ProgressBar,
) -> CliResult<EngineData> {
    // default cities index file
    static DEFAULT_GEONAMES_CITIES_INDEX: u16 = 15000;

    let index_file = std::path::Path::new(&geocode_index_file);

    // check if geocode_index_file is a 500, 1000, 5000 or 15000 record index file
    // by looking at the filestem, and checking if its a number
    // if it is, for convenience, we download the desired index file from the qsv GitHub repo
    let geocode_index_file_stem = geocode_index_file
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let download_url = format!(
        "https://github.com/dathere/qsv/releases/download/{QSV_VERSION}/{DEFAULT_GEOCODE_INDEX_FILENAME}.cities"
    );

    if geocode_index_file_stem.parse::<u16>().is_ok() {
        // its a number, check if its a 500, 1000, 5000 or 15000 record index file
        if geocode_index_file_stem != "500"
            && geocode_index_file_stem != "1000"
            && geocode_index_file_stem != "5000"
            && geocode_index_file_stem != "15000"
        {
            // we only do the convenience download for 500, 1000, 5000 or 15000 record index files
            return fail_incorrectusage_clierror!(
                "Only 500, 1000, 5000 or 15000 record index files are supported."
            );
        }

        progressbar.println(format!(
            "Alternate Geonames index file is a 500, 1000, 5000 or 15000 record index file. \
             Downloading {geocode_index_file_stem} Geonames index for qsv {QSV_VERSION} release..."
        ));

        util::download_file(
            &format!("{download_url}{geocode_index_file_stem}.sz"),
            geocode_index_file.clone(),
            !progressbar.is_hidden(),
            None,
            Some(60),
            None,
        )
        .await?;
    } else if index_file.exists() {
        // load existing local index
        progressbar.println(format!(
            "Loading existing Geonames index from {}",
            index_file.display()
        ));
    } else {
        // initial load or index-reset, download index file from qsv releases
        progressbar.println(format!(
            "Downloading default Geonames index for qsv {QSV_VERSION} release..."
        ));

        util::download_file(
            &format!("{download_url}{DEFAULT_GEONAMES_CITIES_INDEX}"),
            geocode_index_file.clone(),
            !progressbar.is_hidden(),
            None,
            Some(60),
            None,
        )
        .await?;
    }

    // check if the geocode_index_file is snappy compressed
    // if it is, decompress it
    let geocode_index_file = if geocode_index_file.extension().unwrap() == "sz" {
        let decompressed_geocode_index_file = geocode_index_file.with_extension(".rkyv");
        progressbar.println(format!(
            "Decompressing {} to {}",
            geocode_index_file.display(),
            decompressed_geocode_index_file.display()
        ));
        let tmpdir = tempdir()?;
        let decompressed_tmpfile = util::decompress_snappy_file(&geocode_index_file, &tmpdir)?;
        fs::copy(decompressed_tmpfile, &decompressed_geocode_index_file)?;
        decompressed_geocode_index_file
    } else {
        geocode_index_file
    };

    let storage = storage::Storage::new();

    let engine = storage
        .load_from(geocode_index_file)
        .map_err(|e| format!("On load index file: {e}"))?;

    if let Some(metadata) = &engine.metadata {
        let now = std::time::SystemTime::now();
        let age = now.duration_since(metadata.created_at).unwrap();
        let created_at_formatted = util::format_systemtime(metadata.created_at, "%+");

        progressbar.println(format!(
            "Geonames index loaded. Created: {created_at_formatted}  Age: {}",
            indicatif::HumanDuration(age)
        ));
    }

    Ok(engine)
}

/// search_index is a cached function that returns a geocode result for a given cell value.
/// It is used by the suggest/suggestnow and reverse/reversenow subcommands.
/// It uses an LRU cache using the cell value/language as the key, storing the formatted geocoded
/// result in the cache. As such, we CANNOT use the cache when in dyncols mode as the cached result
/// is the formatted result, not the individual fields.
/// search_index_no_cache() is automatically derived from search_index() by the cached macro.
/// search_index_no_cache() is used in dyncols mode, and as the name implies, does not use a cache.
#[cached(
    ty = "SizedCache<String, String>",
    create = "{ SizedCache::try_with_size(CACHE_SIZE).unwrap_or_else(|_| \
              SizedCache::with_size(FALLBACK_CACHE_SIZE)) }",
    key = "String",
    convert = r#"{ cell.to_owned() }"#,
    option = true
)]
fn search_index(
    engine: &Engine,
    mode: GeocodeSubCmd,
    cell: &str,
    formatstr: &str,
    lang_lookup: &str,
    min_score: Option<f32>,
    k: Option<f32>,
    country_filter_list: Option<&Vec<String>>,
    admin1_filter_list: Option<&Vec<Admin1Filter>>,
    column_values: &[&str], //&Vec<&str>,
    record: &mut csv::StringRecord,
) -> Option<String> {
    if mode == GeocodeSubCmd::Suggest || mode == GeocodeSubCmd::SuggestNow {
        let search_result: Vec<&CitiesRecord>;
        let cityrecord = if admin1_filter_list.is_none() {
            // no admin1 filter, run a search for 1 result (top match)
            search_result = engine.suggest(cell, 1, min_score, country_filter_list.map(|v| &**v));
            let Some(cr) = search_result.into_iter().next() else {
                // no results, so return early with None
                return None;
            };
            cr
        } else {
            // we have an admin1 filter, run a search for top SUGGEST_ADMIN1_LIMIT results
            search_result = engine.suggest(
                cell,
                SUGGEST_ADMIN1_LIMIT,
                min_score,
                country_filter_list.map(|v| &**v),
            );

            // first, get the first result and store that in cityrecord
            let Some(cr) = search_result.clone().into_iter().next() else {
                // no results, so return early with None
                return None;
            };
            let first_result = cr;

            // then iterate through search results and find the first one that matches admin1
            // the search results are already sorted by score, so we just need to find the first
            if let Some(admin1_filter_list) = admin1_filter_list {
                // we have an admin1 filter, so we need to find the first admin1 result that matches
                let mut admin1_filter_map: HashMap<String, bool, RandomState> = HashMap::default();
                for admin1_filter in admin1_filter_list {
                    admin1_filter_map
                        .insert(admin1_filter.clone().admin1_string, admin1_filter.is_code);
                }
                let mut matched_record: Option<&CitiesRecord> = None;
                'outer: for cr in &search_result {
                    if let Some(admin_division) = cr.admin_division.as_ref() {
                        for (admin1_filter, is_code) in &admin1_filter_map {
                            if *is_code {
                                // admin1 is a code, so we search for admin1 code
                                if admin_division.code.starts_with(admin1_filter) {
                                    matched_record = Some(cr);
                                    break 'outer;
                                }
                            } else {
                                // admin1 is a name, so we search for admin1 name, case-insensitive
                                if admin_division
                                    .name
                                    .to_lowercase()
                                    .starts_with(admin1_filter)
                                {
                                    matched_record = Some(cr);
                                    break 'outer;
                                }
                            }
                        }
                    }
                }

                if let Some(cr) = matched_record {
                    cr
                } else {
                    // no admin1 match, so we return the first result
                    first_result
                }
            } else {
                // no admin1 filter, so we return the first result
                first_result
            }
        };

        let country = &cityrecord.country.as_ref().unwrap().code;

        let nameslang = get_cityrecord_name_in_lang(cityrecord, lang_lookup);

        if formatstr == "%+" {
            // default for suggest is location - e.g. "(lat, long)"
            if mode == GeocodeSubCmd::SuggestNow {
                // however, make SuggestNow default more verbose
                return Some(format!(
                    "{name}, {admin1name} {country}: {latitude}, {longitude}",
                    name = nameslang.cityname,
                    admin1name = nameslang.admin1name,
                    latitude = cityrecord.latitude,
                    longitude = cityrecord.longitude
                ));
            }
            return Some(format!(
                "({latitude}, {longitude})",
                latitude = cityrecord.latitude,
                longitude = cityrecord.longitude
            ));
        }

        let capital = engine
            .capital(country)
            .map(|cr| cr.name.as_str())
            .unwrap_or_default();

        if formatstr.starts_with("%dyncols:") {
            let countryrecord = engine.country_info(country)?;
            add_dyncols(
                record,
                cityrecord,
                countryrecord,
                &nameslang,
                country,
                capital,
                column_values,
            );
            return Some(DYNCOLS_POPULATED.to_string());
        }

        return Some(format_result(
            engine, cityrecord, &nameslang, country, capital, formatstr, true,
        ));
    } else if mode == GeocodeSubCmd::Iplookup || mode == GeocodeSubCmd::IplookupNow {
        // check if the cell is an IP address
        let ip_addr = if let Ok(ip_addr) = cell.to_string().parse::<IpAddr>() {
            ip_addr
        } else {
            // not an IP address, check if it's a URL and lookup the IP address
            let url = Url::parse(cell)
                .map_err(|_| CliError::Other("Invalid URL".to_string()))
                .ok()?;
            let host = url.host_str().unwrap_or_default().to_string();
            // try to resolve the host to an IP address using cached DNS lookup
            cached_dns_lookup(host)?
        };

        let search_result = engine.geoip2_lookup(ip_addr);
        let Some(cityrecord) = search_result else {
            // if no cityrecord is found, return the IP address
            return Some(format!("{ip_addr}"));
        };
        let nameslang = get_cityrecord_name_in_lang(cityrecord, lang_lookup);
        let country = &cityrecord.country.as_ref().unwrap().code;
        let capital = engine
            .capital(country)
            .map(|cr| cr.name.as_str())
            .unwrap_or_default();
        #[allow(clippy::literal_string_with_formatting_args)]
        let formatstr = if formatstr == "%+" {
            if mode == GeocodeSubCmd::IplookupNow {
                "{name}, {admin1} {country}: {latitude}, {longitude}"
            } else {
                "%+"
            }
        } else {
            formatstr
        };

        // %dyncols: handling for IP lookup
        if formatstr.starts_with("%dyncols:") {
            let countryrecord = engine.country_info(country)?;
            add_dyncols(
                record,
                cityrecord,
                countryrecord,
                &nameslang,
                country,
                capital,
                column_values,
            );
            return Some(DYNCOLS_POPULATED.to_string());
        }

        return Some(format_result(
            engine, cityrecord, &nameslang, country, capital, formatstr, false,
        ));
    }

    // we're doing a Reverse/Now command and expect a WGS 84 coordinate
    // the regex validates for "(lat, long)" or "lat, long"
    // note that it is not pinned to the start of the string, so it can be in the middle
    // of a string, e.g. "The location of the incident is 40.7128, -74.0060"
    let locregex = LOCATION_REGEX();

    let loccaps = locregex.captures(cell);
    if let Some(loccaps) = loccaps {
        let lat = loccaps[1].to_string().parse::<f32>().unwrap_or_default();
        let long = loccaps[2].to_string().parse::<f32>().unwrap_or_default();
        if (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&long) {
            let search_result =
                engine.reverse((lat, long), 1, k, country_filter_list.map(|v| &**v));
            let cityrecord = (match search_result {
                Some(search_result) => search_result.into_iter().next().map(|ri| ri.city),
                None => return None,
            })?;

            let nameslang = get_cityrecord_name_in_lang(cityrecord, lang_lookup);

            // safety: we know country is Some because we got a cityrecord
            let country = &cityrecord.country.as_ref().unwrap().code;

            if formatstr == "%+" {
                // default for reverse is city, admin1 country - e.g. "Brooklyn, New York US"
                return Some(format!(
                    "{cityname}, {admin1name} {country}",
                    cityname = nameslang.cityname,
                    admin1name = nameslang.admin1name,
                    country = country,
                ));
            }

            let capital = engine
                .capital(country)
                .map(|cr| cr.name.as_ref())
                .unwrap_or_default();

            if formatstr.starts_with("%dyncols:") {
                let countryrecord = engine.country_info(country)?;
                add_dyncols(
                    record,
                    cityrecord,
                    countryrecord,
                    &nameslang,
                    country,
                    capital,
                    column_values,
                );
                return Some(DYNCOLS_POPULATED.to_string());
            }

            return Some(format_result(
                engine, cityrecord, &nameslang, country, capital, formatstr, false,
            ));
        }
    }

    // not a valid lat, long
    None
}

#[cached(
    ty = "SizedCache<String, Option<IpAddr>>",
    create = "{ SizedCache::try_with_size(CACHE_SIZE).unwrap_or_else(|_| \
              SizedCache::with_size(FALLBACK_CACHE_SIZE)) }",
    key = "String",
    convert = r#"{ host.to_owned() }"#
)]
fn cached_dns_lookup(host: String) -> Option<IpAddr> {
    dns_lookup::lookup_host(&host)
        .map(|ips| {
            ips.into_iter()
                .next()
                .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        })
        .ok()
}

/// "%dyncols:" formatstr used. Adds dynamic columns to CSV.
fn add_dyncols(
    record: &mut csv::StringRecord,
    cityrecord: &CitiesRecord,
    countryrecord: &CountryRecord,
    nameslang: &NamesLang,
    country: &str,
    capital: &str,
    column_values: &[&str],
) {
    for column in column_values {
        match *column {
            // CityRecord fields
            "id" => record.push_field(&cityrecord.id.to_string()),
            "name" => record.push_field(&nameslang.cityname),
            "latitude" => record.push_field(&cityrecord.latitude.to_string()),
            "longitude" => record.push_field(&cityrecord.longitude.to_string()),
            "country" => record.push_field(country),
            "admin1" => record.push_field(&nameslang.admin1name),
            "admin2" => record.push_field(&nameslang.admin2name),
            "capital" => record.push_field(capital),
            "timezone" => record.push_field(&cityrecord.timezone),
            "population" => record.push_field(&cityrecord.population.to_string()),

            // US FIPS fields
            "us_state_fips_code" => {
                let us_state_code = if let Some(admin1) = cityrecord.admin_division.as_ref() {
                    // admin1 code is a US state code, the two-letter state code
                    // is the last two characters of the admin1 code
                    // e.g. US.NY -> NY
                    // if not a US state code, return empty string
                    admin1.code.strip_prefix("US.").unwrap_or_default()
                } else {
                    // no admin1 code
                    // set to empty string
                    ""
                };
                // lookup US state FIPS code
                record.push_field(lookup_us_state_fips_code(us_state_code).unwrap_or_default());
            },
            "us_county_fips_code" => {
                let us_county_fips_code = if let Some(admin2) = cityrecord.admin2_division.as_ref()
                {
                    if admin2.code.starts_with("US.") && admin2.code.len() == 9 {
                        // admin2 code is a US county code, the three-digit county code
                        // is the last three characters of the admin2 code
                        // start at index 7 to skip the US. prefix
                        // e.g. US.NY.061 -> 061
                        format!("{:0>3}", &admin2.code[7..])
                    } else {
                        // admin2 code is not a US county code
                        // set to empty string
                        String::new()
                    }
                } else {
                    // no admin2 code
                    // set to empty string
                    String::new()
                };
                record.push_field(&us_county_fips_code);
            },

            // CountryRecord fields
            "country_name" => record.push_field(&nameslang.countryname),
            "iso3" => record.push_field(&countryrecord.info.iso3),
            "fips" => record.push_field(&countryrecord.info.fips),
            "area" => record.push_field(&countryrecord.info.area),
            "country_population" => record.push_field(&countryrecord.info.population.to_string()),
            "continent" => record.push_field(&countryrecord.info.continent),
            "tld" => record.push_field(&countryrecord.info.tld),
            "currency_code" => record.push_field(&countryrecord.info.currency_code),
            "currency_name" => record.push_field(&countryrecord.info.currency_name),
            "phone" => record.push_field(&countryrecord.info.phone),
            "postal_code_format" => record.push_field(&countryrecord.info.postal_code_format),
            "postal_code_regex" => record.push_field(&countryrecord.info.postal_code_regex),
            "languages" => record.push_field(&countryrecord.info.languages),
            "country_geonameid" => record.push_field(&countryrecord.info.geonameid.to_string()),
            "neighbours" => record.push_field(&countryrecord.info.neighbours),
            "equivalent_fips_code" => record.push_field(&countryrecord.info.equivalent_fips_code),

            // this should not happen as column_values has been pre-validated for these values
            _ => unreachable!(),
        }
    }
}

/// format the geocoded result based on formatstr if its not %+
#[cached(
    key = "String",
    convert = r#"{ format!("{}-{}-{}", cityrecord.id, formatstr, suggest_mode) }"#
)]
fn format_result(
    engine: &Engine,
    cityrecord: &CitiesRecord,
    nameslang: &NamesLang,
    country: &str,
    capital: &str,
    formatstr: &str,
    suggest_mode: bool,
) -> String {
    if formatstr.starts_with('%') {
        // if formatstr starts with %, then we're using a predefined format
        match formatstr {
            "%city-state" | "%city-admin1" => {
                format!("{}, {}", nameslang.cityname, nameslang.admin1name)
            },
            "%location" => format!("({}, {})", cityrecord.latitude, cityrecord.longitude),
            "%city-state-country" | "%city-admin1-country" => {
                format!(
                    "{}, {} {}",
                    nameslang.cityname, nameslang.admin1name, country
                )
            },
            "%lat-long" => format!("{}, {}", cityrecord.latitude, cityrecord.longitude),
            "%city-country" => format!("{}, {}", nameslang.cityname, country),
            "%city" => nameslang.cityname.clone(),
            "%city-county-state" | "%city-admin2-admin1" => {
                format!(
                    "{}, {}, {}",
                    nameslang.cityname, nameslang.admin2name, nameslang.admin1name,
                )
            },
            "%state" | "%admin1" => nameslang.admin1name.clone(),
            "%county" | "%admin2" => nameslang.admin2name.clone(),
            "%country" => country.to_string(),
            "%country_name" => nameslang.countryname.clone(),
            "%id" => cityrecord.id.to_string(),
            "%capital" => capital.to_string(),
            "%population" => cityrecord.population.to_string(),
            "%timezone" => cityrecord.timezone.to_string(),
            "%cityrecord" => format!("{cityrecord:?}"),
            "%admin1record" => format!("{:?}", cityrecord.admin_division),
            "%admin2record" => format!("{:?}", cityrecord.admin2_division),
            "%json" => {
                // safety: it is safe to unwrap as we will always have a country record at this
                // stage as the calling search_index function returns early if we
                // don't have a country record
                let countryrecord = engine.country_info(country).unwrap();
                let cr_json =
                    serde_json::to_string(cityrecord).unwrap_or_else(|_| "null".to_string());
                let country_json =
                    serde_json::to_string(countryrecord).unwrap_or_else(|_| "null".to_string());
                let us_fips_codes_json = get_us_fips_codes(cityrecord, nameslang);
                format!(
                    "{{\"cityrecord\":{cr_json}, \"countryrecord\":{country_json} \
                     \"us_fips_codes\":{us_fips_codes_json}}}",
                )
            },
            "%pretty-json" => {
                // safety: see safety note above for "%json"
                let countryrecord = engine.country_info(country).unwrap();
                let cr_json =
                    serde_json::to_string_pretty(cityrecord).unwrap_or_else(|_| "null".to_string());
                let country_json = serde_json::to_string_pretty(countryrecord)
                    .unwrap_or_else(|_| "null".to_string());
                let us_fips_codes = get_us_fips_codes(cityrecord, nameslang);
                let us_fips_codes_json = serde_json::to_string_pretty(&us_fips_codes)
                    .unwrap_or_else(|_| "null".to_string());
                format!(
                    "{{\n  \"cityrecord\":{cr_json},\n  \"countryrecord\":{country_json}\n \
                     \"us_fips_codes\":{us_fips_codes_json}\n}}",
                )
            },
            _ => {
                // invalid formatstr, so we use the default for suggest/now or reverse/now
                if suggest_mode {
                    // default for suggest is location - e.g. "(lat, long)"
                    format!(
                        "({latitude}, {longitude})",
                        latitude = cityrecord.latitude,
                        longitude = cityrecord.longitude
                    )
                } else {
                    // default for reverse/now or iplookup is city-admin1-country - e.g. "Brooklyn,
                    // New York US"
                    format!(
                        "{city}, {admin1} {country}",
                        city = nameslang.cityname,
                        admin1 = nameslang.admin1name,
                    )
                }
            },
        }
    } else {
        // if formatstr does not start with %, then we're using dynfmt,
        // i.e. twenty-eight predefined fields below in curly braces are replaced with values
        // e.g. "City: {name}, State: {admin1}, Country: {country} - {continent}"
        // unlike the predefined formats, we don't have a default format for dynfmt
        // so we return INVALID_DYNFMT if dynfmt fails to format the string
        // also, we have access to the country info fields as well

        // check if we have a valid country record
        let Some(countryrecord) = engine.country_info(country) else {
            return INVALID_COUNTRY_CODE.to_string();
        };

        // Now, parse the formatstr to get the fields to initialize in
        // the hashmap lookup. We do this so we only populate the hashmap with fields
        // that are actually used in the formatstr.
        let mut dynfmt_fields = Vec::with_capacity(10); // 10 is a reasonable default to save allocs
        let formatstr_re = FORMATSTR_REGEX();
        for format_fields in formatstr_re.captures_iter(formatstr) {
            // safety: the regex will always have a "key" group per the regex above
            dynfmt_fields.push(format_fields.name("key").unwrap().as_str());
        }

        let mut cityrecord_map: HashMap<&str, String> = HashMap::with_capacity(dynfmt_fields.len());

        for field in &dynfmt_fields {
            match *field {
                // cityrecord fields
                "id" => cityrecord_map.insert("id", cityrecord.id.to_string()),
                "name" => cityrecord_map.insert("name", nameslang.cityname.clone()),
                "latitude" => cityrecord_map.insert("latitude", cityrecord.latitude.to_string()),
                "longitude" => cityrecord_map.insert("longitude", cityrecord.longitude.to_string()),
                "country" => cityrecord_map.insert("country", country.to_string()),
                "country_name" => {
                    cityrecord_map.insert("country_name", nameslang.countryname.clone())
                },
                "admin1" => cityrecord_map.insert("admin1", nameslang.admin1name.clone()),
                "admin2" => cityrecord_map.insert("admin2", nameslang.admin2name.clone()),
                "capital" => cityrecord_map.insert("capital", capital.to_string()),
                "timezone" => cityrecord_map.insert("timezone", cityrecord.timezone.to_string()),
                "population" => {
                    cityrecord_map.insert("population", cityrecord.population.to_string())
                },

                // US FIPS fields
                // set US state FIPS code
                "us_state_fips_code" => {
                    let us_state_code = if let Some(admin1) = cityrecord.admin_division.as_ref() {
                        admin1.code.strip_prefix("US.").unwrap_or_default()
                    } else {
                        ""
                    };
                    cityrecord_map.insert(
                        "us_state_fips_code",
                        lookup_us_state_fips_code(us_state_code)
                            .unwrap_or("")
                            .to_string(),
                    )
                },

                // set US county FIPS code
                "us_county_fips_code" => cityrecord_map.insert("us_county_fips_code", {
                    match cityrecord.admin2_division.as_ref() {
                        Some(admin2) => {
                            if admin2.code.starts_with("US.") && admin2.code.len() == 9 {
                                // admin2 code is a US county code, the three-digit county code
                                // is the last three characters of the admin2 code
                                // start at index 7 to skip the US. prefix
                                // e.g. US.NY.061 -> 061
                                format!("{:0>3}", &admin2.code[7..])
                            } else {
                                // admin2 code is not a US county code
                                // set to empty string
                                String::new()
                            }
                        },
                        None => {
                            // no admin2 code
                            // set to empty string
                            String::new()
                        },
                    }
                }),

                // countryrecord fields
                "iso3" => cityrecord_map.insert("iso3", countryrecord.info.iso3.to_string()),
                "fips" => cityrecord_map.insert("fips", countryrecord.info.fips.to_string()),
                "area" => cityrecord_map.insert("area", countryrecord.info.area.to_string()),
                "country_population" => cityrecord_map.insert(
                    "country_population",
                    countryrecord.info.population.to_string(),
                ),
                "continent" => {
                    cityrecord_map.insert("continent", countryrecord.info.continent.to_string())
                },
                "tld" => cityrecord_map.insert("tld", countryrecord.info.tld.to_string()),
                "currency_code" => cityrecord_map.insert(
                    "currency_code",
                    countryrecord.info.currency_code.to_string(),
                ),
                "currency_name" => cityrecord_map.insert(
                    "currency_name",
                    countryrecord.info.currency_name.to_string(),
                ),
                "phone" => cityrecord_map.insert("phone", countryrecord.info.phone.to_string()),
                "postal_code_format" => cityrecord_map.insert(
                    "postal_code_format",
                    countryrecord.info.postal_code_format.to_string(),
                ),
                "postal_code_regex" => cityrecord_map.insert(
                    "postal_code_regex",
                    countryrecord.info.postal_code_regex.to_string(),
                ),
                "languages" => {
                    cityrecord_map.insert("languages", countryrecord.info.languages.to_string())
                },
                "country_geonameid" => cityrecord_map.insert(
                    "country_geonameid",
                    countryrecord.info.geonameid.to_string(),
                ),
                "neighbours" => {
                    cityrecord_map.insert("neighbours", countryrecord.info.neighbours.to_string())
                },
                "equivalent_fips_code" => cityrecord_map.insert(
                    "equivalent_fips_code",
                    countryrecord.info.equivalent_fips_code.to_string(),
                ),
                _ => return INVALID_DYNFMT.to_string(),
            };
        }

        if let Ok(formatted) = dynfmt2::SimpleCurlyFormat.format(formatstr, cityrecord_map) {
            formatted.to_string()
        } else {
            INVALID_DYNFMT.to_string()
        }
    }
}

/// get_countryinfo is a cached function that returns a countryinfo result for a given cell value.
/// It is used by the countryinfo/countryinfonow subcommands.
#[cached(key = "String", convert = r#"{ format!("{cell}-{formatstr}") }"#)]
fn get_countryinfo(
    engine: &Engine,
    cell: &str,
    lang_lookup: &str,
    formatstr: &str,
) -> Option<String> {
    let Some(countryrecord) = engine.country_info(&cell.to_ascii_uppercase()) else {
        // no results, so return early with None
        return None;
    };

    if formatstr.starts_with('%') {
        // if formatstr starts with %, then we're using a predefined format
        let formatted = match formatstr {
            "%capital" => countryrecord.info.capital.to_string(),
            "%continent" => countryrecord.info.continent.to_string(),
            "%json" => serde_json::to_string(countryrecord).unwrap_or_else(|_| "null".to_string()),
            "%pretty-json" => {
                serde_json::to_string_pretty(countryrecord).unwrap_or_else(|_| "null".to_string())
            },
            _ => countryrecord
                .names
                .as_ref()
                .and_then(|n| n.get(lang_lookup))
                .map(ToString::to_string)
                .unwrap_or_default(),
        };
        Some(formatted)
    } else {
        // if formatstr does not start with %, then we're using dynfmt,
        // i.e. sixteen predefined fields below in curly braces are replaced with values
        // e.g. "Country: {country_name}, Continent: {continent} Currency: {currency_name}
        // ({currency_code})})"

        // first, parse the formatstr to get the fields to initialixe in the hashmap lookup
        // we do this so we only populate the hashmap with fields that are actually used
        // in the formatstr.
        let mut dynfmt_fields = Vec::with_capacity(10); // 10 is a reasonable default to save allocs
        let formatstr_re = FORMATSTR_REGEX();
        for format_fields in formatstr_re.captures_iter(formatstr) {
            // safety: the regex will always have a "key" group per the regex above
            dynfmt_fields.push(format_fields.name("key").unwrap().as_str());
        }

        let mut countryrecord_map: HashMap<&str, String> =
            HashMap::with_capacity(dynfmt_fields.len());

        for field in &dynfmt_fields {
            match *field {
                "country_name" => countryrecord_map.insert("country_name", {
                    countryrecord
                        .names
                        .as_ref()
                        .and_then(|n| n.get(lang_lookup))
                        .map(ToString::to_string)
                        .unwrap_or_default()
                }),
                "iso3" => countryrecord_map.insert("iso3", countryrecord.info.iso3.to_string()),
                "fips" => countryrecord_map.insert("fips", countryrecord.info.fips.to_string()),
                "capital" => {
                    countryrecord_map.insert("capital", countryrecord.info.capital.to_string())
                },
                "area" => countryrecord_map.insert("area", countryrecord.info.area.to_string()),
                "country_population" => countryrecord_map.insert(
                    "country_population",
                    countryrecord.info.population.to_string(),
                ),
                "continent" => {
                    countryrecord_map.insert("continent", countryrecord.info.continent.to_string())
                },
                "tld" => countryrecord_map.insert("tld", countryrecord.info.tld.to_string()),
                "currency_code" => countryrecord_map.insert(
                    "currency_code",
                    countryrecord.info.currency_code.to_string(),
                ),
                "currency_name" => countryrecord_map.insert(
                    "currency_name",
                    countryrecord.info.currency_name.to_string(),
                ),
                "phone" => countryrecord_map.insert("phone", countryrecord.info.phone.to_string()),
                "postal_code_format" => countryrecord_map.insert(
                    "postal_code_format",
                    countryrecord.info.postal_code_format.to_string(),
                ),
                "postal_code_regex" => countryrecord_map.insert(
                    "postal_code_regex",
                    countryrecord.info.postal_code_regex.to_string(),
                ),
                "languages" => {
                    countryrecord_map.insert("languages", countryrecord.info.languages.to_string())
                },
                "geonameid" => {
                    countryrecord_map.insert("geonameid", countryrecord.info.geonameid.to_string())
                },
                "neighbours" => countryrecord_map
                    .insert("neighbours", countryrecord.info.neighbours.to_string()),
                "equivalent_fips_code" => countryrecord_map.insert(
                    "equivalent_fips_code",
                    countryrecord.info.equivalent_fips_code.to_string(),
                ),
                _ => return Some(INVALID_DYNFMT.to_string()),
            };
        }

        if let Ok(formatted) = dynfmt2::SimpleCurlyFormat.format(formatstr, countryrecord_map) {
            Some(formatted.to_string())
        } else {
            Some(INVALID_DYNFMT.to_string())
        }
    }
}

/// get_cityrecord_name_in_lang is a cached function that returns a NamesLang struct
/// containing the city, admin1, admin2, and country names in the specified language.
/// Note that the index file needs to be built with the desired languages for this to work.
/// Use the "index-update" subcommand with the --languages option to rebuild the index
/// with the desired languages. Otherwise, all names will be in English (en)
#[cached(key = "String", convert = r#"{ format!("{}", cityrecord.id) }"#)]
fn get_cityrecord_name_in_lang(cityrecord: &CitiesRecord, lang_lookup: &str) -> NamesLang {
    let cityname = cityrecord
        .names
        .as_ref()
        .and_then(|n| n.get(lang_lookup))
        // Note that the city name is the default name if the language is not found.
        .unwrap_or(&cityrecord.name)
        .to_string();
    let admin1name = cityrecord
        .admin1_names
        .as_ref()
        .and_then(|n| n.get(lang_lookup))
        .map(ToString::to_string)
        .unwrap_or_default();
    let admin2name = cityrecord
        .admin2_names
        .as_ref()
        .and_then(|n| n.get(lang_lookup))
        .map(ToString::to_string)
        .unwrap_or_default();
    let countryname = cityrecord
        .country_names
        .as_ref()
        .and_then(|n| n.get(lang_lookup))
        .map(ToString::to_string)
        .unwrap_or_default();

    NamesLang {
        cityname,
        admin1name,
        admin2name,
        countryname,
    }
}

#[inline]
fn lookup_us_state_fips_code(state: &str) -> Option<&'static str> {
    US_STATES_FIPS_CODES.get(state).copied()
}

fn get_us_fips_codes(cityrecord: &CitiesRecord, nameslang: &NamesLang) -> serde_json::Value {
    let us_state_code = if let Some(admin1) = cityrecord.admin_division.as_ref() {
        admin1.code.strip_prefix("US.").unwrap_or_default()
    } else {
        ""
    };
    let us_state_fips_code = lookup_us_state_fips_code(us_state_code).unwrap_or("null");

    let us_county_code = match cityrecord.admin2_division.as_ref() {
        Some(admin2) => {
            if admin2.code.starts_with("US.") && admin2.code.len() == 9 {
                // admin2 code is a US county code, the three-digit county code
                // is the last three characters of the admin2 code
                // start at index 7 to skip the US. prefix
                // e.g. US.NY.061 -> 061
                format!("{:0>3}", &admin2.code[7..])
            } else {
                // admin2 code is not a US county code
                // set to empty string
                String::new()
            }
        },
        None => {
            // no admin2 code
            // set to empty string
            String::new()
        },
    };
    json!(
    {
        "us_state_code": us_state_code,
        "us_state_name": nameslang.admin1name,
        "us_state_fips_code": us_state_fips_code,
        "us_county": nameslang.admin2name,
        "us_county_fips_code": us_county_code,
    })
}

#[inline]
fn add_fields(record: &mut csv::StringRecord, value: &str, count: u8) {
    (0..count).for_each(|_| {
        record.push_field(value);
    });
}
