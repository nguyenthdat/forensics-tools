use serde::Deserialize;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    CliResult,
    config::{Config, Delimiter},
    select::SelectColumns,
    util,
};

const NULL_VALUE: &str = "<NULL>";

#[derive(Deserialize)]
struct Args {
    arg_input:       Option<String>,
    flag_new_column: Option<String>,
    flag_start:      u64,
    flag_increment:  Option<u64>,
    flag_constant:   Option<String>,
    flag_copy:       Option<SelectColumns>,
    flag_uuid4:      bool,
    flag_uuid7:      bool,
    flag_hash:       Option<SelectColumns>,
    flag_output:     Option<String>,
    flag_no_headers: bool,
    flag_delimiter:  Option<Delimiter>,
}

#[derive(PartialEq)]
enum EnumOperation {
    Increment,
    Uuid4,
    Uuid7,
    Constant,
    Copy,
    Hash,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let mut rconfig = Config::new(args.arg_input.as_ref())
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(args.flag_output.as_ref()).writer()?;

    let mut headers = rdr.byte_headers()?.clone();
    let mut hash_index = None;

    let mut copy_index = 0;
    let mut copy_operation = false;

    if let Some(column_name) = args.flag_copy {
        rconfig = rconfig.select(column_name);
        let sel = rconfig.selection(&headers)?;
        copy_index = *sel.iter().next().unwrap();
        copy_operation = true;
    }

    let mut hash_sel = None;

    if let Some(hash_columns) = &args.flag_hash {
        // get the index of the column named "hash", if it exists
        hash_index = headers.iter().position(|col| col == b"hash");

        // get the original selection
        rconfig = rconfig.select(hash_columns.clone());
        let original_selection = rconfig
            .clone()
            .select(hash_columns.clone())
            .selection(&headers)?;

        // Filter out the "hash" column from the original selection, if it exists
        let filtered_selection = original_selection
            .iter()
            .filter(|&&index| index != hash_index.unwrap_or(usize::MAX))
            .collect::<Vec<_>>();

        // Construct selection string without "hash" column
        let selection_string = filtered_selection
            .iter()
            .map(|&&index| (index + 1).to_string())
            .collect::<Vec<String>>()
            .join(",");

        // Parse the new selection without "hash" column
        let no_hash_column_selection = SelectColumns::parse(&selection_string)?;

        // Update the configuration with the new selection
        rconfig = rconfig.select(no_hash_column_selection);
        hash_sel = Some(rconfig.selection(&headers)?);
    }

    let constant_value = if args.flag_constant == Some(NULL_VALUE.to_string()) {
        b""
    } else {
        args.flag_constant.as_deref().unwrap_or("").as_bytes()
    };

    let enum_operation = if args.flag_constant.is_some() {
        EnumOperation::Constant
    } else if args.flag_uuid4 {
        EnumOperation::Uuid4
    } else if args.flag_uuid7 {
        EnumOperation::Uuid7
    } else if copy_operation {
        EnumOperation::Copy
    } else if args.flag_hash.is_some() {
        EnumOperation::Hash
    } else {
        EnumOperation::Increment
    };

    if !rconfig.no_headers {
        if enum_operation == EnumOperation::Hash {
            // Remove an existing "hash" column from the header, if it exists
            headers = if let Some(hash_index) = hash_index {
                headers
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, field)| if i == hash_index { None } else { Some(field) })
                    .collect()
            } else {
                headers
            };
        }
        let column_name = match args.flag_new_column {
            Some(new_column_name) => new_column_name,
            _ => match enum_operation {
                EnumOperation::Increment => "index".to_string(),
                EnumOperation::Uuid4 => "uuid4".to_string(),
                EnumOperation::Uuid7 => "uuid7".to_string(),
                EnumOperation::Constant => "constant".to_string(),
                EnumOperation::Copy => {
                    let current_header = match simdutf8::compat::from_utf8(&headers[copy_index]) {
                        Ok(s) => s,
                        Err(e) => return fail_clierror!("Could not parse header as utf-8!: {e}"),
                    };
                    format!("{current_header}_copy")
                },
                EnumOperation::Hash => "hash".to_string(),
            },
        };
        headers.push_field(column_name.as_bytes());
        wtr.write_byte_record(&headers)?;
    }

    // amortize allocations
    let mut record = csv::ByteRecord::new();
    let mut counter: u64 = args.flag_start;
    #[allow(unused_assignments)]
    let mut colcopy: Vec<u8> = Vec::with_capacity(20);
    let increment = args.flag_increment.unwrap_or(1);
    let mut hash_string = String::new();
    let mut hash;
    let uuid7_ctxt = uuid::ContextV7::new();
    let mut uuid;

    while rdr.read_byte_record(&mut record)? {
        match enum_operation {
            EnumOperation::Increment => {
                record.push_field(itoa::Buffer::new().format(counter).as_bytes());
                counter += increment;
            },
            EnumOperation::Uuid4 => {
                uuid = Uuid::new_v4();
                record.push_field(
                    uuid.as_hyphenated()
                        .encode_lower(&mut Uuid::encode_buffer())
                        .as_bytes(),
                );
            },
            EnumOperation::Uuid7 => {
                uuid = Uuid::new_v7(uuid::Timestamp::now(&uuid7_ctxt));
                record.push_field(
                    uuid.as_hyphenated()
                        .encode_lower(&mut Uuid::encode_buffer())
                        .as_bytes(),
                );
            },
            EnumOperation::Constant => {
                record.push_field(constant_value);
            },
            EnumOperation::Copy => {
                colcopy = record[copy_index].to_vec();
                record.push_field(&colcopy);
            },
            EnumOperation::Hash => {
                hash_string.clear();

                // build the hash string from the filtered selection
                if let Some(ref sel) = hash_sel {
                    sel.iter().for_each(|i| {
                        hash_string
                            .push_str(simdutf8::basic::from_utf8(&record[*i]).unwrap_or_default());
                    });
                }
                hash = xxh3_64(hash_string.as_bytes());

                // Optionally remove the "hash" column if it already exists from the output
                record = if let Some(hash_index) = hash_index {
                    record
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, field)| if i == hash_index { None } else { Some(field) })
                        .collect()
                } else {
                    record
                };
                record.push_field(hash.to_string().as_bytes());
            },
        }

        wtr.write_byte_record(&record)?;
    }
    Ok(wtr.flush()?)
}
