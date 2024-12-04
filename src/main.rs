use postgres::types::Type;
use std::io::Write;
use chrono::{DateTime, Utc};
use csv::Reader;
use postgres::{Client, NoTls};
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::env;
use clap::{Parser, ValueEnum};
use postgres::binary_copy::BinaryCopyInWriter;
use prettytable::{Table, row};
use postgres_openssl::MakeTlsConnector;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};

#[derive(Debug, Clone)]
struct ConnectionInfo {
    name: String,
    connection_string: String,
}

impl ConnectionInfo {
    fn from_str(s: &str, ordinal: usize) -> Result<Self, Box<dyn Error>> {
        let connection_string = if s.starts_with("postgresql://") || s.starts_with("postgres://") {
            s.to_string()
        } else if let Some(rest) = s.strip_prefix("://") {
            format!("postgresql://{}", rest)
        } else if let Some((prefix, rest)) = s.split_once("://") {
            format!("postgresql://{}", rest)
        } else {
            format!("postgresql://{}", s)
        };

        let name = if let Some((prefix, _)) = s.split_once("://") {
            if prefix == "postgresql" || prefix == "postgres" {
                format!("postgres-{}", ordinal + 1)
            } else {
                prefix.to_string()
            }
        } else {
            format!("postgres-{}", ordinal + 1)
        };

        Ok(ConnectionInfo {
            name,
            connection_string,
        })
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(value_enum, value_delimiter = ',')]
    methods: Vec<IngestMethod>,

    #[arg(short, long, value_delimiter = ',', default_value = "1000")]
    batch_sizes: Vec<usize>,

    #[arg(short, long, default_value = "false")]
    transactions: bool,

    #[arg(short, long, default_value = "false")]
    csv_output: bool,

    #[arg(short = 'a', long, default_value = "false")]
    all: bool,

    #[arg(short = 'c', long, env = "CONNECTION_STRING", value_delimiter = ',')]
    connection_strings: Vec<String>,

    #[arg(short = 'f', long, default_value = "power_generation_1m.csv")]
    input_file: String,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum IngestMethod {
    InsertValues,
    PreparedInsertValues,
    InsertUnnest,
    PreparedInsertUnnest,
    Copy,
    BinaryCopy,
}

#[derive(Debug)]
struct BatterySensorData {
    id: i32,
    timestamp: DateTime<Utc>,
    voltage: f64,
    current: f64,
    temperature: f64,
    state_of_charge: f64,
    internal_resistance: f64,
}

#[derive(Debug)]
struct BenchmarkResult {
    connection_name: String,
    method: String,
    batch_size: usize,
    transaction: bool,
    duration: std::time::Duration,
    rows_per_sec: f64,
}

fn truncate_table(client: &mut Client) -> Result<(), Box<dyn Error>> {
    client.simple_query("
        CREATE TABLE IF NOT EXISTS power_generation (
            generator_id INTEGER,               -- Unique identifier for the generator or energy source
            timestamp TIMESTAMP WITH TIME ZONE, -- Timestamp of the reading
            power_output_kw DOUBLE PRECISION,   -- Real-time power output in kilowatts (kW)
            voltage DOUBLE PRECISION,           -- Voltage in volts (V)
            current DOUBLE PRECISION,           -- Current in amperes (A)
            frequency DOUBLE PRECISION,         -- Electrical frequency in hertz (Hz)
            temperature DOUBLE PRECISION        -- Equipment temperature in degrees Celsius (°C)
        );
    ")?;
    client.simple_query("TRUNCATE TABLE power_generation")?;
    client.simple_query("ALTER TABLE power_generation SET ( autovacuum_enabled = false);")?;
    client.simple_query("CHECKPOINT")?;
    Ok(())
}

fn create_benchmark_result(
    connection_name: &str,
    method: &str, 
    duration: std::time::Duration, 
    rows_per_sec: f64, 
    transactions: bool, 
    batch_size: usize
) -> BenchmarkResult {
    BenchmarkResult {
        connection_name: connection_name.to_string(),
        method: method.to_string(),
        batch_size,
        transaction: transactions,
        duration,
        rows_per_sec,
    }
}

fn print_results(results: &[BenchmarkResult], csv_output: bool, total_records: usize) {
    let max_speed = results.iter()
        .map(|r| r.rows_per_sec)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(1.0);

    if csv_output {
        println!("Connection,Method,Batch Size,Transaction,Duration,Rows/sec,Relative Speed");
        for result in results {
            println!("{},{},{},{},{:.2?},{:.0},x{:.2}",
                result.connection_name,
                result.method,
                result.batch_size,
                if result.transaction { "Yes" } else { "No" },
                result.duration,
                result.rows_per_sec,
                max_speed / result.rows_per_sec
            );
        }
    } else {
        println!();
        println!("\x1B[1m Results for import of {} records\x1B[0m", total_records);

        let mut table = Table::new();
        table.set_format(*prettytable::format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.add_row(row![
            b->"Connection",
            b->"Method",
            b->"Batch Size",
            b->"Transaction",
            b->"Duration",
            b->"Rows/sec",
            b->"Relative Speed"
        ]);
        
        for result in results {
            table.add_row(row![
                result.connection_name,
                result.method,
                format!("{}", result.batch_size),
                if result.transaction { "Yes" } else { "No" },
                format!("{:.2?}", result.duration),
                format!("{:.0}", result.rows_per_sec),
                format!("x{:.2}", max_speed / result.rows_per_sec)
            ]);
        }
        
        table.printstd();
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    
    // Parse connection strings
    let connections: Vec<ConnectionInfo> = if cli.connection_strings.is_empty() {
        // Default to environment variable if no connections specified
        vec![ConnectionInfo::from_str(
            &env::var("CONNECTION_STRING")
                .expect("CONNECTION_STRING must be provided via argument or environment variable"),
            1
        )?]
    } else {
        cli.connection_strings
            .iter()
            .enumerate()
            .map(|(i, s)| ConnectionInfo::from_str(s, i))
            .collect::<Result<Vec<_>, _>>()?
    };

    let records: Vec<BatterySensorData> = read_csv(&cli.input_file)?;
    let mut results: Vec<BenchmarkResult> = Vec::new();

    // For each connection
    for conn_info in &connections {
        eprintln!("Testing connection: {}", conn_info.name);
        
        let mut builder = SslConnector::builder(SslMethod::tls())?;
        builder.set_verify(SslVerifyMode::NONE);
        let connector = MakeTlsConnector::new(builder.build());

        let mut client = match Client::connect(&conn_info.connection_string, connector) {
            Ok(client) => {
                eprintln!("Successfully connected with SSL to {}", conn_info.name);
                client
            },
            Err(e) => {
                eprintln!("SSL connection failed for {}: {}", conn_info.name, e);
                eprintln!("Attempting fallback to non-SSL connection...");
                Client::connect(&conn_info.connection_string, NoTls)?
            }
        };

        let methods = if cli.all {
            vec![
                IngestMethod::InsertValues,
                IngestMethod::PreparedInsertValues,
                IngestMethod::InsertUnnest,
                IngestMethod::PreparedInsertUnnest,
                IngestMethod::Copy,
                IngestMethod::BinaryCopy,
            ]
        } else {
            cli.methods.clone()
        };

        // Run benchmarks for this connection
        for batch_size in &cli.batch_sizes {
            for method in &methods {
                let result = match method {
                    IngestMethod::InsertValues => insert_values(&mut client, records.as_slice(), cli.transactions, *batch_size, conn_info)?,
                    IngestMethod::PreparedInsertValues => prepared_insert_values(&mut client, records.as_slice(), cli.transactions, *batch_size, conn_info)?,
                    IngestMethod::InsertUnnest => insert_unnest(&mut client, records.as_slice(), cli.transactions, *batch_size, conn_info)?,
                    IngestMethod::PreparedInsertUnnest => prepared_insert_unnest(&mut client, records.as_slice(), cli.transactions, *batch_size, conn_info)?,
                    IngestMethod::Copy => copy(&mut client, records.as_slice(), cli.transactions, *batch_size, conn_info)?,
                    IngestMethod::BinaryCopy => binary_copy(&mut client, records.as_slice(), cli.transactions, *batch_size, conn_info)?,
                };
                results.push(result);
            }
        }
    }
    
    results.sort_by(|a, b| a.rows_per_sec.partial_cmp(&b.rows_per_sec).unwrap_or(std::cmp::Ordering::Equal));
    let valid_results: Vec<_> = results.into_iter().filter(|r| r.duration.as_nanos() > 0).collect();
    print_results(&valid_results, cli.csv_output, records.len());
    Ok(())
}

fn read_csv(path: &str) -> Result<Vec<BatterySensorData>, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut csv_reader = Reader::from_reader(reader);
    let mut records = Vec::new();

    for result in csv_reader.records() {
        let record = result?;
        records.push(BatterySensorData {
            id: record[0].parse()?,
            timestamp: DateTime::parse_from_rfc3339(&record[1]).expect("santa").into(),
            voltage: record[2].parse()?,
            current: record[3].parse()?,
            temperature: record[4].parse()?,
            state_of_charge: record[5].parse()?,
            internal_resistance: record[6].parse()?,
        });
    }

    Ok(records)
}

fn insert_unnest(client: &mut Client, records: &[BatterySensorData], transactions: bool, batch_size: usize, conn_info: &ConnectionInfo) -> Result<BenchmarkResult, Box<dyn Error>>
{
    truncate_table(client)?;
    
    let start = std::time::Instant::now();
    if transactions {
        client.simple_query("BEGIN")?;
    }

    let stmt = 
        "INSERT INTO power_generation 
         SELECT * FROM unnest($1::int4[], $2::timestamptz[], $3::float8[], $4::float8[], $5::float8[], $6::float8[], $7::float8[])";

    for chunk in records.chunks(batch_size) {
        let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut ids: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut voltages: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut currents: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut temperatures: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut socs: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut resistances: Vec<f64> = Vec::with_capacity(chunk.len());

        for record in chunk {
            ids.push(record.id);
            timestamps.push(record.timestamp);
            voltages.push(record.voltage);
            currents.push(record.current);
            temperatures.push(record.temperature);
            socs.push(record.state_of_charge);
            resistances.push(record.internal_resistance);
        }

        client.execute(stmt, &[&ids, &timestamps, &voltages, &currents, &temperatures, &socs, &resistances])?;
    }
    if transactions {
        client.simple_query("COMMIT")?;
    }
    let duration = start.elapsed();
    let rows_per_sec = records.len() as f64 / duration.as_secs_f64();
    Ok(create_benchmark_result(
        &conn_info.name, 
        "UNNEST insert",
        duration,
        rows_per_sec,
        transactions,
        batch_size
    ))
}

fn copy(client: &mut Client, records: &[BatterySensorData], transactions: bool, batch_size: usize, conn_info: &ConnectionInfo) -> Result<BenchmarkResult, Box<dyn Error>>
{
    truncate_table(client)?;
    
    let start = std::time::Instant::now();
    if transactions {
        client.simple_query("BEGIN")?;
    }
    
    for chunk in records.chunks(batch_size) {
        let mut writer = client.copy_in(
            "COPY power_generation FROM STDIN"
        )?;

        for record in chunk {
            write!(writer, "{}\t", record.id)?;
            write!(writer, "{}\t", record.timestamp)?;
            write!(writer, "{}\t", record.voltage)?;
            write!(writer, "{}\t", record.current)?;
            write!(writer, "{}\t", record.temperature)?;
            write!(writer, "{}\t", record.state_of_charge)?;
            writeln!(writer, "{}", record.internal_resistance)?;
        }
        writer.finish()?;
    }

    if transactions {
        client.simple_query("COMMIT")?;
    }
    let duration = start.elapsed();
    let rows_per_sec = records.len() as f64 / duration.as_secs_f64();
    Ok(create_benchmark_result(
        &conn_info.name,
        "Copy",
        duration,
        rows_per_sec,
        transactions,
        batch_size
    ))
}

fn binary_copy(client: &mut Client, records: &[BatterySensorData], transactions: bool, batch_size: usize, conn_info: &ConnectionInfo) -> Result<BenchmarkResult, Box<dyn Error>>
{
    truncate_table(client)?;
    
    let start = std::time::Instant::now();
    if transactions {
        client.simple_query("BEGIN")?;
    }
    
    let types = [
        Type::INT4, Type::TIMESTAMPTZ, Type::FLOAT8, Type::FLOAT8,
        Type::FLOAT8, Type::FLOAT8, Type::FLOAT8,
    ];


    for chunk in records.chunks(batch_size) {
    	let writer = client.copy_in(
       	 "COPY power_generation FROM STDIN WITH (FORMAT binary)"
    	)?;
    	let mut writer = BinaryCopyInWriter::new(writer, &types);
        for record in chunk {
           writer.write(&[&record.id, &record.timestamp, &record.voltage, &record.current, &record.temperature, &record.state_of_charge, &record.internal_resistance])?;
        }
        writer.finish()?;
    }

    if transactions {
        client.simple_query("COMMIT")?;
    }
    let duration = start.elapsed();
    let rows_per_sec = records.len() as f64 / duration.as_secs_f64();
    Ok(create_benchmark_result(
        &conn_info.name,
        "Binary Copy",
        duration,
        rows_per_sec,
        transactions,
        batch_size
    ))
}

fn insert_values(client: &mut Client, records: &[BatterySensorData], transactions: bool, batch_size: usize, conn_info: &ConnectionInfo) -> Result<BenchmarkResult, Box<dyn Error>>
{
    truncate_table(client)?;
    
    if batch_size > 4000 {
        eprintln!("Insert Values with batch size of {} failed, too many parameters", batch_size);
        return Ok(create_benchmark_result(
            &conn_info.name,
            "Insert VALUES",
            std::time::Duration::from_secs(0),
            0.0,
            transactions,
            batch_size
        ));
    }
    let start = std::time::Instant::now();
    if transactions {
        client.simple_query("BEGIN")?;
    }
    
    let mut value_strings = Vec::new();
    for i in 0..batch_size {
        let offset = i * 7;
        value_strings.push(format!("(${}, ${}, ${}, ${}, ${}, ${}, ${})", 
            offset + 1, offset + 2, offset + 3, offset + 4, offset + 5, offset + 6, offset + 7));
    }
    let query = format!(
        "INSERT INTO power_generation 
         VALUES {}", value_strings.join(", ")
    );

    for chunk in records.chunks(batch_size) {
        let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::with_capacity(chunk.len() * 7);
        for record in chunk {
            params.push(&record.id);
            params.push(&record.timestamp);
            params.push(&record.voltage);
            params.push(&record.current);
            params.push(&record.temperature);
            params.push(&record.state_of_charge);
            params.push(&record.internal_resistance);
        }
        client.execute(&query, &params[..])?;
    }

    if transactions {
        client.simple_query("COMMIT")?;
    }
    let duration = start.elapsed();
    let rows_per_sec = records.len() as f64 / duration.as_secs_f64();
    Ok(create_benchmark_result(
        &conn_info.name,
        "Insert VALUES",
        duration,
        rows_per_sec,
        transactions,
        batch_size
    ))
}

fn prepared_insert_values(client: &mut Client, records: &[BatterySensorData], transactions: bool, batch_size: usize, conn_info: &ConnectionInfo) -> Result<BenchmarkResult, Box<dyn Error>>
{
    truncate_table(client)?;
    
    if batch_size > 4000 {
        eprintln!("Prepared Insert Values with batch size of {} failed, too many parameters", batch_size);
        return Ok(create_benchmark_result(
            &conn_info.name,
            "Prepared Insert VALUES",
            std::time::Duration::from_secs(0),
            0.0,
            transactions,
            batch_size
        ));
    }
    let start = std::time::Instant::now();
    if transactions {
        client.simple_query("BEGIN")?;
    }
    
    let mut value_strings = Vec::new();
    for i in 0..batch_size {
        let offset = i * 7;
        value_strings.push(format!("(${}, ${}, ${}, ${}, ${}, ${}, ${})", 
            offset + 1, offset + 2, offset + 3, offset + 4, offset + 5, offset + 6, offset + 7));
    }
    let query = format!(
        "INSERT INTO power_generation 
         VALUES {}", value_strings.join(", ")
    );

    let stmt = client.prepare(&query)?;

    for chunk in records.chunks(batch_size) {
        let mut params: Vec<&(dyn postgres::types::ToSql + Sync)> = Vec::with_capacity(chunk.len() * 7);
        for record in chunk {
            params.push(&record.timestamp);
            params.push(&record.id);
            params.push(&record.voltage);
            params.push(&record.current);
            params.push(&record.temperature);
            params.push(&record.state_of_charge);
            params.push(&record.internal_resistance);
        }
        client.execute(&stmt, &params[..])?;
    }

    if transactions {
        client.simple_query("COMMIT")?;
    }
    let duration = start.elapsed();
    let rows_per_sec = records.len() as f64 / duration.as_secs_f64();
    Ok(create_benchmark_result(
        &conn_info.name,
        "Prepared Insert VALUES",
        duration,
        rows_per_sec,
        transactions,
        batch_size
    ))
}

fn prepared_insert_unnest(client: &mut Client, records: &[BatterySensorData], transactions: bool, batch_size: usize, conn_info: &ConnectionInfo) -> Result<BenchmarkResult, Box<dyn Error>>
{
    truncate_table(client)?;
    
    let start = std::time::Instant::now();
    if transactions {
        client.simple_query("BEGIN")?;
    }
    let stmt = client.prepare(
        "INSERT INTO power_generation 
         SELECT * FROM unnest($1::int4[], $2::timestamptz[], $3::float8[], $4::float8[], $5::float8[], $6::float8[], $7::float8[])"
    )?;


    for chunk in records.chunks(batch_size) {
        let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(chunk.len());
        let mut ids: Vec<i32> = Vec::with_capacity(chunk.len());
        let mut voltages: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut currents: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut temperatures: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut socs: Vec<f64> = Vec::with_capacity(chunk.len());
        let mut resistances: Vec<f64> = Vec::with_capacity(chunk.len());

        for record in chunk {
            ids.push(record.id);
            timestamps.push(record.timestamp);
            voltages.push(record.voltage);
            currents.push(record.current);
            temperatures.push(record.temperature);
            socs.push(record.state_of_charge);
            resistances.push(record.internal_resistance);
        }

        client.execute(&stmt, &[&ids, &timestamps, &voltages, &currents, &temperatures, &socs, &resistances])?;
    }

    if transactions {
        client.simple_query("COMMIT")?;
    }
    let duration = start.elapsed();
    let rows_per_sec = records.len() as f64 / duration.as_secs_f64();
    Ok(create_benchmark_result(
        &conn_info.name,
        "Prepared Insert UNNEST",
        duration,
        rows_per_sec,
        transactions,
        batch_size
    ))
}
