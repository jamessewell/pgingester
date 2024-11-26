# Ingestion Benchmark Tool

This Rust program benchmarks different methods of batch ingesting data into a PostgreSQL database. It supports various ingestion techniques such as `INSERT`, `COPY`, `Binary COPY`, and `UNNEST` and measures their performance in terms of rows per second and ingestion duration. The program is configurable via command-line arguments, making it a flexible tool for evaluating database ingestion strategies.

The tool was orginally written for a blog, you can read it here.

---

## Features

- **Benchmark Multiple Ingestion Methods**: 
  - `InsertValues`
  - `PreparedInsertValues`
  - `InsertUnnest`
  - `PreparedInsertUnnest`
  - `Copy`
  - `BinaryCopy`

- **Batch Size Customization**: Define one or more batch sizes to test ingestion performance (comma seperated).

- **Transaction Control**: Enable or disable a single transaction for benchmarking.

- **Results Output**: Display results in either CSV format or a pretty table for better readability.

---

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Command-Line Options](#command-line-options)
- [CSV Format](#csv-format)
- [Example Usage](#example-usage)

---

## Installation

### Prerequisites
- **Rust**: Install Rust from [rustup.rs](https://rustup.rs/).
- **PostgreSQL**: Ensure you have a running PostgreSQL instance and a valid connection string.

### Clone, build, and unzip default dataset
```bash
git clone https://github.com/jamessewell/pgingester.git
cd pgingester
cargo build --release
unzip power_generation_1m.csv.zip
```

---

## Usage

### Run the Program
```bash
./target/release/pgingester --connection-string <YOUR_CONNECTION_STRING> [OPTIONS]
```

### Command-Line Options

| Option                  | Description                                                                                               |
|-------------------------|-----------------------------------------------------------------------------------------------------------|
| `--methods`             | Ingestion methods to benchmark (comma-separated). Use `all` to benchmark all methods.                    |
| `--all    `             | Shortcut to run all methods                                                                              |
| `--batch-sizes`         | Batch sizes to test (comma-separated). Default: `1000`.                                                  |
| `--transactions`        | Enable single transaction during ingestion. Default: `false`.                                            |
| `--csv-output`          | Output results in CSV format. Default: `false`.                                                          |
| `--input-file`          | Path to the input CSV file. Default: `battery_data.csv`.                                                 |
| `--connection-string`   | PostgreSQL connection string (can also be set via `CONNECTION_STRING` environment variable).              |

---

## CSV Format

The program reads sensor data from a CSV file with the following columns:

| Column Name          | Type      | Description                                             |
|----------------------|-----------|---------------------------------------------------------|
| `id`                | `integer` | Sensor ID.                                              |
| `timestamp`         | `RFC3339` | UTC timestamp of the reading.                          |
| `voltage`           | `float`   | Voltage reading in volts (V).                          |
| `current`           | `float`   | Current reading in amperes (A).                        |
| `temperature`       | `float`   | Temperature reading in degrees Celsius (°C).           |
| `state_of_charge`   | `float`   | Battery state of charge as a percentage.               |
| `internal_resistance` | `float` | Internal resistance of the battery in ohms (Ω).        |

### Example CSV:
```csv
1,2024-11-27T12:00:00Z,3.7,1.2,25.0,85.0,0.01
2,2024-11-27T12:01:00Z,3.8,1.3,26.0,84.5,0.012
...
```

---

## Example Usage

### Benchmark All Methods
```bash
./target/release/battery-data-benchmark     --methods all     --batch-sizes 500,1000,2000     --transactions true     --connection-string "postgresql://user:password@localhost:5432/mydb"
```

### Output Results in CSV
```bash
./target/release/battery-data-benchmark     --methods InsertValues,Copy     --batch-sizes 1000     --csv-output true     --connection-string "postgresql://user:password@localhost:5432/mydb"
```

---
Feel free to open an issue or submit a pull request for any feature requests or bug reports!
