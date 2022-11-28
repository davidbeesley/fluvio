use std::{
    fs::File,
    io::{Read, Write}, path::PathBuf,
};
use clap::{arg, Parser};
use fluvio_cli_common::install::fluvio_base_dir;
use fluvio_future::task::run_block_on;
use pad::PadStr;
use fluvio::Compression;
use fluvio_benchmark::{
    benchmark_config::{benchmark_matrix::{
        BenchmarkMatrix, RecordKeyAllocationStrategy, get_config_from_file, get_default_config,
        SharedConfig, FluvioProducerConfig, FluvioConsumerConfig, FluvioTopicConfig,
        BenchmarkLoadConfig,
    }, Seconds, Millis},
    benchmark_driver::BenchmarkDriver,
    stats::AllStats,
    BenchmarkError,
};


fn main() {
    fluvio_future::subscriber::init_logger();
    let args = Args::parse();

    if args.example_config {
        print_example_config();
        return;
    }

    // TODO accept directory of files.
    let matrices = match Args::parse().config {
        Some(path) => get_config_from_file(&path),
        None => get_default_config(),
    };

    let all_stats = AllStats::default();
    let previous = load_previous_stats();

    for matrix in matrices {
        print_divider();
        println!(
            "# Beginning Matrix for: {}",
            matrix.shared_config.matrix_name
        );
        print_divider();

        for config in matrix.into_iter() {
            println!("{}", config);
            run_block_on(BenchmarkDriver::run_benchmark(
                config.clone(),
                all_stats.clone(),
            ))
            .unwrap();
            run_block_on(all_stats.print_results(&config));
            if let Some(other) = previous.as_ref() {
                run_block_on(all_stats.compare_stats(&config, other.clone()));
            }
            print_divider();
            println!()
        }
    }
    println!("Note: throughput is based on total produced bytes only");

    run_block_on(write_stats(all_stats)).unwrap();
}
fn benchmarking_dir() -> Result<PathBuf, BenchmarkError> {
    let dir_path = fluvio_base_dir()?.join("benchmarks");
    if !dir_path.exists() {
        std::fs::create_dir_all(&dir_path)?;
    }
    Ok(dir_path)
}

fn historic_run_path() -> Result<PathBuf, BenchmarkError> {
    let mut path = benchmarking_dir()?;
    path.push("previous");
    Ok(path)



}

fn load_previous_stats() -> Option<AllStats> {
    let mut file = File::open(historic_run_path().ok()?).ok()?;
    let mut buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut buffer).ok()?;
    AllStats::decode(&buffer).ok()
}

async fn write_stats(stats: AllStats) -> Result<(), BenchmarkError> {
    let encoded = stats.encode().await;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(historic_run_path()?)
        .map_err(|e| BenchmarkError::ErrorWithExplanation(format!("{:?}", e)))?;
    file.write(&encoded)
        .map_err(|e| BenchmarkError::ErrorWithExplanation(format!("{:?}", e)))?;
    Ok(())
}

fn print_example_config() {
    let example_config = BenchmarkMatrix {
        shared_config: SharedConfig {
            matrix_name: "ExampleMatrix".to_string(),
            num_samples: 100,
            millis_between_samples: Millis::new(500),
            worker_timeout_seconds: Seconds::new(3600),
        },
        producer_config: FluvioProducerConfig {
            batch_size: vec![16000],
            queue_size: vec![100],
            linger_millis: vec![Millis::new(10)],
            server_timeout_millis: vec![Millis::new(5000)],
            compression: vec![Compression::None],
        },
        consumer_config: FluvioConsumerConfig {
            max_bytes: vec![64000],
        },
        topic_config: FluvioTopicConfig {
            num_partitions: vec![1],
        },
        load_config: BenchmarkLoadConfig {
            num_records_per_producer_worker_per_batch: vec![1000],
            record_key_allocation_strategy: vec![RecordKeyAllocationStrategy::NoKey],
            num_concurrent_producer_workers: vec![1],
            num_concurrent_consumers_per_partition: vec![1],
            record_size: vec![1000],
        },
    };
    println!("{}", serde_yaml::to_string(&example_config).unwrap());
}

fn print_divider() {
    println!("{}", "".pad_to_width_with_char(50, '#'));
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to a config file to run. If not found, looks for config files in crates/fluvio-benchmark/benches/
    #[arg(short, long)]
    config: Option<String>,

    /// Print out an example config file and then exit
    #[arg(short, long)]
    example_config: bool,
}
