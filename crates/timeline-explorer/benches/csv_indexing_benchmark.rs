use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use tempfile::TempDir;
use timeline_explorer::{
    core::csv_loader::load_csv,
    index::{
        search::{fetch_hits_df, search_ids},
        writer::{index_csv, index_csv_streaming},
    },
};

struct BenchmarkData {
    csv_path: String,
    temp_dir: TempDir,
}

impl BenchmarkData {
    fn new() -> anyhow::Result<Self> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let csv_path = format!("{}/tests/data/mft.csv", manifest_dir);
        let temp_dir = tempfile::tempdir()?;

        Ok(Self { csv_path, temp_dir })
    }
}

fn bench_csv_loading(c: &mut Criterion) {
    let data = BenchmarkData::new().expect("Failed to setup benchmark data");

    c.bench_function("load_csv", |b| {
        b.iter(|| {
            let df = load_csv(black_box(&data.csv_path)).expect("Failed to load CSV file");
            black_box(df)
        })
    });
}

fn bench_index_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_creation");

    // Test different sample sizes for schema inference
    let sample_sizes = [10, 50, 100, 500];

    for &sample_size in &sample_sizes {
        let data = BenchmarkData::new().expect("Failed to setup benchmark data");

        group.bench_with_input(
            BenchmarkId::new("regular", sample_size),
            &sample_size,
            |b, &sample_size| {
                b.iter(|| {
                    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
                    let index = index_csv(
                        black_box(&data.csv_path),
                        black_box(&temp_dir.path().to_string_lossy().to_string()),
                        black_box(sample_size),
                        black_box(512),
                    )
                    .expect("Failed to create index");
                    black_box(index)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("streaming", sample_size),
            &sample_size,
            |b, &sample_size| {
                b.iter(|| {
                    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
                    let index = index_csv_streaming(
                        black_box(&data.csv_path),
                        black_box(&temp_dir.path().to_string_lossy().to_string()),
                        black_box(sample_size),
                        black_box(512),
                    )
                    .expect("Failed to create streaming index");
                    black_box(index)
                })
            },
        );
    }

    group.finish();
}

fn bench_search_operations(c: &mut Criterion) {
    // Setup: Create index once for all search benchmarks
    let data = BenchmarkData::new().expect("Failed to setup benchmark data");
    index_csv(
        &data.csv_path,
        &data.temp_dir.path().to_string_lossy().to_string(),
        10,
        512,
    )
    .expect("Failed to create index for search benchmark");

    let mut group = c.benchmark_group("search_operations");

    // Test different search terms and result limits
    let search_configs = [
        ("volume", 100),
        ("volume", 1000),
        ("volume", 10000),
        ("file", 100),
        ("directory", 100),
    ];

    for &(term, limit) in &search_configs {
        group.bench_with_input(
            BenchmarkId::new("search_ids", format!("{}_{}", term, limit)),
            &(term, limit),
            |b, &(term, limit)| {
                b.iter(|| {
                    let results = search_ids(
                        black_box(data.temp_dir.path()),
                        black_box(term),
                        black_box(limit),
                    )
                    .expect("Failed to search");
                    black_box(results)
                })
            },
        );
    }

    // Benchmark fetch_hits_df with pre-searched results
    let search_results = search_ids(data.temp_dir.path(), "volume", 1000)
        .expect("Failed to get search results for fetch benchmark");

    group.bench_function("fetch_hits_df", |b| {
        b.iter(|| {
            let df = fetch_hits_df(black_box(&data.csv_path), black_box(&search_results))
                .expect("Failed to fetch hits");
            black_box(df)
        })
    });

    group.finish();
}

fn bench_end_to_end_workflow(c: &mut Criterion) {
    c.bench_function("end_to_end_index_and_search", |b| {
        b.iter(|| {
            let data = BenchmarkData::new().expect("Failed to setup benchmark data");

            // Index the CSV
            let _index = index_csv(
                black_box(&data.csv_path),
                black_box(&data.temp_dir.path().to_string_lossy().to_string()),
                black_box(10),
                black_box(512),
            )
            .expect("Failed to create index");

            // Search
            let results = search_ids(
                black_box(data.temp_dir.path()),
                black_box("volume"),
                black_box(1000),
            )
            .expect("Failed to search");

            // Fetch results
            let df = fetch_hits_df(black_box(&data.csv_path), black_box(&results))
                .expect("Failed to fetch hits");

            black_box(df)
        })
    });
}

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_efficiency");

    // Test different buffer sizes for indexing
    let buffer_sizes = [256, 512, 1024, 2048];

    for &buffer_size in &buffer_sizes {
        let data = BenchmarkData::new().expect("Failed to setup benchmark data");

        group.bench_with_input(
            BenchmarkId::new("buffer_size", buffer_size),
            &buffer_size,
            |b, &buffer_size| {
                b.iter(|| {
                    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
                    let index = index_csv(
                        black_box(&data.csv_path),
                        black_box(&temp_dir.path().to_string_lossy().to_string()),
                        black_box(10),
                        black_box(buffer_size),
                    )
                    .expect("Failed to create index");
                    black_box(index)
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_csv_loading,
    bench_index_creation,
    bench_search_operations,
    bench_end_to_end_workflow,
    bench_memory_usage
);
criterion_main!(benches);
