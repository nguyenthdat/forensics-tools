use rayon::prelude::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub operation_times: HashMap<String, Vec<Duration>>,
    pub memory_usage: HashMap<String, usize>,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub total_operations: u64,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            operation_times: HashMap::new(),
            memory_usage: HashMap::new(),
            cache_hits: 0,
            cache_misses: 0,
            total_operations: 0,
        }
    }
}

impl PerformanceMetrics {
    pub fn record_operation(&mut self, operation: &str, duration: Duration) {
        self.operation_times
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration);
        self.total_operations += 1;
    }

    pub fn record_memory_usage(&mut self, operation: &str, bytes: usize) {
        self.memory_usage.insert(operation.to_string(), bytes);
    }

    pub fn record_cache_hit(&mut self) {
        self.cache_hits += 1;
    }

    pub fn record_cache_miss(&mut self) {
        self.cache_misses += 1;
    }

    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total > 0 {
            self.cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }

    pub fn average_operation_time(&self, operation: &str) -> Option<Duration> {
        self.operation_times.get(operation).map(|times| {
            let sum: Duration = times.iter().sum();
            sum / times.len() as u32
        })
    }

    pub fn max_operation_time(&self, operation: &str) -> Option<Duration> {
        self.operation_times
            .get(operation)
            .and_then(|times| times.iter().max().copied())
    }

    pub fn min_operation_time(&self, operation: &str) -> Option<Duration> {
        self.operation_times
            .get(operation)
            .and_then(|times| times.iter().min().copied())
    }

    pub fn p95_operation_time(&self, operation: &str) -> Option<Duration> {
        self.operation_times.get(operation).map(|times| {
            let mut sorted = times.clone();
            sorted.sort();
            let index = (times.len() as f64 * 0.95) as usize;
            sorted.get(index).copied().unwrap_or(Duration::ZERO)
        })
    }

    pub fn report(&self) -> String {
        let mut report = String::new();

        report.push_str("=== Performance Report ===\n");
        report.push_str(&format!("Total Operations: {}\n", self.total_operations));
        report.push_str(&format!(
            "Cache Hit Rate: {:.2}%\n",
            self.cache_hit_rate() * 100.0
        ));

        report.push_str("\nOperation Times:\n");
        for (operation, _) in &self.operation_times {
            if let Some(avg) = self.average_operation_time(operation) {
                if let Some(max) = self.max_operation_time(operation) {
                    if let Some(p95) = self.p95_operation_time(operation) {
                        report.push_str(&format!(
                            "  {}: avg={:?}, max={:?}, p95={:?}\n",
                            operation, avg, max, p95
                        ));
                    }
                }
            }
        }

        report.push_str("\nMemory Usage:\n");
        for (operation, bytes) in &self.memory_usage {
            report.push_str(&format!("  {}: {} bytes\n", operation, bytes));
        }

        report
    }
}

pub struct PerformanceTimer {
    start: Instant,
    operation: String,
}

impl PerformanceTimer {
    pub fn new(operation: &str) -> Self {
        Self {
            start: Instant::now(),
            operation: operation.to_string(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn finish(self, metrics: &mut PerformanceMetrics) -> Duration {
        let duration = self.elapsed();
        metrics.record_operation(&self.operation, duration);
        duration
    }
}

/// Parallel processing utilities with performance tracking
pub struct ParallelProcessor {
    metrics: PerformanceMetrics,
}

impl Default for ParallelProcessor {
    fn default() -> Self {
        Self {
            metrics: PerformanceMetrics::default(),
        }
    }
}

impl ParallelProcessor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn process_in_parallel<T, R, F>(
        &mut self,
        items: Vec<T>,
        operation_name: &str,
        processor: F,
    ) -> Vec<R>
    where
        T: Send,
        R: Send,
        F: Fn(T) -> R + Send + Sync,
    {
        let timer = PerformanceTimer::new(operation_name);

        let results: Vec<R> = items.into_par_iter().map(processor).collect();

        timer.finish(&mut self.metrics);
        results
    }

    pub fn process_chunks_in_parallel<T, R, F>(
        &mut self,
        items: Vec<T>,
        chunk_size: usize,
        operation_name: &str,
        processor: F,
    ) -> Vec<R>
    where
        T: Send + Clone,
        R: Send,
        F: Fn(Vec<T>) -> Vec<R> + Send + Sync,
    {
        let timer = PerformanceTimer::new(operation_name);

        let chunks: Vec<Vec<T>> = items
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        let results: Vec<R> = chunks.into_par_iter().flat_map(processor).collect();

        timer.finish(&mut self.metrics);
        results
    }

    pub fn get_metrics(&self) -> &PerformanceMetrics {
        &self.metrics
    }

    pub fn reset_metrics(&mut self) {
        self.metrics = PerformanceMetrics::default();
    }
}

/// Memory usage tracking utilities
pub struct MemoryTracker {
    peak_usage: usize,
    current_usage: usize,
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self {
            peak_usage: 0,
            current_usage: 0,
        }
    }
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allocate(&mut self, bytes: usize) {
        self.current_usage += bytes;
        if self.current_usage > self.peak_usage {
            self.peak_usage = self.current_usage;
        }
    }

    pub fn deallocate(&mut self, bytes: usize) {
        self.current_usage = self.current_usage.saturating_sub(bytes);
    }

    pub fn current_usage(&self) -> usize {
        self.current_usage
    }

    pub fn peak_usage(&self) -> usize {
        self.peak_usage
    }

    pub fn reset(&mut self) {
        self.current_usage = 0;
        self.peak_usage = 0;
    }
}

/// Cache performance tracking
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: usize,
    pub max_size: usize,
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            size: 0,
            max_size: 1000, // Default cache size
        }
    }
}

impl CacheMetrics {
    pub fn record_hit(&mut self) {
        self.hits += 1;
    }

    pub fn record_miss(&mut self) {
        self.misses += 1;
    }

    pub fn record_eviction(&mut self) {
        self.evictions += 1;
        self.size = self.size.saturating_sub(1);
    }

    pub fn record_insertion(&mut self) {
        self.size += 1;
        if self.size > self.max_size {
            self.size = self.max_size; // Assume LRU eviction
        }
    }

    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        }
    }

    pub fn utilization(&self) -> f64 {
        self.size as f64 / self.max_size as f64
    }
}

/// Benchmark utilities for performance testing
pub mod benchmarks {
    use super::*;
    use std::fmt;

    pub struct BenchmarkResult {
        pub operation: String,
        pub iterations: usize,
        pub total_time: Duration,
        pub average_time: Duration,
        pub min_time: Duration,
        pub max_time: Duration,
        pub throughput: f64, // operations per second
    }

    impl fmt::Display for BenchmarkResult {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "{}: {} iterations, avg: {:?}, min: {:?}, max: {:?}, throughput: {:.2} ops/sec",
                self.operation,
                self.iterations,
                self.average_time,
                self.min_time,
                self.max_time,
                self.throughput
            )
        }
    }

    pub fn benchmark<F>(
        operation_name: &str,
        iterations: usize,
        mut operation: F,
    ) -> BenchmarkResult
    where
        F: FnMut() -> (),
    {
        let mut times = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let start = Instant::now();
            operation();
            times.push(start.elapsed());
        }

        let total_time: Duration = times.iter().sum();
        let average_time = total_time / iterations as u32;
        let min_time = *times.iter().min().unwrap();
        let max_time = *times.iter().max().unwrap();
        let throughput = iterations as f64 / total_time.as_secs_f64();

        BenchmarkResult {
            operation: operation_name.to_string(),
            iterations,
            total_time,
            average_time,
            min_time,
            max_time,
            throughput,
        }
    }

    pub fn benchmark_parallel<F>(
        operation_name: &str,
        iterations: usize,
        thread_counts: Vec<usize>,
        operation: F,
    ) -> Vec<(usize, BenchmarkResult)>
    where
        F: Fn() -> () + Send + Sync + Clone,
    {
        thread_counts
            .into_iter()
            .map(|thread_count| {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(thread_count)
                    .build()
                    .map(|pool| {
                        let result = pool.install(|| {
                            benchmark(
                                &format!("{}_{}threads", operation_name, thread_count),
                                iterations,
                                operation.clone(),
                            )
                        });
                        (thread_count, result)
                    })
                    .unwrap_or_else(|_| {
                        let result = benchmark(
                            &format!("{}_{}threads_fallback", operation_name, thread_count),
                            iterations,
                            operation.clone(),
                        );
                        (thread_count, result)
                    })
            })
            .collect()
    }
}

/// System resource monitoring
pub mod system {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct ResourceMonitor {
        memory_usage: Arc<AtomicUsize>,
        cpu_usage: Arc<AtomicUsize>,
    }

    impl Default for ResourceMonitor {
        fn default() -> Self {
            Self {
                memory_usage: Arc::new(AtomicUsize::new(0)),
                cpu_usage: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl ResourceMonitor {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn update_memory_usage(&self, bytes: usize) {
            self.memory_usage.store(bytes, Ordering::Relaxed);
        }

        pub fn update_cpu_usage(&self, percentage: usize) {
            self.cpu_usage.store(percentage, Ordering::Relaxed);
        }

        pub fn get_memory_usage(&self) -> usize {
            self.memory_usage.load(Ordering::Relaxed)
        }

        pub fn get_cpu_usage(&self) -> usize {
            self.cpu_usage.load(Ordering::Relaxed)
        }

        pub fn get_memory_usage_mb(&self) -> f64 {
            self.get_memory_usage() as f64 / 1024.0 / 1024.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_performance_metrics() {
        let mut metrics = PerformanceMetrics::default();

        metrics.record_operation("test_op", Duration::from_millis(100));
        metrics.record_operation("test_op", Duration::from_millis(200));

        assert_eq!(metrics.total_operations, 2);
        assert_eq!(
            metrics.average_operation_time("test_op"),
            Some(Duration::from_millis(150))
        );
    }

    #[test]
    fn test_performance_timer() {
        let mut metrics = PerformanceMetrics::default();
        let timer = PerformanceTimer::new("sleep_test");

        thread::sleep(Duration::from_millis(10));
        let duration = timer.finish(&mut metrics);

        assert!(duration >= Duration::from_millis(10));
        assert_eq!(metrics.total_operations, 1);
    }

    #[test]
    fn test_parallel_processor() {
        let mut processor = ParallelProcessor::new();
        let data = vec![1, 2, 3, 4, 5];

        let results = processor.process_in_parallel(data, "square_numbers", |x| x * x);

        assert_eq!(results, vec![1, 4, 9, 16, 25]);
        assert!(processor.get_metrics().total_operations > 0);
    }

    #[test]
    fn test_cache_metrics() {
        let mut cache = CacheMetrics::default();

        cache.record_hit();
        cache.record_miss();
        cache.record_hit();

        assert_eq!(cache.hit_rate(), 2.0 / 3.0);
    }

    #[test]
    fn test_benchmark() {
        let result = benchmarks::benchmark("test_operation", 10, || {
            thread::sleep(Duration::from_millis(1));
        });

        assert_eq!(result.iterations, 10);
        assert!(result.average_time >= Duration::from_millis(1));
        assert!(result.throughput > 0.0);
    }
}
