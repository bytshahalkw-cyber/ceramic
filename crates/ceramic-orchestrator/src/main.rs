use anyhow::{Context, Result};
use ceramic_core::{types::Request, capabilities::CapabilitySet};
use ceramic_runtime::{WasmPool, ExecutionConfig};
use clap::Parser;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::signal;
use tracing::{info, warn, error};

#[derive(Parser, Debug)]
#[command(name = "ceramic-orchestrator")]
#[command(about = "Hybrid Ceramic Architecture Orchestrator")]
struct Args {
    #[arg(long, default_value = "nats://localhost:4222")]
    nats_url: String,

    #[arg(long)]
    node_id: String,

    #[arg(long, default_value = "./modules")]
    modules_dir: String,

    #[arg(long, default_value = "1000")]
    max_instances: usize,

    #[arg(long, default_value = "50")]
    p99_threshold_ms: u64,
}

#[derive(Debug)]
struct Orchestrator {
    config: Args,
    wasm_pool: Arc<WasmPool>,
    nats_client: async_nats::Client,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Default)]
struct Metrics {
    requests_total: AtomicU64,
    requests_failed: AtomicU64,
    latency_samples: std::sync::Mutex<Vec<u64>>,
}

impl Orchestrator {
    async fn new(config: Args) -> Result<Self> {
        info!("Initializing Ceramic Orchestrator on node {}", config.node_id);

        let nats_client = async_nats::connect(&config.nats_url)
            .await
            .context("Failed to connect to NATS")?;
        info!("Connected to NATS at {}", config.nats_url);

        let wasm_pool = WasmPool::new(ExecutionConfig {
            max_instances: config.max_instances,
            modules_dir: config.modules_dir.clone(),
            enable_checkpointing: true,
        })
        .await
        .context("Failed to initialize Wasm pool")?;
        info!("Wasm pool initialized with {} max instances", config.max_instances);

        Ok(Self {
            config,
            wasm_pool: Arc::new(wasm_pool),
            nats_client,
            metrics: Arc::new(Metrics::default()),
        })
    }

    async fn run(self) -> Result<()> {
        let self_arc = Arc::new(self);

        let handler = {
            let orchestrator = Arc::clone(&self_arc);
            tokio::spawn(async move {
                if let Err(e) = orchestrator.handle_requests().await {
                    error!("Request handler failed: {:?}", e);
                }
            })
        };

        let health_monitor = {
            let orchestrator = Arc::clone(&self_arc);
            tokio::spawn(async move {
                orchestrator.monitor_health().await;
            })
        };

        signal::ctrl_c().await?;
        info!("Shutdown signal received, initiating graceful drain...");

        self_arc.drain().await?;
        let _ = tokio::join!(handler, health_monitor);

        info!("Orchestrator shutdown complete");
        Ok(())
    }

    async fn handle_requests(self: &Arc<Self>) -> Result<()> {
        let mut subscriber = self
            .nats_client
            .subscribe("ceramic.requests.>")
            .await
            .context("Failed to subscribe to requests topic")?;

        info!("Listening for requests on ceramic.requests.>");

        while let Some(msg) = subscriber.next().await {
            let orchestrator = Arc::clone(self);
            tokio::spawn(async move {
                if let Err(e) = orchestrator.process_request(msg).await {
                    error!("Failed to process request: {:?}", e);
                    orchestrator.metrics.requests_failed.fetch_add(1, Ordering::Relaxed);
                }
            });
        }

        Ok(())
    }

    async fn process_request(&self, msg: async_nats::Message) -> Result<()> {
        let start = std::time::Instant::now();

        let request: Request = serde_json::from_slice(&msg.payload)
            .context("Failed to parse request")?;

        if self.should_throttle() {
            warn!("Throttling request due to high P99 latency");
            if let Some(reply) = msg.reply {
                let response = serde_json::json!({
                    "error": "Service temporarily unavailable",
                    "retry_after_ms": 1000
                });
                self.nats_client.publish(reply, response.to_string().into()).await?;
            }
            return Ok(());
        }

        let capabilities = CapabilitySet::from_request(&request);
        let result = self.wasm_pool.execute(request, capabilities).await?;

        let latency_ms = start.elapsed().as_millis() as u64;
        self.record_latency(latency_ms);

        if let Some(reply) = msg.reply {
            let response_json = serde_json::to_string(&result)?;
            self.nats_client.publish(reply, response_json.into()).await?;
        }

        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn should_throttle(&self) -> bool {
        let samples = self.metrics.latency_samples.lock().unwrap();
        if samples.len() < 100 { return false; }
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let p99_index = (sorted.len() as f64 * 0.99) as usize;
        let p99 = sorted.get(p99_index).copied().unwrap_or(0);
        p99 > self.config.p99_threshold_ms
    }

    fn record_latency(&self, latency_ms: u64) {
        let mut samples = self.metrics.latency_samples.lock().unwrap();
        samples.push(latency_ms);
        if samples.len() > 1000 { samples.remove(0); }
    }

    async fn monitor_health(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let total = self.metrics.requests_total.load(Ordering::Relaxed);
            let failed = self.metrics.requests_failed.load(Ordering::Relaxed);
            let success_rate = if total > 0 { ((total - failed) as f64 / total as f64) * 100.0 } else { 100.0 };
            let samples = self.metrics.latency_samples.lock().unwrap();
            let p50 = self.calculate_percentile(&samples, 0.50);
            let p99 = self.calculate_percentile(&samples, 0.99);
            info!("Health: total={}, failed={}, success_rate={:.1}%, p50={}ms, p99={}ms", total, failed, success_rate, p50, p99);
        }
    }

    fn calculate_percentile(&self, samples: &[u64], percentile: f64) -> u64 {
        if samples.is_empty() { return 0; }
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let index = (sorted.len() as f64 * percentile) as usize;
        sorted.get(index).copied().unwrap_or(0)
    }

    async fn drain(&self) -> Result<()> {
        info!("Starting graceful drain...");
        let drain_timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();
        while start.elapsed() < drain_timeout {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        info!("Drain complete");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("ceramic=info".parse()?))
        .init();
    let config = Args::parse();
    let orchestrator = Orchestrator::new(config).await?;
    orchestrator.run().await?;
    Ok(())
}
