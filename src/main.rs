mod connection;
mod monitor;
mod parser;
mod phoenix;

use std::env;
use std::time::Duration;

use tracing::{error, info};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let secret = env::var("AS_TOKEN").expect("AS_TOKEN environment variable is required");
    let endpoint = env::var("AS_ENDPOINT").expect("AS_ENDPOINT environment variable is required");
    let initial_uuid = env::var("AS_UUID").ok();
    let accelerator_type = env::var("AS_ACCELERATOR_TYPE").unwrap_or_else(|_| "cpu".to_string());

    info!("agent-smith-lite starting — endpoint: {}", endpoint);

    loop {
        info!("Connecting to constellation...");
        match connection::run(
            &secret,
            &endpoint,
            initial_uuid.as_deref(),
            &accelerator_type,
        )
        .await
        {
            Ok(_) => {
                info!(
                    "Connection closed cleanly. Reconnecting in {}s...",
                    RECONNECT_DELAY.as_secs()
                );
            }
            Err(e) => {
                error!(
                    "Connection error: {}. Reconnecting in {}s...",
                    e,
                    RECONNECT_DELAY.as_secs()
                );
            }
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}
