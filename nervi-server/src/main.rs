mod mcp;

use anyhow::Result;
use axum::{routing::post, Router};
use nervi_core::NerviClient;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub nervi: Arc<NerviClient>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "nervi_server=info".into()),
        )
        .init();

    let nats_url = std::env::var("NATS_URL")
        .unwrap_or_else(|_| "nats://nats.occitan-system.svc.cluster.local:4222".into());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    tracing::info!("connecting to NATS at {}", nats_url);
    let nervi = Arc::new(NerviClient::connect(&nats_url).await?);
    tracing::info!("NATS connected");

    let state = AppState { nervi };

    let app = Router::new()
        .route("/mcp", post(mcp::handle))
        .route("/health", axum::routing::get(|| async { "ok" }))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("nervi-server listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
