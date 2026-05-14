// src/main.rs
// Control Plane – Step 1: Submission Receiver API
// Bootstraps the actix-web server and wires the /api/v1/submit route.

mod errors;
mod handlers;
mod builder;
mod orchestrator;

use actix_web::{web, App, HttpServer, middleware};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialise structured logging (respects RUST_LOG env var).
    env_logger::init();

    let bind_addr = "127.0.0.1";
    let bind_port = 8080u16;

    log::info!(
        "🚀 Submission Receiver starting on http://{}:{}",
        bind_addr,
        bind_port
    );

    HttpServer::new(|| {
        App::new()
            // Request logger middleware for observability.
            .wrap(middleware::Logger::default())
            // ── Routes ──────────────────────────────────────────────
            .service(
                web::resource("/api/v1/submit")
                    .route(web::post().to(handlers::submit_handler)),
            )
    })
    .bind((bind_addr, bind_port))?
    .run()
    .await
}
