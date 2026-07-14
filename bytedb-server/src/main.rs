use clap::Parser;
use tracing_subscriber;

mod server;
mod connection;
mod protocol;
mod auth;
mod config;
mod error;
mod metrics;
#[cfg(feature = "tls")]
mod tls;

use config::Config;
use server::Server;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = Config::parse();

    println!(r#"
 ____        _       ____  ____
| __ ) _   _| |_ ___|  _ \| __ )
|  _ \| | | | __/ _ \ | | |  _ \
| |_) | |_| | ||  __/ |_| | |_) |
|____/ \__, |\__\___|____/|____/
       |___/
    "#);
    println!("ByteDB v{} - Universal Database Engine", env!("CARGO_PKG_VERSION"));
    println!("Listening on {}:{}", config.host, config.port);
    println!();

    let server = Server::new(config);
    if let Err(e) = server.run().await {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
