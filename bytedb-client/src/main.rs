use clap::Parser;

mod repl;
mod connection;
mod protocol;
mod formatter;

use connection::ClientConnection;
use repl::Repl;

#[derive(Debug, Parser)]
#[command(name = "bytedb-client", about = "ByteDB CLI Client")]
struct Args {
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, default_value_t = 7654)]
    port: u16,

    #[arg(short, long, default_value = "admin")]
    user: String,

    #[arg(short = 'P', long, default_value = "admin")]
    password: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);

    println!("ByteDB Client v0.1.0");
    println!("Connecting to {}...", addr);

    match ClientConnection::connect(&addr, &args.user, &args.password).await {
        Ok(conn) => {
            println!("Connected! Type 'help' for commands, 'exit' to quit.");
            println!();
            let mut repl = Repl::new(conn);
            repl.run().await;
        }
        Err(e) => {
            eprintln!("Connection failed: {}", e);
            std::process::exit(1);
        }
    }
}
