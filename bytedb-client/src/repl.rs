use std::time::Instant;
use rustyline::DefaultEditor;

use crate::connection::ClientConnection;
use crate::formatter::format_response;

pub struct Repl {
    conn: ClientConnection,
}

impl Repl {
    pub fn new(conn: ClientConnection) -> Self {
        Repl { conn }
    }

    pub async fn run(&mut self) {
        let mut rl = DefaultEditor::new().unwrap();
        let history_path = dirs_next().unwrap_or_else(|| ".bytedb_history".into());
        let _ = rl.load_history(&history_path);

        let mut buffer = String::new();

        loop {
            let prompt = if !buffer.is_empty() {
                "     ...> "
            } else if self.conn.active_txn.is_some() {
                "bytedb[txn]> "
            } else {
                "bytedb> "
            };

            match rl.readline(prompt) {
                Ok(line) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() && buffer.is_empty() {
                        continue;
                    }

                    if buffer.is_empty() {
                        let _ = rl.add_history_entry(trimmed);
                    }

                    // Handle backslash continuation
                    if trimmed.ends_with('\\') {
                        buffer.push_str(&trimmed[..trimmed.len() - 1]);
                        buffer.push(' ');
                        continue;
                    }

                    buffer.push_str(trimmed);

                    // If no semicolon yet and not a special command, continue reading
                    let lower = buffer.trim().to_lowercase();
                    let is_special = lower == "exit" || lower == "quit" || lower.starts_with("\\")
                        || lower == "help" || lower == "ping";
                    if !is_special && !buffer.trim().ends_with(';') {
                        buffer.push(' ');
                        continue;
                    }

                    let input = buffer.trim().to_string();
                    buffer.clear();

                    match input.to_lowercase().as_str() {
                        "exit" | "quit" | "\\q" => {
                            let _ = self.conn.disconnect().await;
                            println!("Bye!");
                            break;
                        }
                        "help" | "\\h" => {
                            print_help();
                            continue;
                        }
                        "\\dt" => {
                            self.execute_and_print("SHOW TABLES;").await;
                            continue;
                        }
                        "ping" => {
                            match self.conn.ping().await {
                                Ok(_) => println!("PONG"),
                                Err(e) => println!("Error: {}", e),
                            }
                            continue;
                        }
                        _ => {}
                    }

                    // Handle \d table_name
                    if input.to_lowercase().starts_with("\\d ") {
                        let table = input[3..].trim().trim_end_matches(';');
                        let sql = format!("DESCRIBE {};", table);
                        self.execute_and_print(&sql).await;
                        continue;
                    }

                    self.execute_and_print(&input).await;
                }
                Err(rustyline::error::ReadlineError::Interrupted) => {
                    buffer.clear();
                    println!("^C");
                    continue;
                }
                Err(rustyline::error::ReadlineError::Eof) => {
                    let _ = self.conn.disconnect().await;
                    println!("Bye!");
                    break;
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    break;
                }
            }
        }

        let _ = rl.save_history(&history_path);
    }

    async fn execute_and_print(&mut self, sql: &str) {
        let start = Instant::now();
        match self.conn.query(sql).await {
            Ok(response) => {
                let elapsed = start.elapsed();
                let output = format_response(&response);
                println!("{}", output);
                println!("Time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
        println!();
    }
}

fn dirs_next() -> Option<String> {
    std::env::var("HOME").ok()
        .or_else(|| std::env::var("USERPROFILE").ok())
        .map(|h| format!("{}/.bytedb_history", h))
}

fn print_help() {
    println!("ByteDB Commands:");
    println!("  SQL Queries:");
    println!("    CREATE TABLE name (col TYPE, ...);");
    println!("    INSERT INTO name VALUES (...);");
    println!("    SELECT * FROM name WHERE ...;");
    println!("    UPDATE name SET col = val WHERE ...;");
    println!("    DELETE FROM name WHERE ...;");
    println!();
    println!("  Key-Value:");
    println!("    KV SET \"key\" \"value\";");
    println!("    KV GET \"key\";");
    println!("    KV DELETE \"key\";");
    println!("    KV SCAN \"start\" \"end\";");
    println!();
    println!("  Documents:");
    println!("    DOC INSERT INTO collection {{...}};");
    println!("    DOC FIND IN collection WHERE $.field == \"value\";");
    println!("    DOC UPDATE IN collection WHERE ... SET $.field = val;");
    println!("    DOC DELETE FROM collection WHERE ...;");
    println!();
    println!("  Transactions:");
    println!("    BEGIN;");
    println!("    BEGIN SERIALIZABLE;");
    println!("    COMMIT;");
    println!("    ROLLBACK;");
    println!();
    println!("  Shortcuts:");
    println!("    \\dt          - Show tables");
    println!("    \\d table     - Describe table");
    println!("    \\q           - Quit");
    println!("    \\h           - Help");
    println!();
    println!("  Multi-line: end line with \\ to continue, or omit ; to keep typing");
    println!("  Utility:");
    println!("    ping     - Check server connection");
    println!("    exit     - Quit");
}
