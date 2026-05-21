use std::process::{Command, Stdio};
use std::io::{BufRead, BufReader, Write};
use std::time::Duration;
use std::thread;

fn main() {
    let dir = std::env::var("DATA_DIR").unwrap_or_else(|_| {
        std::env::temp_dir().join(format!("bytedb_k9_{}", std::process::id())).to_string_lossy().into_owned()
    });
    let log_path = format!("{dir}/commits.log");
    let kill_after_secs: u64 = std::env::var("KILL_AFTER").ok().and_then(|s| s.parse().ok()).unwrap_or(3);
    let runs: u32 = std::env::var("RUNS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let writer_bin = which_bin("kill9_writer");
    let verify_bin = which_bin("kill9_verify");

    for run in 1..=runs {
        println!("\n=== run {}/{} ===", run, runs);

        let mut child = Command::new(&writer_bin)
            .env("DATA_DIR", &dir)
            .env("THREADS", "4")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn writer");

        let stdout = child.stdout.take().unwrap();
        let log_path_clone = log_path.clone();
        let pump = thread::spawn(move || {
            let mut f = std::fs::File::create(&log_path_clone).unwrap();
            let r = BufReader::new(stdout);
            for line in r.lines().flatten() {
                writeln!(f, "{}", line).ok();
                f.flush().ok();
            }
        });

        thread::sleep(Duration::from_secs(kill_after_secs));
        println!("[harness] killing pid {} after {}s", child.id(), kill_after_secs);
        let _ = child.kill();
        let _ = child.wait();
        let _ = pump.join();

        let commits_in_log = count_commits(&log_path);
        println!("[harness] writer logged {} commits before kill", commits_in_log);

        let v = Command::new(&verify_bin)
            .env("DATA_DIR", &dir)
            .env("COMMIT_LOG", &log_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .expect("spawn verify");
        if !v.success() {
            eprintln!("[harness] verify FAILED on run {}", run);
            std::process::exit(1);
        }
    }
    println!("\n[harness] all {} runs passed", runs);
}

fn which_bin(name: &str) -> std::path::PathBuf {
    let exe = std::env::current_exe().unwrap();
    let dir = exe.parent().unwrap();
    let candidate = dir.join(format!("{}{}", name, std::env::consts::EXE_SUFFIX));
    if candidate.exists() {
        return candidate;
    }
    panic!("could not find {} next to harness at {:?}", name, dir);
}

fn count_commits(path: &str) -> usize {
    match std::fs::File::open(path) {
        Ok(f) => BufReader::new(f).lines().filter_map(|l| l.ok()).filter(|l| l.starts_with("COMMITTED ")).count(),
        Err(_) => 0,
    }
}
