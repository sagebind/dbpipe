use std::io;

use anyhow::bail;
use once_cell::sync::Lazy;
use regex::Regex;
use sqlx::{AnyConnection, Connection, any::AnyRow};
use structopt::StructOpt;
use tokio_stream::StreamExt;
use url::Url;

mod csv;
mod json;

#[derive(StructOpt)]
struct Options {
    /// Connection URL of the database
    #[structopt(long, env = "DBPIPE_DB")]
    db: Url,

    /// Database user name
    #[structopt(short, long, env = "DBPIPE_USER")]
    user: Option<String>,

    /// Database password
    #[structopt(short, long, env = "DBPIPE_PASSWORD")]
    password: Option<String>,

    /// Execute an UPDATE or DELETE query
    #[structopt(short, long)]
    execute: bool,

    /// Write each matching row in JSON format separated by newlines
    #[structopt(short, long)]
    json: bool,

    /// Don't print CSV headers
    #[structopt(long)]
    no_header: bool,

    /// Silence all output
    #[structopt(short, long)]
    quiet: bool,

    /// Verbose mode (-v, -vv, -vvv, etc)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: usize,

    /// The query to run
    query: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut options = Options::from_args();

    stderrlog::new()
        .module(module_path!())
        .quiet(options.quiet)
        .verbosity(options.verbose)
        .init()
        .unwrap();

    let query = options.query.join(" ");

    log::info!("Query to run: {}", query);

    if !options.execute && is_query_destructive(&query) {
        bail!("Only SELECT queries are allowed when in read-only mode.");
    }

    if let Some(user) = options.user {
        let _ = options.db.set_username(user.as_str());
    }

    if let Some(password) = options.password {
        let _ = options.db.set_password(Some(password.as_str()));
    }

    log::info!("Connecting to {}", options.db.host_str().unwrap());

    let mut connection = AnyConnection::connect(options.db.as_str()).await?;
    let query = sqlx::query(query.as_str());

    if options.execute {
        let result = query.execute(&mut connection).await?;
        println!("{} row(s) affected", result.rows_affected());
    } else {
        let mut stream = query.fetch(&mut connection);

        let stdout = io::stdout();
        let stdout = stdout.lock();

        let mut writer: Box<dyn RowWriter> = if options.json {
            Box::new(json::JsonWriter::new(stdout))
        } else {
            Box::new(csv::CsvWriter::new(stdout, !options.no_header))
        };

        while let Some(result) = stream.next().await {
            let row = result?;
            writer.write(&row)?;
        }
    }

    Ok(())
}

trait RowWriter {
    fn write(&mut self, row: &AnyRow) -> io::Result<()>;
}

fn is_query_destructive(query: &str) -> bool {
    static REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\b(UPDATE|DELETE)\b").unwrap());

    REGEX.is_match(query)
}
