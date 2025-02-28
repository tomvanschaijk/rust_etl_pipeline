use anyhow::{Context, Result};
use clap::Parser;
use csv_async::{AsyncReader, StringRecord};
use futures::stream::StreamExt;
use rand::Rng;
use sqlx::{
    Pool, Sqlite,
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc::channel,
};

use std::{io::Cursor, str::FromStr};

mod commands;
mod models;

use commands::{Args, Commands};
use models::Person;

const FILE_NAME: &str = "./data/people.csv";
const FIRST_NAMES: [&str; 10] = [
    "Tom", "Johnny", "Jim", "Eric", "Amanda", "Grace", "Judy", "Frank", "Sally", "Will",
];
const LAST_NAMES: [&str; 10] = [
    "Connor",
    "Henderson",
    "Farley",
    "Henson",
    "Jeffries",
    "Carlin",
    "Anderson",
    "O' Sullivan",
    "Dorothy",
    "McDougal",
];

async fn create_csv(number_rows: u32) -> Result<()> {
    tracing::info!("Creating csv file");

    let start = std::time::Instant::now();

    let file = File::create(FILE_NAME).await?;
    let mut writer = BufWriter::new(file);

    // Write the header
    writer.write_all(b"first_name,last_name,age\n").await?;

    // We'll not write every single line, but write in chunks to limit the overhead
    const CHUNK_SIZE: usize = 1000;
    let mut buffer = String::with_capacity(CHUNK_SIZE * 50);

    let mut rng = rand::rng();
    for i in 0..number_rows {
        let first_name = FIRST_NAMES[rng.random_range(0..FIRST_NAMES.len())];
        let last_name = LAST_NAMES[rng.random_range(0..LAST_NAMES.len())];
        let age = rng.random_range(18..=65);

        buffer.push_str(&format!("{},{},{}\n", first_name, last_name, age));

        // Write the chunk if the chunk size is reached
        if i % CHUNK_SIZE as u32 == 0 {
            writer.write_all(buffer.as_bytes()).await?;
            buffer.clear();
        }
    }

    // Write any remaining data in the buffer.
    if !buffer.is_empty() {
        writer.write_all(buffer.as_bytes()).await?;
    }

    writer.flush().await?;

    tracing::info!("CSV file created in {:?}", start.elapsed());

    Ok(())
}

async fn create_empty_table(pool: &Pool<Sqlite>) -> Result<()> {
    tracing::info!("Creating empty people table");

    sqlx::query("DROP TABLE IF EXISTS people")
        .execute(pool)
        .await
        .context("Database error while dropping people table")?;

    sqlx::query(
        r#"
    CREATE TABLE IF NOT EXISTS people (
            id BLOB PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            age INTEGER,
            email TEXT)"#,
    )
    .execute(pool)
    .await
    .context("Database error while creating people table")?;

    // Let's lower the synchronous mode to boost performance by reducing disk flushing
    sqlx::query("PRAGMA synchronous = OFF")
        .execute(pool)
        .await
        .context("Database error while setting synchronous mode")?;

    // Set a 40MB cache (negative value is KB)
    sqlx::query("PRAGMA cache_size = -40000")
        .execute(pool)
        .await
        .context("Database error while setting cache size")?;
    Ok(())
}

async fn process_csv(pool: &sqlx::Pool<Sqlite>) -> Result<()> {
    tracing::info!("Running ETL pipeline");

    // Create channels
    let (to_workers, mut from_reader) = channel::<StringRecord>(100);
    let (to_db, mut from_worker) = channel(100);

    let pool = pool.clone();
    let reader_handle = tokio::spawn(async move {
        // Open the CSV file and concurrently process all records
        let file = File::open(FILE_NAME).await.unwrap();
        let mut reader = AsyncReader::from_reader(file);
        let num_workers = num_cpus::get();
        reader
            .records()
            .for_each_concurrent(num_workers, |record| {
                let to_workers = to_workers.clone();
                async move {
                    let record = record.unwrap();
                    to_workers.send(record).await.unwrap();
                }
            })
            .await;

        tracing::info!("CSV file reading completed, all lines pushed to processing worker");
    });

    let processor_handle = tokio::spawn(async move {
        // Batch configuration
        const BATCH_SIZE: usize = 5_000;
        let mut batch = Vec::with_capacity(BATCH_SIZE);

        // Process and send records in batch
        while let Some(record) = from_reader.recv().await {
            let first_name = &record[0];
            let last_name = &record[1];
            let age = record[2].parse().unwrap_or(0);
            let email = format!("{}.{}_{}@somemail.com", first_name, last_name, age);

            let person = Person::new(first_name, &last_name.to_uppercase(), age, &email);
            batch.push(person);

            if batch.len() == BATCH_SIZE {
                to_db.send(batch).await.unwrap();
                batch = Vec::with_capacity(BATCH_SIZE);
            }
        }

        // Send any remaining records
        if !batch.is_empty() {
            to_db.send(batch).await.unwrap();
        }

        tracing::info!("Processing completed, all records pushed to database writer");
    });

    let writer_handle = tokio::spawn(async move {
        let start = std::time::Instant::now();

        // Dedicated connection for all writes
        let mut connection = pool.acquire().await.unwrap();

        while let Some(people) = from_worker.recv().await {
            let batch_length = people.len();

            let mut query =
                String::from("INSERT INTO people (id, first_name, last_name, age, email) VALUES ");
            let mut values = Vec::new();
            for (i, person) in people.iter().enumerate() {
                if i > 0 {
                    query.push(',');
                }
                query.push_str("(?, ?, ?, ?, ?)");
                values.push(person.id.to_string());
                values.push(person.first_name.clone());
                values.push(person.last_name.clone());
                values.push(person.age.to_string());
                values.push(person.email.clone());
            }

            let mut query = sqlx::query(&query);
            for value in values {
                query = query.bind(value);
            }

            if let Err(e) = query.execute(&mut *connection).await {
                tracing::error!(
                    "Failed to insert batch of {} records: {:?}",
                    batch_length,
                    e
                );

                // Skip to next batch
                continue;
            }

            tracing::info!("Batch of {} records stored in database", batch_length);
        }

        tracing::info!(
            "All person records stored in {:?}, cleaning up database",
            start.elapsed()
        );
        if let Err(e) = sqlx::query("VACUUM").execute(&pool).await {
            tracing::warn!("VACUUM failed: {:?}", e);
        }

        tracing::info!("Completed database clean up");
    });

    let _ = tokio::join!(reader_handle, processor_handle, writer_handle);

    Ok(())
}

async fn run_pipeline() -> Result<()> {
    tracing::info!("Setting up environment");

    let start = std::time::Instant::now();

    const ENV_FILE: &str = include_str!("../.env");
    let mut env_reader = Cursor::new(ENV_FILE);
    dotenvy::from_read(&mut env_reader).expect("Failed to load embedded .env");

    tracing::info!("Connecting to database");
    let database_url = std::env::var("DATABASE_URL")?;
    let connect_options =
        SqliteConnectOptions::from_str(&database_url)?.journal_mode(SqliteJournalMode::Memory);
    let pool = sqlx::SqlitePool::connect_with(connect_options)
        .await
        .context("Could not connect to database")?;

    create_empty_table(&pool).await?;

    process_csv(&pool).await?;

    tracing::info!("Pipeline completed in {:?}", start.elapsed());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Args::parse();
    match cli.command {
        Some(Commands::CreateFile { number_rows }) => create_csv(number_rows).await?,
        Some(Commands::RunPipeline) => run_pipeline().await?,
        None => {
            println!("Run with --help to see instructions");
        }
    }

    Ok(())
}
