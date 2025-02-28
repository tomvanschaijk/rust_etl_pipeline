use clap::{Parser, Subcommand, command};

#[derive(Parser)]
#[command()]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Create the data file
    CreateFile {
        /// The number of lines in the CSV file
        number_rows: u32,
    },
    /// Run the ETL pipeline
    RunPipeline,
}
