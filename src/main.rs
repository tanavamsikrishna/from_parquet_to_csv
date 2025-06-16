use std::path::PathBuf;

use clap::Parser;
use polars::{
    io::SerWriter,
    prelude::{CsvWriter, LazyFrame},
};

#[derive(Parser)]
#[command()]
struct Args {
    file: PathBuf,
}

fn main() {
    let file_name = Args::parse().file;

    let mut df = LazyFrame::scan_parquet(file_name, Default::default())
        .unwrap()
        .collect()
        .unwrap();

    let stdout = std::io::stdout();
    let writer = std::io::BufWriter::new(stdout.lock());

    CsvWriter::new(writer)
        .include_header(true)
        .finish(&mut df)
        .unwrap();
}
