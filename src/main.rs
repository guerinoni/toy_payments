use std::error::Error;
use std::process;
fn main() {
    let args = std::env::args();
    if args.len() != 2 {
        println!("input error: Malformed input");
        process::exit(1);
    }

    match read_csv(&args.last().unwrap()) {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}
fn read_csv(path: &str) -> Result<(), Box<dyn Error>> {
    let data = std::fs::read_to_string(path)?;
    let mut reader = csv::Reader::from_reader(data.as_bytes());
    for result in reader.records() {
        let record = result?;
        println!("{:?}", record);
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_read_csv_with_invalid_path() {
        assert!(read_csv("ok").is_err());
    }

    #[test]
    fn test_read_csv_with_not_csv_file() {
        assert!(read_csv("Cargo.lock").is_err());
    }
}
