use serde::Deserialize;
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

#[derive(Deserialize)]
struct Transaction {
    // Type of transaction.
    #[serde(alias = "type")]
    kind: String,

    // Client ID.
    #[serde(alias = "client")]
    client_id: u16,

    // Transaction ID.
    #[serde(alias = "tx")]
    transaction_id: u32,

    amount: f32,
}

fn read_csv(path: &str) -> Result<Vec<Transaction>, Box<dyn Error>> {
    let data = std::fs::read_to_string(path)?;
    let mut reader = csv::Reader::from_reader(data.as_bytes());
    let mut transactions = Vec::new();
    for result in reader.deserialize() {
        let trasaction: Transaction = result?;
        transactions.push(trasaction);
    }

    Ok(transactions)
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

    #[test]
    fn test_read_csv_ok() {
        let ret = read_csv("testdata/transactions.csv");
        assert!(ret.is_ok());
        assert!(ret.unwrap().len() == 2);
    }

    #[test]
    fn test_read_csv_ok_with_four_decimal() {
        let ret = read_csv("testdata/transactions.csv");
        let tr = ret.unwrap();
        assert!(tr[0].amount == 1.0191);
        assert!(tr[1].amount == 2.0001);
    }
}
