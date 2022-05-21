use csv::Writer;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::io::Write;
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

#[derive(Serialize)]
struct Account {
    // Client ID.
    #[serde(rename = "client")]
    client_id: u16,

    // Total founds available for trading.
    // Should be equal to (total - held).
    #[serde(with = "four_precision_number_format")]
    available: f32,

    // Total founds held for dispute.
    // Should be equal to (total - available).
    #[serde(with = "four_precision_number_format")]
    held: f32,

    // The total funds that are available or held.
    // This should be equal to (available + held).
    #[serde(with = "four_precision_number_format")]
    total: f32,

    // When charge back occurs, account is locked.
    locked: bool,
}

fn write_accounts(accounts: &[Account], write_impl: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_writer(write_impl);
    for a in accounts.iter() {
        writer.serialize(a)?;
    }

    writer.flush()?;
    Ok(())
}

mod four_precision_number_format {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(number: &f32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let precision = 4;
        let s = format!("{number:.precision$}");
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<f32, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<f32>().map_err(serde::de::Error::custom)
    }
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

    #[test]
    fn test_serialize_account_ok() {
        let a = Account {
            client_id: 1,
            available: 1.5,
            held: 0.0,
            total: 1.5,
            locked: false,
        };

        let b = Account {
            client_id: 2,
            available: 2.0,
            held: 0.0,
            total: 2.0,
            locked: false,
        };

        let accounts = vec![a, b];

        let mut output: Vec<u8> = Vec::new();
        let ret = write_accounts(&accounts, &mut output);
        assert!(ret.is_ok());
        let data = String::from_utf8(output);
        assert!(data.is_ok());
        let expected = std::fs::read_to_string("testdata/accounts.csv");
        assert!(expected.is_ok());
        assert_eq!(data.unwrap(), expected.unwrap());
    }

    #[test]
    fn test_serialize_output_four_decimal_precision() {
        let accounts = vec![Account {
            client_id: 2,
            available: 2.0,
            held: 0.0,
            total: 2.0000,
            locked: false,
        }];

        let mut output: Vec<u8> = Vec::new();
        write_accounts(&accounts, &mut output).unwrap();
        let data = String::from_utf8(output).unwrap();
        assert!(data.contains("2.0000"));
    }
}
