use csv::Writer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::process;

fn main() {
    let args = std::env::args();
    if args.len() != 2 {
        println!("input error: Malformed input");
        process::exit(1);
    }

    let transactions = match read_csv(&args.last().unwrap()) {
        Ok(t) => t,
        Err(e) => {
            println!("input error: {}", e);
            process::exit(2);
        }
    };

    let mut engine = Engine::default();
    match engine.process_transactions(&transactions) {
        Ok(()) => (),
        Err(e) => {
            println!("process error: {}", e);
            process::exit(3);
        }
    }

    match write_accounts(&engine.get_accounts(), &mut std::io::stdout()) {
        Ok(()) => (),
        Err(e) => {
            println!("writing error: {}", e);
            process::exit(4);
        }
    }
}

#[derive(Default, Deserialize)]
struct Transaction {
    // Type of transaction.
    #[serde(alias = "type")]
    kind: String,

    // Client ID.
    #[serde(alias = "client")]
    client_id: u16,

    // Transaction ID.
    #[serde(alias = "tx")]
    transaction_id: TransactionID,

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

#[derive(Default, Serialize)]
struct Account {
    // Client ID.
    #[serde(rename = "client")]
    client_id: ClientID,

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

fn write_accounts(
    accounts: &[&Account],
    write_impl: &mut impl Write,
) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_writer(write_impl);
    for a in accounts.iter() {
        writer.serialize(a)?;
    }

    writer.flush()?;
    Ok(())
}

mod four_precision_number_format {
    use serde::Serializer;

    pub fn serialize<S>(number: &f32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let precision = 4;
        let s = format!("{number:.precision$}");
        serializer.serialize_str(&s)
    }
}

enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl std::str::FromStr for TransactionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "deposit" => Ok(TransactionType::Deposit),
            "withdrawal" => Ok(TransactionType::Withdrawal),
            "dispute" => Ok(TransactionType::Dispute),
            "resolve" => Ok(TransactionType::Resolve),
            "chargeback" => Ok(TransactionType::Chargeback),
            _ => Err(format!("'{}' is not a valid value for TransactionType", s)),
        }
    }
}

type TransactionID = u32;
type TransactionIndex = usize;
type ClientID = u16;

#[derive(Default)]
struct Engine {
    client_account: HashMap<ClientID, Account>,
    transaction_under_dispute: HashMap<TransactionIndex, bool>,
}

impl Engine {
    fn process_transactions(&mut self, transactions: &[Transaction]) -> Result<(), Box<dyn Error>> {
        for tr in transactions {
            let account = self
                .client_account
                .entry(tr.client_id)
                .or_insert_with(|| Account {
                    client_id: tr.client_id,
                    ..Default::default()
                });

            match tr.kind.parse().unwrap() {
                TransactionType::Deposit => {
                    account.available += tr.amount;
                    account.total += tr.amount;
                }
                TransactionType::Withdrawal => {
                    if account.available < tr.amount {
                        let msg = format!(
                            "engine error: Client ID {} doesn't have sufficient avalable",
                            account.client_id
                        );
                        return Err(msg.into());
                    }
                    account.available -= tr.amount;
                    account.total -= tr.amount;
                }
                TransactionType::Dispute => {
                    let idx = match transactions
                        .iter()
                        .position(|t| t.transaction_id == tr.transaction_id)
                    {
                        Some(index) => index,
                        None => continue,
                    };

                    let amount = transactions[idx].amount;
                    account.available -= amount;
                    account.held += amount;
                    self.transaction_under_dispute.insert(idx, true);
                }
                TransactionType::Resolve => {
                    let idx = match transactions
                        .iter()
                        .position(|t| t.transaction_id == tr.transaction_id)
                    {
                        Some(index) => index,
                        None => continue,
                    };

                    match self.transaction_under_dispute.get(&idx) {
                        Some(_) => self.transaction_under_dispute.remove(&idx),
                        None => continue,
                    };

                    let amount = transactions[idx].amount;
                    account.available += amount;
                    account.held -= amount;
                }
                TransactionType::Chargeback => {
                    let idx = match transactions
                        .iter()
                        .position(|t| t.transaction_id == tr.transaction_id)
                    {
                        Some(index) => index,
                        None => continue,
                    };

                    match self.transaction_under_dispute.get(&idx) {
                        Some(_) => self.transaction_under_dispute.remove(&idx),
                        None => continue,
                    };

                    let amount = transactions[idx].amount;
                    account.held -= amount;
                    account.total -= amount;
                    account.locked = true;
                }
            }
        }

        Ok(())
    }

    fn get_accounts(&self) -> Vec<&Account> {
        self.client_account.iter().map(|a| a.1).collect::<Vec<_>>()
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

        let accounts = vec![&a, &b];

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
        let accounts = vec![&Account {
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

    #[test]
    fn test_deposit_increase_total_and_available() {
        let t = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 1.0,
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);
        assert!(e.process_transactions(&vec![t]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 2.0);
        assert_eq!(account.available, account.total);
    }

    #[test]
    fn test_withdrawal_decrease_available_and_total() {
        let t = Transaction {
            kind: "withdrawal".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 5.0,
        };

        let a = Account {
            client_id: 1,
            total: 10.0,
            available: 10.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);
        assert!(e.process_transactions(&vec![t]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 5.0);
        assert_eq!(account.available, account.total);
    }

    #[test]
    fn test_withdrawal_with_not_sufficient_available() {
        let t = Transaction {
            kind: "withdrawal".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 5.0,
        };

        let a = Account {
            client_id: 1,
            total: 3.0,
            available: 3.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);
        assert!(e.process_transactions(&vec![t]).is_err());
    }

    #[test]
    fn test_dispute_decrease_available_increase_held() {
        let t0 = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 10.0,
        };
        let t1 = Transaction {
            kind: "dispute".to_string(),
            client_id: 1,
            transaction_id: 1,
            ..Default::default()
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            held: 0.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);

        assert!(e.process_transactions(&vec![t0, t1]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 1.0);
        assert_eq!(account.held, 10.0);
    }

    #[test]
    fn test_dispute_refere_to_not_existing_transaction() {
        let t0 = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 10.0,
        };
        let t1 = Transaction {
            kind: "dispute".to_string(),
            client_id: 1,
            transaction_id: 2,
            ..Default::default()
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            held: 0.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);

        assert!(e.process_transactions(&vec![t0, t1]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 11.0);
        assert_eq!(account.held, 0.0);
        assert_eq!(account.total, 11.0);
    }

    #[test]
    fn test_resolve_increase_available_decrease_held() {
        let t0 = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 10.0,
        };
        let t1 = Transaction {
            kind: "dispute".to_string(),
            client_id: 1,
            transaction_id: 1,
            ..Default::default()
        };
        let t2 = Transaction {
            kind: "resolve".to_string(),
            client_id: 1,
            transaction_id: 1,
            ..Default::default()
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            held: 0.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);

        assert!(e.process_transactions(&vec![t0, t1, t2]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 11.0);
        assert_eq!(account.total, 11.0);
        assert_eq!(account.held, 0.0);
    }

    #[test]
    fn test_resolve_refere_to_not_existing_transaction() {
        let t0 = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 10.0,
        };
        let t1 = Transaction {
            kind: "resolve".to_string(),
            client_id: 1,
            transaction_id: 11,
            ..Default::default()
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            held: 0.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);

        assert!(e.process_transactions(&vec![t0, t1]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 11.0);
        assert_eq!(account.total, 11.0);
        assert_eq!(account.held, 0.0);
    }

    #[test]
    fn test_resolve_refere_to_transaction_not_under_dispute() {
        let t0 = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 10.0,
        };
        let t1 = Transaction {
            kind: "resolve".to_string(),
            client_id: 1,
            transaction_id: 1,
            ..Default::default()
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            held: 0.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);

        assert!(e.process_transactions(&vec![t0, t1]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 11.0);
        assert_eq!(account.total, 11.0);
        assert_eq!(account.held, 0.0);
    }

    #[test]
    fn test_chargeback_decrease_total_decrease_held_and_lock() {
        let t0 = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 10.0,
        };
        let t1 = Transaction {
            kind: "dispute".to_string(),
            client_id: 1,
            transaction_id: 1,
            ..Default::default()
        };
        let t2 = Transaction {
            kind: "chargeback".to_string(),
            client_id: 1,
            transaction_id: 1,
            ..Default::default()
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            held: 0.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);

        assert!(e.process_transactions(&vec![t0, t1, t2]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 1.0);
        assert_eq!(account.total, 1.0);
        assert_eq!(account.held, 0.0);
        assert!(account.locked);
    }

    #[test]
    fn test_chargeback_refere_to_not_existing_transaction() {
        let t0 = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 10.0,
        };
        let t1 = Transaction {
            kind: "chargeback".to_string(),
            client_id: 1,
            transaction_id: 11,
            ..Default::default()
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            held: 0.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);

        assert!(e.process_transactions(&vec![t0, t1]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 11.0);
        assert_eq!(account.total, 11.0);
        assert_eq!(account.held, 0.0);
    }

    #[test]
    fn test_chargeback_refere_to_transaction_not_under_dispute() {
        let t0 = Transaction {
            kind: "deposit".to_string(),
            client_id: 1,
            transaction_id: 1,
            amount: 10.0,
        };
        let t1 = Transaction {
            kind: "chargeback".to_string(),
            client_id: 1,
            transaction_id: 1,
            ..Default::default()
        };

        let a = Account {
            client_id: 1,
            total: 1.0,
            available: 1.0,
            held: 0.0,
            ..Default::default()
        };

        let mut e = Engine::default();
        e.client_account.insert(a.client_id, a);

        assert!(e.process_transactions(&vec![t0, t1]).is_ok());

        let account = e.client_account.get(&1u16).unwrap();
        assert_eq!(account.available, 11.0);
        assert_eq!(account.total, 11.0);
        assert_eq!(account.held, 0.0);
    }

    #[test]
    fn test_only_deposit() {
        let transactions = read_csv("testdata/transactions.csv").unwrap();
        let mut engine = Engine::default();
        let ret = engine.process_transactions(&transactions);
        assert!(ret.is_ok());
        let mut output: Vec<u8> = Vec::new();
        let ret = write_accounts(&engine.get_accounts(), &mut output);
        assert!(ret.is_ok());

        let data = String::from_utf8(output).unwrap();
        assert_eq!(
            String::from(
                "client,available,held,total,locked
1,1.0191,0.0000,1.0191,false
2,2.0001,0.0000,2.0001,false
"
            ),
            data
        )
    }
}
