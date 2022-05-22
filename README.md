# Toy Payments

This is a little exercise that emulate a payments engine. 
From a .csv input of list of transactions returns the client account balances in .csv.

## run
```bash
cargo run -- transactions.csv > accounts.csv
```

## test
```bash
cargo test
```

```bash
cargo tarpaulin -v
91.45% coverage, 214/234 lines covered
```
