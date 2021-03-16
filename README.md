### linedb-rs

`cargo build --release`

`RUST_LOG=debug ./target/release/linedb-rs`

```shell
> nc 127.0.0.1 10322
GET 123
OK
1f0ec9ef-2b8f-4f25-8708-b254b1428145
GET 1
OK
5a0f23e-9347-4173-8c8c-bb00148d561e
GET 9999999
OK
c460dcb2-b1e9-4d2e-ad58-513f60397492
GET asd
ERR: invalid line number
qwe
ERR: invalid command
QUIT
BYE
```

```shell
> nc 127.0.0.1 10322
SHUTDOWN
STOPPING SERVER
```
