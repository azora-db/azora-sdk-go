# Changelog

## Unreleased

First version of the Go SDK.

- Connecting takes two values: the database's public URL
  (`https://host/<database>?key=…`) and a private token. `ConnectEnv`
  reads `AZORA_URL` and `AZORA_TOKEN`; the realtime endpoint and file
  storage are found from the URL, and a URL without a key reads the
  server's key from `/.well-known/azora.json` (HTTPS or loopback only).
- Connections over TCP and WebSocket with the Noise IK and XX handshakes
  (pinned server key or fingerprint), heartbeats, and reconnects with
  backoff and jitter; an optional pool.
- `Query`, `Select`, `Get` and the generic `All`, `One`, `Scalar`, with
  decoding into structs, maps and scalars; `Exec` and `Batch` with
  exactly-once delivery through mutation ids.
- `Subscribe` (database, table, row, files and notify subscriptions) with
  resume, flow control and checksum verification; `Replica`; `Live`
  tables; `Watch`.
- Filters as builders and as text.
- File storage: uploads, resumable uploads, ranged downloads, listing,
  signed download and upload links.
- API keys and client tokens, with refresh on the open connection.
- `azorasql`: a `database/sql` driver, with buffered transactions.
- `cmd/azora-dev` and seven runnable examples.
