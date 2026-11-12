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
- Units: `Call[R]`, `DB.Call` and `DB.CallRaw` call functions published
  into the database, exactly once per mutation id (`MutationID`,
  `Replayed`); a unit's own error is a `*UnitError`; `Bytes` carries
  blobs as `{"$bytes": …}`. `DB.Units()` publishes bundles, rolls back,
  and reads the deploy history, run log, triggers and secrets.
  `ErrUnitsUnsupported` when the server's discovery document does not
  list `"units"`.
- Units jobs: `Units.Jobs` lists the jobs units scheduled with
  `ctx.scheduler` (pending and failed, with counts), `Units.CancelJob`
  removes one and `Units.RetryJob` runs a failed one again; `Job`,
  `JobsQuery`, `JobList` and the `JobState` constants. Cancel and retry
  are not resent after a lost connection (`ErrUnknownOutcome`).
- Run log constants: `RunStatusOK`, `RunStatusError`, `RunStatusReplayed`
  and `RunStatusRejected` (refused before the unit ran, also accepted by
  `RunsQuery.Status`); `RunTriggerCall`, `RunTriggerSchedule`,
  `RunTriggerChange`, `RunTriggerJob`, `RunTriggerAdmin`, `RunTriggerCLI`
  (what `Units.Call` is logged as) and `RunTriggerMCP`.
- `cmd/azora-dev` and eight runnable examples; `azora-dev` publishes
  `examples/units/units.js` when the engine supports units.
