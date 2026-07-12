# Scylla E2E on AWS — verified pass

## Campaign `boson-scylla-e2e-verify` (post-adapter fix)

Topology: **boson-scylla-2n** (1× `t3.medium` bench + 2× `t3.medium` Scylla 6.2), contact points `172.31.44.92:9042,172.31.38.97:9042`. Built on ephemeral `c7i.xlarge`.

### Contract (`boson-backend-scylla`)

```
test result: ok. 11 passed; 0 failed
```

### Catalog (`scenarios_full` `*_scylla`)

```
test result: ok. 26 passed; 0 failed
```

Log: [`aws-verify.log`](aws-verify.log)

## Prior campaign (before fix)

Contract 7/11, catalog 10/16 — see git history / earlier notes.

## Adapter fixes that unblocked AWS

1. **`lwt_applied`** — read first column only via `ColumnIterator` (LWT rows include extra PK/IF columns; derive types rejected them).
2. **`list_queued_for_pool_sorted`** — scan all ready shards, not a small sample.
3. **Idempotency after terminal** — reuse only for queued/running; overwrite mapping when prior job is terminal.
4. **`upsert_job`** — re-insert into `boson_ready` when status is `Queued` (retry path).
