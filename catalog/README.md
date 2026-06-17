# Strangler catalog (Phase 0 / Phase 1)

A **shadow source-of-truth** that runs alongside the existing control plane without
changing its behaviour. It's the first two steps of the incremental rewrite: stand
up a real catalog, prove it matches reality, then start feeding it live — all before
anything depends on it.

It uses its **own** database (`CATALOG_DB`, default `~/.tt-recorder/catalog.sqlite`),
never the control plane's `control.sqlite`. Every integration point is wrapped so a
catalog fault can't affect the running system, and the whole thing is inert unless
`CATALOG_ENABLED=1`.

## What it tracks

| Table | Purpose |
|---|---|
| `recordings` | one row per recording, keyed by final filename, with an explicit lifecycle `state` |
| `blob_locations` | where the bytes live — `local` (hot) and/or `cloud` (cold), per store |
| `transcripts` | transcript state/location per recording |
| `jobs` | explicit work queue (`transcribe`/`archive`/…); `shadow=1` = intent only, never executed |

`recordings.state`: `discovered → recording → stored → transcribing → transcribed →
archived → evicted` (plus side states `orphan_flv`, `missing`). State only ever
advances — a late event can't regress a known one.

## Phase 0 — shadow & validate (read-only)

```bash
# import what the control plane already knows (transfers table), read-only
python -m catalog backfill --control-db ~/.tt-recorder/control.sqlite

# also scan the live fleet (each backend /files + each storage /files/inventory)
python -m catalog backfill --live

# diff the catalog against live inventory and report drift
python -m catalog parity

# what the catalog knows
python -m catalog stats
```

`parity` reports three kinds of drift: `live_only` (on disk, untracked),
`catalog_missing` (tracked, not found live), and `size_mismatch`. The full report is
stored in `catalog_meta.last_parity_report`. Run it for a while and watch the counts
trend to zero — that's the signal the catalog is trustworthy.

## Phase 1 — write-through (live, still no executor)

Set `CATALOG_ENABLED=1` on the control plane. Then:

- every recorder `POST /events` is mirrored into the catalog (`session_started` →
  `recording`, `session_ended` → `stored` + a **shadow** transcribe job);
- a background loop materialises transcribe *intent* and compares it to what the live
  transcription workers actually report.

Read-only visibility (both require login):

```
GET /api/catalog/stats      # recordings/locations/jobs counts
GET /api/catalog/drift      # last parity report + last shadow-vs-live comparison
```

Nothing here executes a job or moves a byte. `claim_job()` skips shadow jobs by
default, so even if a future worker is pointed at this DB it won't act on Phase 1
intent.

### Env vars

| Var | Default | Purpose |
|---|---|---|
| `CATALOG_ENABLED` | `0` | master switch for the control-plane integration |
| `CATALOG_DB` | `~/.tt-recorder/catalog.sqlite` | catalog database path |
| `CONTROL_PLANE_DB` | `~/.tt-recorder/control.sqlite` | read-only source for backfill |
| `CATALOG_SHADOW_INTERVAL` | `900` | seconds between shadow passes |

## Tests

```bash
python tests/test_catalog.py        # no pytest needed
python -m pytest tests/ -q          # also works
```

## What comes next (not in these phases)

Phase 2 adds a stateless transcribe worker that claims **non-shadow** jobs; Phase 3
adds the hybrid hot/cold lifecycle (`archive`/`evict`/`verify`); Phase 4 flips the
Files/transcripts reads onto the catalog and deletes the move engine + routing rules.
See the design discussion for the full sequence.
