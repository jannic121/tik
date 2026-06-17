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

## Refinements R1 + R2 (truthful numbers)

The first live run exposed two gaps; both are control-plane-only (no fleet deploy):

- **R1 — state reconciliation.** A `done` transfer doesn't mean the bytes are still
  on hot storage. `reconcile_states()` relabels `stored` recordings whose `.mp4`
  isn't in the live inventory as **`evicted`** (and cancels their stale transcribe
  jobs). Non-destructive — only labels change. So `by_state` reflects what's
  actually on disk now, not historical "we shipped it once."
- **R2 — real transcript status.** `/files/inventory` never returns `.txt`, so the
  catalog couldn't see transcripts. `collect_transcript_statuses()` queries the
  **existing** `/transcripts/all-statuses` on every storage server and merges by
  filename (so transcripts on a *different* box than the recording are still found).
  `ingest_transcript_statuses()` marks them done and cancels the now-pointless
  transcribe jobs. **No storage-side code change needed.**

Both run automatically every shadow pass, and on `python -m catalog backfill --live`.
The net effect: the fake "everything needs transcribing" backlog collapses to the
real one, and `by_state` splits into honest `stored` vs `evicted`.

## Phase 2 — stateless transcribe worker (shadow first)

`worker_transcribe.py` works the **real** backlog (`transcribe_backlog()` =
`stored` + no done transcript). Dry-run by default — it changes nothing until you
both flip the work to real jobs *and* run the worker with `--execute`:

```bash
python worker_transcribe.py plan            # read-only: show the real backlog
python worker_transcribe.py run             # loop, DRY-RUN (no whisper, no writes)

# --- go live (deliberate) ---
python -m catalog promote                   # turn the backlog into REAL (non-shadow) jobs
python worker_transcribe.py run --execute   # now it actually transcribes
```

`claim_job()` skips shadow jobs, so until `promote` is run there is nothing for
`--execute` to do — the gate is explicit.

Extra CLI:

```bash
python -m catalog backlog      # how many recordings genuinely need transcription
python -m catalog promote      # Phase 2 go-live (creates real transcribe jobs)
```

## What comes next

Phase 3 adds the hybrid hot/cold lifecycle (`archive`/`evict`/`verify` jobs); Phase 4
flips the Files/transcripts reads onto the catalog and deletes the move engine +
routing rules. **Phase 2 going live** (transcribing) and **Phase 3 evict** (deleting
local copies) are the two points of no return — each its own deliberate, watched step.
