# TikTok Live Recorder — Product & Functional Specification

> **Status:** living spec. Describes what the system is *for* and what it *should do* —
> the intended behaviour, not a line-by-line description of the current code. Where the
> implementation already matches, this doubles as documentation; where it doesn't, this
> is the target.
>
> **Audience:** the operator (a single owner running a self-hosted fleet) and anyone
> extending the system.

---

## 1. Purpose & vision

A self-hosted system that **monitors a watchlist of TikTok creators, records their live
streams the moment they go live, and turns every recording into durable, searchable
content** — video safely archived to cheap cloud storage, audio transcribed to
timestamped text, and live chat captured alongside.

It exists because live streams are ephemeral: they vanish when the creator ends them.
The system's job is to **never miss a stream, never lose a recording, and make the
archive instantly searchable** — all without babysitting.

### Design principles

1. **Never lose the only copy.** A recording is deleted from a box *only after* it is
   verified present somewhere else. This invariant overrides every optimisation.
2. **Automatic by default.** The happy path — detect live → record → archive →
   transcribe → index — needs zero human action. Humans intervene only for setup,
   cookies, and exceptions.
3. **Single operator, many machines.** One control plane orchestrates a fleet of
   cheap VPSes. The operator drives everything from one web UI; the machines are
   cattle, not pets.
4. **Degrade, don't fail.** An unreachable box, a corrupt recording, an expired
   cookie, or a full disk should degrade one creator or one file — never take down
   the pipeline or lose unrelated data.
5. **The UI is the interface.** Anything an operator needs to do routinely — toggle a
   mode, move a watcher, rotate cookies, install a dependency — is a button, not an
   SSH session or an env-file edit.

---

## 2. Actors & roles

| Actor | Description |
|---|---|
| **Operator** | The single human owner. Logs into the control plane, manages the watchlist, storage, and archive, and reads transcripts. |
| **Control plane** | The brain. One process on the operator's machine (or a home server). Holds the source-of-truth database, orchestrates all transfers, and serves the web UI. |
| **Recorder backend** | A VPS that watches assigned creators and records their live streams. Stateless beyond its local recordings + watchlist. |
| **Storage / transcription server** | A VPS that receives recordings (or audio), transcribes them with Whisper, serves transcripts, and pushes video to cloud archive. |
| **Cloud archive** | An external object store (Filen, Backblaze B2, S3, Dropbox, or any rclone remote) that holds the durable copy of the video. |
| **Creator** | A TikTok user being watched. Not a system user — the subject of recording. |

---

## 3. The pipeline (end to end)

The canonical flow, fully automatic:

```
Creator goes live
   │
   ▼
Recorder detects live, starts recording      ──►  TK_<user>_<timestamp>.mp4
   │                                                 (per-creator dir on the recorder)
   ▼
Recording ends (creator offline)  ──►  remux to final .mp4
   │
   ├─────────────────────────────► VIDEO PATH
   │                                Video is moved to durable storage:
   │                                  • to a storage server, and/or
   │                                  • straight to cloud archive (rclone, verified)
   │                                Local copy deleted only after verification.
   │
   └─────────────────────────────► TRANSCRIPT PATH
                                    Audio is transcribed with Whisper:
                                      • from the full video, or
                                      • from a small extracted audio track (audio-first)
                                    Output: timestamped .txt (+ optional .srt/.json)
                                    Folded into a full-text search index.

Meanwhile, during the live:
   Chat capture records comments / gifts / likes  ──►  <recording>_chat.jsonl
                                                        indexed for search, matched to
                                                        its recording.
```

The operator sees, per recording, a live status: **recording → on storage / archived →
transcript available → searchable**.

---

## 4. Functional requirements

### 4.1 Watchlist management

- **Add a creator** by TikTok username (with or without `@`). The system assigns them
  to a recorder backend (automatically to the least-loaded, or to a chosen backend).
- **Poll interval** is configurable per creator (how often to check if they're live).
- **Chat capture** is opt-in per creator (default on): capture live comments, gifts,
  and likes to a log alongside the recording.
- **Remove a creator** stops recording and removes them from the watchlist.
- **Migrate a creator** between recorder backends without losing coverage: the watcher
  is added on the target *before* it's removed from the source, and the move is refused
  (or explicitly forced) if the source can't confirm the old watcher was stopped — so a
  creator is **never recorded on two backends at once**.
- **No hard cap** on watchers per backend by default; capacity is a soft signal, not a
  wall. An optional per-backend limit can be set.
- **State visibility:** each watcher shows its state (starting / idle / recording /
  backoff / error), consecutive failures, last error, and which backend hosts it.
- **Self-healing:** a crashed recorder subprocess is restarted with exponential backoff;
  after repeated crashes it enters an `error` state that surfaces in the UI for one-click
  re-enable. A clean exit (creator simply offline) is **not** treated as a crash.

### 4.2 Recording

- Records a creator's live stream to a per-creator directory as
  `TK_<user>_<timestamp>.mp4`.
- Recording is a fast, lossless remux (no re-encode) so the recorder stays cheap.
- An interrupted recording (process killed, creator's connection dropped) leaves a
  recoverable intermediate that the system finalises rather than discarding.
- **A recording in progress is never touched** by any downstream step (transfer,
  transcription, eviction). Downstream work waits for the file to settle and for no
  process to hold it open.

### 4.3 Video durability & archive

- Every finished recording must reach **at least one durable location** beyond the
  recorder before the recorder's copy is eligible for deletion.
- Durable = a storage server **or** the cloud archive, verified by size/checksum.
- **Cloud archive** supports any rclone remote (Filen, B2, S3, Dropbox, raw config).
  Credentials are configured once through the UI and pushed to the box that performs the
  upload; secrets are obscured on the box, never stored in plaintext in the app.
- **Retention & eviction:** the operator sets how long a box keeps its local copy after
  a verified archive, and disk-pressure thresholds that trigger early eviction of the
  oldest already-archived files. Eviction is **presence-verified** — a file is only
  removed locally once confirmed still present on the remote.
- **Playback** of archived video works from the UI even after the local copy is gone
  (streamed back from the cloud on demand, with in-browser seeking).

### 4.4 Transcription

- Every recording is transcribed to a **timestamped, human-readable transcript**
  (`[HH:MM:SS] text`), with a header carrying language, duration, and model.
- Transcription uses Whisper (faster-whisper). Model, beam size, VAD, and concurrency
  are configurable per box.
- **Salvage corrupt recordings.** Unreliable streams produce corrupt/truncated files.
  The system must extract *as much transcript as possible* rather than giving up:
  1. Try a direct decode.
  2. On failure or a suspiciously empty result, do a fault-tolerant ffmpeg pass that
     skips bad packets and pulls whatever audio still decodes.
  3. As a last resort, rebuild a broken container (missing/broken moov atom) using a
     known-good recording as a structural reference, then extract from that.
  Recovered transcripts are **flagged as salvaged/partial** in the UI.
- **Audio-first mode (the rebuilt pipeline).** Optionally, transcription is decoupled
  from the video transfer: the recorder extracts a small audio track (16 kHz mono),
  and only that small file is sent to a transcription box. The full video takes the
  separate archive path and **never transits the transcription box**, which then needs
  almost no disk or bandwidth. Most salvage happens at the extraction step. This is a
  toggle; when on, transcription boxes stop auto-scanning full videos.
- **Give-up handling:** a file that repeatedly fails is marked and stops being retried
  forever, but is visible and manually re-queueable.
- **On-demand re-transcription:** the operator can re-run a specific recording — e.g.
  with a larger model for accuracy, or to translate a non-English stream to English.
- Transcripts are **durable enough to outlive the video**: a recording whose video has
  been evicted to cloud still shows its transcript and remains searchable.

### 4.5 Chat capture

- When enabled for a creator, capture live comments, gifts, and likes to a structured
  log (`<recording>_chat.jsonl`) during the stream.
- Chat logs are **matched to their recording** and **full-text indexed** so the operator
  can search what was said in chat, not just what the creator said.
- Chat capture depends on optional recorder dependencies; the UI must **diagnose** when
  those are missing (and why chat isn't recording) and offer a **one-click install**.

### 4.6 Search & catalog

- A **catalog** (source-of-truth database on the control plane) records every recording,
  its locations (local / cloud tiers), transcript status, and chat linkage.
- **Full-text search** across all transcripts *and* all chat logs, from one search box,
  with snippets, filters (transcript vs chat, by creator), and links that jump to the
  recording / player.
- Search is a **fast local query** (the catalog holds the index), not a fan-out to every
  box on every keystroke.
- The **Files view** is catalog-backed: it lists every known recording with its status
  regardless of which box currently holds it, and falls back to a live fan-out only when
  the catalog is empty.

### 4.7 Playback & viewing

- A **transcript viewer** shows the timestamped text for any recording.
- A **video player** plays a recording (local or streamed from cloud archive).
- **Synced playback (target):** clicking a transcript line seeks the video there; the
  current line highlights and follows as the video plays; a search hit within a recording
  jumps the player to that moment.

### 4.8 Cookies

- Recorders need TikTok cookies (notably `sessionid_ss`) to access restricted streams.
- Cookies are **stored per recorder backend** and picked up on the next recording session
  (no restart).
- The operator can **view, at a glance, the stored cookies on every backend** — which
  keys are set, masked values, and a clear "set / missing / expired-looking" status —
  and edit them inline.
- **(Target)** the system should detect likely-expired cookies (e.g. persistent
  "went-offline-immediately" loops) and alert, rather than silently failing to record.

### 4.9 Alerts & notifications

- The operator is proactively notified about operational conditions via webhook, ntfy,
  or Slack: disk filling / projected time-to-full, a box gone unreachable, watchers in
  error state, transcription give-ups, and archive failures.
- Alerts are configurable (which conditions, which channel) and testable from the UI.
- **(Target)** content-facing notifications, distinct from ops alerts: "creator X is
  live now", and "transcript ready" with a short summary + a search link.

### 4.10 Fleet operations & deployment

- **One-click provisioning** of a new recorder or storage server over SSH from the UI,
  including all dependencies and a systemd service.
- **One-click updates:** push new code to any box and restart its service. Updates are
  **detected** by comparing each box's build id against the control plane's, and surfaced
  in an Updates view. Deploys must be **robust to ownership** — they stage files and
  install into place so they work whether logged in as root or a sudo-capable user, and
  regardless of who owns the install directory.
- **Per-machine identity** is `ssh_host:ssh_port`, not public IP — so two servers behind
  one NAT (same IP, different ports) are treated as distinct, and colocation (recorder +
  storage on one box) is detected correctly.
- Routine box-level actions are **one click with saved SSH creds**: install a missing
  dependency, toggle a mode, set retention, protect a service from the OOM killer.
- **Health checks** run continuously; each box reports build, capacity, disk, and
  live state. Disk usage is sampled over time to drive fill-rate forecasting.

### 4.11 Storage cost & capacity (target)

- A single view across recorders, storage boxes, and cloud archive: total stored, growth
  rate (GB/day), projected days-to-full per box, and estimated monthly cloud cost per the
  configured provider — broken down per creator — so retention decisions are data-driven.

---

## 5. Cross-cutting invariants (must always hold)

1. **No data loss:** a file is deleted from a location only after verified presence
   elsewhere. Truncated/smaller remote copies do not count as verified.
2. **No duplicate recording:** a creator is recorded by exactly one backend at a time;
   migrations preserve this or refuse.
3. **No double transcription:** a recording is transcribed once; enabling audio-first
   must not cause a box to also auto-transcribe the same video.
4. **In-progress files are sacrosanct:** never transfer, transcribe, evict, or delete a
   file that is still being written.
5. **Secrets never leak into artifacts:** cookies, tokens, and archive credentials are
   never written to logs, transcripts, commit history, or plaintext config on a box.
6. **A single broken thing degrades locally:** one bad file, box, or creator must not
   stall or crash the pipeline for the rest.
7. **The UI cannot be silently bricked:** a front-end error must not disable navigation
   or hide state; the operator always has a working way to see and fix the fleet.

---

## 6. Data model (conceptual)

- **Backend** — a recorder VPS: identity, URL, auth, health, machine (ssh host:port).
- **Watcher** — a creator on the watchlist: username, assigned backend, poll interval,
  chat on/off, live state.
- **Storage server** — a storage/transcription VPS: URL, auth, health, disk, machine,
  archive config.
- **Recording** — a captured stream: filename, creator, source box, byte size, start
  time; the catalog's primary entity.
- **Blob location** — where a recording's bytes live: tier (local / cloud), which box or
  remote, verified size.
- **Transcript** — status (none / pending / processing / done / gave-up / salvaged) and
  indexed text for a recording.
- **Chat log** — a captured chat stream, matched to a recording, indexed.
- **Transfer** — the ledger of moving a file between locations: status, attempts,
  destination — the basis of retry and crash recovery.
- **Disk sample** — periodic free/total per box, for fill-rate forecasting.
- **Config / events** — key-value settings and lifecycle event history.

---

## 7. Non-goals

- **Not** a multi-tenant SaaS. One operator, one trust boundary. (Read-only viewer
  accounts are a possible future extension, not a core goal.)
- **Not** a re-encoding / editing suite. Recording is a fast remux; heavy media
  processing (clip cutting, highlights) is an optional add-on, not the core.
- **Not** a TikTok API client beyond what recording/chat require. It does not post,
  interact, or scrape profiles.
- **Not** responsible for the legality/ethics of recording — that's the operator's
  responsibility; the system provides the mechanism.

---

## 8. Roadmap themes (intent, unordered)

These are directions the system is meant to grow, consistent with the vision above:

- **Rebuilt transcription pipeline** — recorder → cloud video direct; recorder → small
  audio → transcriber; fixes-on-failure → catalog. (Audio-first transcription is the
  first phase.)
- **Smarter coverage** — adaptive polling from each creator's historical live-times;
  cookie-expiry detection; an orphaned-watcher reconciler as defence-in-depth.
- **Content value** — synced transcript/video player; auto-highlights from chat surges +
  transcript; per-creator recording history / coverage timeline.
- **Cost & scale** — storage/cost forecasting; least-*recording* backend placement;
  bulk "drain a backend" for maintenance.
- **Quality** — on-demand re-transcription with larger models / translation; dedup of
  accidentally-double-recorded streams.

---

## 9. Glossary

| Term | Meaning |
|---|---|
| **Control plane** | The orchestrating web app + source-of-truth DB. |
| **Backend / recorder** | A VPS that records creators. |
| **Storage / transcription box** | A VPS that stores video and/or transcribes. |
| **Archive** | The external cloud remote (rclone) holding durable video. |
| **Catalog** | The control-plane database of recordings, transcripts, chat, locations. |
| **Audio-first** | Transcribing from a small extracted audio track instead of the full video. |
| **Salvage** | Recovering a transcript from a corrupt/truncated recording. |
| **Eviction** | Deleting a local copy after it's verified present on the remote. |
| **Colocation** | Recorder and storage roles running on the same machine. |
| **Machine identity** | `ssh_host:ssh_port` — distinguishes boxes behind one NAT'd IP. |
