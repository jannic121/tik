-- TT Recorder catalog — strangler Phase 0/1.
--
-- The single source of truth for recordings, their byte locations across tiers
-- (local hot / cloud cold), transcripts, and jobs. Lives in its OWN sqlite file
-- (CATALOG_DB), separate from the control plane's control.sqlite, so shadow mode
-- can never disturb the running system.

CREATE TABLE IF NOT EXISTS catalog_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS recordings (
    id           TEXT PRIMARY KEY,           -- stable id (uuid4 for now)
    creator      TEXT NOT NULL,              -- normalised username (no @, lowercase)
    filename     TEXT NOT NULL,              -- final form: TK_<user>_<date>_<time>.mp4
    node         TEXT,                       -- backend_id / host that recorded it
    backend_pk   TEXT,                       -- control-plane backend id, if known
    started_at   REAL,                       -- parsed from the filename timestamp
    ended_at     REAL,
    duration     REAL,
    byte_size    INTEGER,
    content_hash TEXT,                       -- sha256, when known (null in shadow)
    state        TEXT NOT NULL DEFAULT 'discovered',
        -- discovered -> recording -> stored -> transcribing -> transcribed
        --            -> archived -> evicted ; plus orphan_flv / missing (side states)
    error        TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rec_filename ON recordings(filename);
CREATE INDEX IF NOT EXISTS idx_rec_state   ON recordings(state);
CREATE INDEX IF NOT EXISTS idx_rec_creator ON recordings(creator);

-- Where the bytes for a recording physically live. A recording can have several
-- locations at once (e.g. recorder-local + storage-local, or local + cloud).
CREATE TABLE IF NOT EXISTS blob_locations (
    id           TEXT PRIMARY KEY,
    recording_id TEXT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
    tier         TEXT NOT NULL,              -- 'local' (hot) | 'cloud' (cold)
    store        TEXT NOT NULL,              -- node/backend label, or bucket/remote name
    key          TEXT NOT NULL,              -- absolute path (local) or object key (cloud)
    byte_size    INTEGER,
    checksum     TEXT,
    verified_at  REAL,
    created_at   REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_loc_unique    ON blob_locations(recording_id, tier, store);
CREATE INDEX        IF NOT EXISTS idx_loc_recording ON blob_locations(recording_id);

CREATE TABLE IF NOT EXISTS transcripts (
    recording_id TEXT PRIMARY KEY REFERENCES recordings(id) ON DELETE CASCADE,
    tier         TEXT,
    store        TEXT,
    key          TEXT,
    language     TEXT,
    model        TEXT,
    words        INTEGER,
    state        TEXT NOT NULL DEFAULT 'none',   -- none | pending | done
    updated_at   REAL NOT NULL
);

-- Explicit job model that replaces the bespoke poll/scan/reconcile loops.
-- shadow=1 means "record the intent only" — Phase 0/1 never executes jobs.
CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    kind         TEXT NOT NULL,              -- remux|transcribe|archive|verify|evict|gc
    recording_id TEXT REFERENCES recordings(id) ON DELETE CASCADE,
    node         TEXT,                       -- pin to a node, or NULL = any
    state        TEXT NOT NULL DEFAULT 'ready',  -- ready|running|done|failed|cancelled
    shadow       INTEGER NOT NULL DEFAULT 1,
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    run_after    REAL NOT NULL DEFAULT 0,
    progress     REAL,
    last_error   TEXT,
    claimed_by   TEXT,
    claimed_at   REAL,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_ready ON jobs(state, run_after);
CREATE INDEX IF NOT EXISTS idx_jobs_rec   ON jobs(kind, recording_id);

-- Full-text search index for transcripts. The control plane pulls each finished
-- .txt off storage once and indexes it here, so search is a single fast local
-- query instead of fan-out grep across every storage box (which also breaks when
-- a box is offline). Only `content` is tokenised/searchable; the rest is
-- UNINDEXED metadata kept for display + routing back to the file.
CREATE VIRTUAL TABLE IF NOT EXISTS transcript_fts USING fts5(
    filename UNINDEXED,
    creator  UNINDEXED,
    store    UNINDEXED,
    mtime    UNINDEXED,
    content,
    tokenize = 'unicode61'
);

-- Captured TikTok live chat logs (TK_<user>_<date_time>_chat.jsonl). Fuzzy-matched
-- to a recording by creator + nearest start time (the two start seconds/minutes
-- apart, so an exact filename match never works).
CREATE TABLE IF NOT EXISTS chat_logs (
    filename     TEXT PRIMARY KEY,
    creator      TEXT,
    store        TEXT,                       -- storage label that holds the log
    started_at   REAL,
    ended_at     REAL,
    events       INTEGER,
    comments     INTEGER,
    gifts        INTEGER,
    recording_id TEXT,                       -- fuzzy-matched recording, or NULL
    match_delta  REAL,                       -- |chat_start - rec_start| seconds
    indexed_at   REAL,                       -- when its text was folded into chat_fts
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chat_creator ON chat_logs(creator);
CREATE INDEX IF NOT EXISTS idx_chat_rec     ON chat_logs(recording_id);

-- Full-text search over chat comment/gift/nickname text, same idea as transcript_fts.
CREATE VIRTUAL TABLE IF NOT EXISTS chat_fts USING fts5(
    filename UNINDEXED,
    creator  UNINDEXED,
    store    UNINDEXED,
    content,
    tokenize = 'unicode61'
);
