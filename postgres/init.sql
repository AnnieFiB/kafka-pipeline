-- Initializes DB (events table etc.)

CREATE TABLE IF NOT EXISTS public.events_stream (
  source       text,
  value        int,
  category     text,
  ingested_at  bigint,
  processed_ts timestamp default now()
);
