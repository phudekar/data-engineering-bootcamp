-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);


  CREATE TABLE IF NOT EXISTS processed_events_aggregated_session (
    session_start TIMESTAMP(3) NOT NULL,
    session_end   TIMESTAMP(3) NOT NULL,
    host          TEXT NOT NULL,
    ip            TEXT NOT NULL,
    num_hits      BIGINT NOT NULL
  );