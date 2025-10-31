-- Table: public.game_details

-- DROP TABLE IF EXISTS public.game_details;

CREATE TABLE IF NOT EXISTS public.game_details
(
    game_id integer,
    team_id integer,
    team_abbreviation text COLLATE pg_catalog."default",
    team_city text COLLATE pg_catalog."default",
    player_id integer,
    player_name text COLLATE pg_catalog."default",
    nickname text COLLATE pg_catalog."default",
    start_position text COLLATE pg_catalog."default",
    comment text COLLATE pg_catalog."default",
    min text COLLATE pg_catalog."default",
    fgm real,
    fga real,
    fg_pct real,
    fg3m real,
    fg3a real,
    fg3_pct real,
    ftm real,
    fta real,
    ft_pct real,
    oreb real,
    dreb real,
    reb real,
    ast real,
    stl real,
    blk real,
    "TO" real,
    pf real,
    pts real,
    plus_minus real,
    CONSTRAINT game_details_game_id_fkey FOREIGN KEY (game_id)
        REFERENCES public.games (game_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

# A query to deduplicate `game_details` from Day 1 so there's no duplicates
WITH dedup_game_details AS (
    SELECT DISTINCT ON (game_id, player_id) *
    FROM public.game_details
    ORDER BY game_id, player_id, min
)
SELECT count(*) FROM dedup_game_details;

-- Table: public.events

-- DROP TABLE IF EXISTS public.events;

CREATE TABLE IF NOT EXISTS public.events
(
    url text COLLATE pg_catalog."default",
    referrer text COLLATE pg_catalog."default",
    user_id numeric,
    device_id numeric,
    host text COLLATE pg_catalog."default",
    event_time text COLLATE pg_catalog."default"
)

-- Table: public.devices

-- DROP TABLE IF EXISTS public.devices;

CREATE TABLE IF NOT EXISTS public.devices
(
    device_id numeric,
    browser_type text COLLATE pg_catalog."default",
    browser_version_major integer,
    browser_version_minor integer,
    browser_version_patch integer,
    device_type text COLLATE pg_catalog."default",
    device_version_major text COLLATE pg_catalog."default",
    device_version_minor integer,
    device_version_patch integer,
    os_type text COLLATE pg_catalog."default",
    os_version_major text COLLATE pg_catalog."default",
    os_version_minor integer,
    os_version_patch integer
)

# Create A DDL for an `user_devices_cumulated` table that has:
#  - a `device_activity_datelist` which tracks a users active days by `browser_type`
#  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
#    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE IF NOT EXISTS public.user_devices_cumulated
(
    user_id NUMERIC NOT NULL, -- Unique identifier for the user
    browser_type TEXT NOT NULL, -- Browser type (e.g., Chrome, Firefox)
    device_activity_datelist DATE[] NOT NULL, -- Array of active dates for the user on this browser
    PRIMARY KEY (user_id, browser_type) -- Composite primary key to ensure uniqueness
)

-- Write a cumulative query to generate `device_activity_datelist` from `events`
-- Use COALESCE for user_id necessary to handle NULLs
INSERT INTO public.user_devices_cumulated (user_id, browser_type, device_activity_datelist)
SELECT
    COALESCE(e.user_id, 0) AS user_id,
    d.browser_type,
    COALESCE(ARRAY_AGG(DISTINCT DATE(e.event_time)), '{}') AS device_activity_datelist
FROM
    public.events e
JOIN
    public.devices d ON e.device_id = d.device_id
GROUP BY
    e.user_id,
    d.browser_type;

-- Create a `datelist_int` generation query. 
-- Convert the `device_activity_datelist` column into a `datelist_int` column 
ALTER TABLE public.user_devices_cumulated
ADD COLUMN datelist_int INT[];

UPDATE public.user_devices_cumulated
SET datelist_int = COALESCE(ARRAY(SELECT EXTRACT(EPOCH FROM unnest(device_activity_datelist))::INT), '{}')
WHERE device_activity_datelist IS NOT NULL;

-- Create a DDL for `hosts_cumulated` table 
-- Create a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
CREATE TABLE IF NOT EXISTS public.hosts_cumulated
(
    host TEXT NOT NULL, -- Unique identifier for the host
    host_activity_datelist DATE[] NOT NULL, -- Array of active dates for the host
    PRIMARY KEY (host) -- Primary key to ensure uniqueness
)

-- Create an incremental query to generate `host_activity_datelist`
INSERT INTO public.hosts_cumulated (host, host_activity_datelist)
SELECT
    e.host,
    COALESCE(ARRAY_AGG(DISTINCT DATE(e.event_time)), '{}') AS host_activity_datelist
FROM
    public.events e
GROUP BY
    e.host;

-- Create a `datelist_int` generation query for `hosts_cumulated` table
ALTER TABLE public.hosts_cumulated
ADD COLUMN datelist_int INT[];

UPDATE public.hosts_cumulated
SET datelist_int = COALESCE(ARRAY(SELECT EXTRACT(EPOCH FROM unnest(host_activity_datelist))::INT), '{}')
WHERE host_activity_datelist IS NOT NULL;

-- Create a monthly, reduced fact table DDL `host_activity_reduced`
--  - month
--  - host
--  - hit_array - think COUNT(1)
--  - unique_visitors array -  think COUNT(DISTINCT user_id)
CREATE TABLE IF NOT EXISTS public.host_activity_reduced
(
    month DATE NOT NULL, -- Month of activity
    host TEXT NOT NULL, -- Host identifier
    hit_array INT[] NOT NULL, -- Array of hit counts per day in the month
    unique_visitors INT[] NOT NULL, -- Array of unique visitor counts per day in the month
    PRIMARY KEY (month, host) -- Composite primary key to ensure uniqueness
)

-- Create an incremental query that loads `host_activity_reduced`
-- - day-by-day
INSERT INTO public.host_activity_reduced (month, host, hit_array, unique_visitors)
WITH daily_activity AS (
    SELECT
        DATE_TRUNC('month', DATE(e.event_time)) AS month,
        e.host,
        DATE(e.event_time) AS activity_date,
        COUNT(1) AS daily_hits,
        COUNT(DISTINCT e.user_id) AS daily_unique_visitors
    FROM
        public.events e
    GROUP BY
        DATE_TRUNC('month', DATE(e.event_time)),
        e.host,
        DATE(e.event_time)
)
SELECT
    month,
    host,
    ARRAY_AGG(daily_hits ORDER BY activity_date) AS hit_array,
    ARRAY_AGG(daily_unique_visitors ORDER BY activity_date) AS unique_visitors
FROM
    daily_activity
GROUP BY
    month,
    host;

