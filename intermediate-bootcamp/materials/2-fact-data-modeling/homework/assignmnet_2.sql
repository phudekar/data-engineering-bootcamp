-- Table: public.game_details

-- DROP TABLE IF EXISTS public.game_details;
-- CREATE TABLE IF NOT EXISTS public.game_details
-- (
--     game_id integer,
--     team_id integer,
--     team_abbreviation text COLLATE pg_catalog."default",
--     team_city text COLLATE pg_catalog."default",
--     player_id integer,
--     player_name text COLLATE pg_catalog."default",
--     nickname text COLLATE pg_catalog."default",
--     start_position text COLLATE pg_catalog."default",
--     comment text COLLATE pg_catalog."default",
--     min text COLLATE pg_catalog."default",
--     fgm real,
--     fga real,
--     fg_pct real,
--     fg3m real,
--     fg3a real,
--     fg3_pct real,
--     ftm real,
--     fta real,
--     ft_pct real,
--     oreb real,
--     dreb real,
--     reb real,
--     ast real,
--     stl real,
--     blk real,
--     "TO" real,
--     pf real,
--     pts real,
--     plus_minus real,
--     CONSTRAINT game_details_game_id_fkey FOREIGN KEY (game_id)
--         REFERENCES public.games (game_id) MATCH SIMPLE
--         ON UPDATE NO ACTION
--         ON DELETE NO ACTION
-- )

-- A query to deduplicate `game_details` from Day 1 so there's no duplicates
-- Create a deduplicated version of game_details

-- DROP TABLE IF EXISTS public.game_details_dedup;

CREATE TABLE IF NOT EXISTS public.game_details_dedup AS
WITH ranked AS (
  SELECT
    gd.*,
    ROW_NUMBER() OVER (
      PARTITION BY gd.game_id, gd.team_id, gd.player_id
      ORDER BY
        (gd.min IS NOT NULL)::int DESC,
        NULLIF(
          make_interval(
            mins => split_part(gd.min, ':', 1)::NUMERIC::INT,
            secs => split_part(gd.min, ':', 2)::NUMERIC::INT
          ),
          NULL
        ) DESC,
        COALESCE(gd.plus_minus, -99999) DESC
    ) AS rn
  FROM public.game_details gd
)
SELECT *
FROM ranked
WHERE rn = 1;
-- Table: public.events

-- DROP TABLE IF EXISTS public.events;

-- CREATE TABLE IF NOT EXISTS public.events
-- (
--     url text COLLATE pg_catalog."default",
--     referrer text COLLATE pg_catalog."default",
--     user_id numeric,
--     device_id numeric,
--     host text COLLATE pg_catalog."default",
--     event_time text COLLATE pg_catalog."default"
-- )

-- Table: public.devices

-- DROP TABLE IF EXISTS public.devices;

-- CREATE TABLE IF NOT EXISTS public.devices
-- (
--     device_id numeric,
--     browser_type text COLLATE pg_catalog."default",
--     browser_version_major integer,
--     browser_version_minor integer,
--     browser_version_patch integer,
--     device_type text COLLATE pg_catalog."default",
--     device_version_major text COLLATE pg_catalog."default",
--     device_version_minor integer,
--     device_version_patch integer,
--     os_type text COLLATE pg_catalog."default",
--     os_version_major text COLLATE pg_catalog."default",
--     os_version_minor integer,
--     os_version_patch integer
-- )

-- Create A DDL for an `user_devices_cumulated` table that has:
--  - a `device_activity_datelist` which tracks a users active days by `browser_type`
--  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

-- DROP TABLE IF EXISTS public.user_devices_cumulated;

CREATE TABLE IF NOT EXISTS public.user_devices_cumulated (
    user_id numeric NOT NULL, -- Use BIGINT for user_id
    browser_type TEXT NOT NULL,
    device_activity_datelist DATE[] NOT NULL DEFAULT '{}', -- Default empty array
    PRIMARY KEY (user_id, browser_type)
);

-- Write a cumulative query to generate `device_activity_datelist` from `events`
-- Use COALESCE for user_id necessary to handle NULLs
INSERT INTO public.user_devices_cumulated (user_id, browser_type, device_activity_datelist)
SELECT
  COALESCE(e.user_id, 0) AS user_id,
  d.browser_type,
  ARRAY_AGG(DISTINCT DATE(e.event_time::timestamp)) AS device_activity_datelist
FROM public.events e
JOIN public.devices d
  ON e.device_id = d.device_id
GROUP BY COALESCE(e.user_id, 0), d.browser_type
ON CONFLICT (user_id, browser_type)
DO UPDATE
SET device_activity_datelist = (
  SELECT ARRAY_AGG(d ORDER BY d)
  FROM (
    SELECT DISTINCT d
    FROM unnest(public.user_devices_cumulated.device_activity_datelist
                || EXCLUDED.device_activity_datelist) AS t(d)
  ) s
);
-- Write a `datelist_int` generation query.
ALTER TABLE public.user_devices_cumulated
ADD COLUMN datelist_int INT;

-- Convert the `device_activity_datelist` column into a `datelist_int` column

-- Compute the bitmask for existing data
WITH exploded AS (
  SELECT
    u.user_id,
    u.browser_type,
    d::date AS activity_date,
    DATE_TRUNC('month', d)::date AS month_start,
    (EXTRACT(day FROM d)::int - 1) AS day_idx
  FROM public.user_devices_cumulated u
  CROSS JOIN LATERAL unnest(u.device_activity_datelist) AS t(d)
  WHERE (EXTRACT(day FROM d)::int - 1) < 31 -- Ensure day_idx is within range
),
bits AS (
  SELECT
    user_id,
    browser_type,
    SUM((1::int) << day_idx) AS datelist_int
  FROM exploded
  GROUP BY user_id, browser_type
)
UPDATE public.user_devices_cumulated u
SET datelist_int = b.datelist_int   
FROM bits b
WHERE u.user_id = b.user_id AND u.browser_type = b.browser_type;

-- Create a DDL for `hosts_cumulated` table
-- Create a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
-- DROP TABLE IF EXISTS public.hosts_cumulated;

CREATE TABLE IF NOT EXISTS public.hosts_cumulated (
    host TEXT NOT NULL,
    host_activity_datelist DATE[] NOT NULL DEFAULT '{}', -- Default empty array
    PRIMARY KEY (host)
);

-- Create an incremental query to generate `host_activity_datelist`
INSERT INTO public.hosts_cumulated (host, host_activity_datelist)
SELECT
  e.host,
  ARRAY_AGG(DISTINCT DATE(e.event_time::timestamp)) AS host_activity_datelist
FROM public.events e
GROUP BY e.host
ON CONFLICT (host)
DO UPDATE
SET host_activity_datelist = (
  SELECT ARRAY_AGG(d ORDER BY d)
  FROM (
    SELECT DISTINCT d
    FROM unnest(public.hosts_cumulated.host_activity_datelist
                || EXCLUDED.host_activity_datelist) AS t(d)
  ) s
);

-- Create a monthly, reduced fact table DDL `host_activity_reduced`
--  - month
--  - host
--  - hit_array - think COUNT(1)
--  - unique_visitors array -  think COUNT(DISTINCT user_id)
DROP TABLE IF EXISTS public.host_activity_reduced;

CREATE TABLE IF NOT EXISTS public.host_activity_reduced (
    month DATE NOT NULL, -- Start of the month
    host TEXT NOT NULL,
    hit_array INT[] NOT NULL, -- Array of daily hit counts
    unique_visitors INT[] NOT NULL, -- Array of daily unique visitor counts
    PRIMARY KEY (month, host)
);

-- Create an incremental query that loads `host_activity_reduced`
-- - day-by-day
WITH month_host AS (
  SELECT
    e.host,
    DATE_TRUNC('month', e.event_time::timestamp)::date AS month_start
  FROM public.events e
  GROUP BY 1, 2
),
calendar AS (
  SELECT
    mh.host,
    mh.month_start,
    gs::date AS day
  FROM month_host mh
  CROSS JOIN LATERAL generate_series(
    mh.month_start,
    (mh.month_start + INTERVAL '1 month - 1 day')::date,
    INTERVAL '1 day'
  ) AS gs
),
daily AS (
  SELECT
    e.host,
    DATE_TRUNC('month', e.event_time::timestamp)::date AS month_start,
    DATE(e.event_time::timestamp) AS day,
    COUNT(*) AS hits,
    COUNT(DISTINCT e.user_id) AS uv
  FROM public.events e
  GROUP BY 1, 2, 3
),
reduced AS (
  SELECT
    c.month_start AS month,
    c.host,
    ARRAY_AGG(COALESCE(d.hits, 0) ORDER BY c.day) AS hit_array,
    ARRAY_AGG(COALESCE(d.uv, 0) ORDER BY c.day) AS unique_visitors
  FROM calendar c
  LEFT JOIN daily d
    ON d.host = c.host AND d.month_start = c.month_start AND d.day = c.day
  GROUP BY 1, 2
)
INSERT INTO public.host_activity_reduced (month, host, hit_array, unique_visitors)
SELECT month, host, hit_array, unique_visitors
FROM reduced
ON CONFLICT (month, host)
DO UPDATE
SET hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;