-- Set default schema to public
SET search_path TO public;

-- DDL for actor_films:
CREATE TABLE IF NOT EXISTS public.actor_films
(
    actor text COLLATE pg_catalog."default",
    actorid text COLLATE pg_catalog."default" NOT NULL,
    film text COLLATE pg_catalog."default",
    year integer,
    votes integer,
    rating real,
    filmid text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT actor_films_pkey PRIMARY KEY (actorid, filmid)
);

-- Drop tables before dropping type to avoid CASCADE
DROP TABLE IF EXISTS public.actors CASCADE;
DROP TABLE IF EXISTS public.actors_history_scd CASCADE;
DROP TYPE IF EXISTS film_info;

-- Create a composite type for film information
CREATE TYPE film_info AS (
    film VARCHAR(255),
    votes INT,
    rating real,
    filmid VARCHAR(255)
);

-- actors table: one row per actor, films from most recent year, PK on actorid
CREATE TABLE IF NOT EXISTS public.actors
(
    actorid text PRIMARY KEY,
    actor TEXT NOT NULL,
    films film_info[] NOT NULL,
    quality_class VARCHAR(10) NOT NULL CHECK (quality_class IN ('star', 'good', 'average', 'bad')),
    is_active BOOLEAN NOT NULL
);

-- Populate actors table: films from most recent year, quality_class from that year, is_active for current year
WITH latest_years AS (
    SELECT actorid, MAX(year) AS latest_year
    FROM public.actor_films
    GROUP BY actorid
),
films_latest AS (
    SELECT
        af.actorid,
        MAX(af.actor) AS actor,
        ARRAY_AGG(
            ROW(af.film, af.votes, af.rating, af.filmid)::film_info
            ORDER BY af.votes DESC NULLS LAST
        ) AS films,
        AVG(af.rating) AS avg_rating
    FROM public.actor_films af
    JOIN latest_years ly ON ly.actorid = af.actorid AND ly.latest_year = af.year
    GROUP BY af.actorid
)
INSERT INTO public.actors (actorid, actor, films, quality_class, is_active)
SELECT
    fl.actorid,
    fl.actor,
    fl.films,
    CASE
        WHEN fl.avg_rating > 8 THEN 'star'
        WHEN fl.avg_rating > 7 THEN 'good'
        WHEN fl.avg_rating > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    EXISTS (
        SELECT 1 FROM public.actor_films af2
        WHERE af2.actorid = fl.actorid
          AND af2.year = EXTRACT(YEAR FROM CURRENT_DATE)
    ) AS is_active
FROM films_latest fl;

-- actors_history_scd: type 2 SCD, year granularity, PK on (actorid, start_year)
-- If strict DATE granularity is required, use DATE columns and adjust logic accordingly.
CREATE TABLE IF NOT EXISTS public.actors_history_scd
(
    actorid text NOT NULL,
    actor TEXT NOT NULL,
    quality_class VARCHAR(10) CHECK (quality_class IN ('star', 'good', 'average', 'bad')),
    is_active BOOLEAN NOT NULL,
    start_date INTEGER NOT NULL,
    end_date INTEGER, -- NULL indicates current record
    CONSTRAINT actors_history_scd_pkey PRIMARY KEY (actorid, start_year)
);

-- SCD backfill: generate a continuous year series for each actor, track is_active and quality_class changes
-- Note: We use actor_films as the source because actors only holds the latest snapshot.
-- This is necessary to reconstruct history.
WITH bounds AS (
    SELECT actorid, MAX(actor) AS actor, MIN(year) AS min_y, GREATEST(MAX(year), EXTRACT(YEAR FROM CURRENT_DATE)::int) AS max_y
    FROM public.actor_films
    GROUP BY actorid
),
years AS (
    SELECT b.actorid, b.actor, y AS year
    FROM bounds b
    CROSS JOIN LATERAL generate_series(b.min_y, b.max_y) AS y
),
agg AS (
    SELECT actorid, year, AVG(rating) AS avg_rating, COUNT(*) AS cnt
    FROM public.actor_films
    GROUP BY actorid, year
),
classified AS (
    SELECT
        y.actorid,
        y.actor,
        y.year AS current_year,
        CASE
            WHEN a.avg_rating > 8 THEN 'star'
            WHEN a.avg_rating > 7 THEN 'good'
            WHEN a.avg_rating > 6 THEN 'average'
            WHEN a.avg_rating IS NULL THEN NULL
            ELSE 'bad'
        END AS quality_class,
        COALESCE(a.cnt > 0, FALSE) AS is_active
    FROM years y
    LEFT JOIN agg a USING (actorid, year)
),
streak_flags AS (
    SELECT
        *,
        CASE
            WHEN LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) IS DISTINCT FROM quality_class
              OR LAG(is_active)     OVER (PARTITION BY actorid ORDER BY current_year) IS DISTINCT FROM is_active
            THEN 1 ELSE 0
        END AS change_flag
    FROM classified
),
streaks AS (
    SELECT
        *,
        SUM(change_flag) OVER (PARTITION BY actorid ORDER BY current_year ROWS UNBOUNDED PRECEDING) AS grp_id
    FROM streak_flags
),
ranges AS (
    SELECT
        actorid,
        MAX(actor) AS actor,
        quality_class,
        is_active,
        MIN(current_year) AS start_year,
        MAX(current_year) AS end_year
    FROM streaks
    GROUP BY actorid, grp_id, quality_class, is_active
),
final AS (
    SELECT
        r.actorid,
        r.actor,
        r.quality_class,
        r.is_active,
        r.start_year,
        CASE WHEN r.end_year = MAX(r.end_year) OVER (PARTITION BY r.actorid)
             THEN NULL
             ELSE r.end_year
        END AS end_year
    FROM ranges r
)
INSERT INTO public.actors_history_scd (actorid, actor, quality_class, is_active, start_year, end_year)
SELECT actorid, actor, quality_class, is_active, start_year, end_year
FROM final
ORDER BY actorid, start_year;

-- Optional: Add indexes for SCD current record and range queries
-- CREATE INDEX IF NOT EXISTS idx_actors_history_scd_current ON public.actors_history_scd(actorid) WHERE end_year IS NULL;
-- CREATE INDEX IF NOT EXISTS idx_actors_history_scd_range ON public.actors_history_scd(actorid, start_year, end_year);