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
)

/*
DDL for actors table: Create a DDL for an actors table with the following fields:

films: An array of struct with the following fields:

film: The name of the film.
votes: The number of votes the film received.
rating: The rating of the film.
filmid: A unique identifier for each film.
quality_class: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:

star: Average rating > 8.
good: Average rating > 7 and ≤ 8.
average: Average rating > 6 and ≤ 7.
bad: Average rating ≤ 6.
is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).

*/

-- drop type if exists film_info cascade;
DROP TYPE IF EXISTS film_info CASCADE;

-- Create a composite type for film information
CREATE TYPE film_info AS (
    film VARCHAR(255),
    votes INT,
    rating FLOAT,
    filmid VARCHAR(255)
);

-- drop table if exists actors cascade;
DROP TABLE IF EXISTS public.actors CASCADE;

CREATE TABLE IF NOT EXISTS public.actors
(
    actorid text COLLATE pg_catalog."default" NOT NULL,
    actor TEXT COLLATE pg_catalog."default" NOT NULL,
    year INTEGER NOT NULL,
    films film_info[],  -- Array of composite type
    quality_class VARCHAR(10) CHECK (quality_class IN ('star', 'good', 'average', 'bad')),
    is_active BOOLEAN NOT NULL
);

-- Get all unique actors
WITH actors_list AS (
    SELECT DISTINCT actorid, actor
    FROM public.actor_films
),
years AS (
    SELECT DISTINCT year
    FROM public.actor_films
),
actor_years AS (
    SELECT a.actorid, a.actor, y.year
    FROM actors_list a
    CROSS JOIN years y
)

INSERT INTO public.actors (actorid, actor, year, films, quality_class, is_active)
SELECT
    ay.actorid,
    ay.actor,
    ay.year,
    COALESCE(
        ARRAY_AGG(
            ROW(af.film, af.votes, af.rating, af.filmid)::film_info
        ) FILTER (WHERE af.filmid IS NOT NULL),
        ARRAY[]::film_info[]
    ) AS films,
    CASE
        WHEN COUNT(af.rating) FILTER (WHERE af.rating IS NOT NULL) = 0 THEN 'bad'
        WHEN AVG(af.rating) > 8 THEN 'star'
        WHEN AVG(af.rating) > 7 THEN 'good'
        WHEN AVG(af.rating) > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    CASE
        WHEN COUNT(af.filmid) > 0 THEN TRUE
        ELSE FALSE
    END AS is_active
FROM
    actor_years ay
LEFT JOIN
    public.actor_films af
    ON ay.actorid = af.actorid AND ay.year = af.year
GROUP BY
    ay.actorid, ay.actor, ay.year;

/* Create a DDL for an `actors_history_scd` table with the following features:
    - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
    - Tracks `quality_class` and `is_active` status for each actor in the `actors` table.
    */
    
-- drop table if exists actors_history_scd cascade;
DROP TABLE IF EXISTS public.actors_history_scd CASCADE;

CREATE TABLE IF NOT EXISTS public.actors_history_scd
(
    actorid text COLLATE pg_catalog."default" NOT NULL,
    actor TEXT COLLATE pg_catalog."default" NOT NULL,
    quality_class VARCHAR(10) CHECK (quality_class IN ('star', 'good', 'average', 'bad')),
    is_active BOOLEAN NOT NULL,
    start_year INTEGER NOT NULL,
    end_year INTEGER, -- NULL indicates current record
    current_year INTEGER,
    CONSTRAINT actors_history_scd_pkey PRIMARY KEY (actorid, start_year)
);

-- Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query from the data in the 'actors' table
WITH streak_started AS (
    SELECT
        actorid,
        actor,
        year AS current_year,
        quality_class,
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year) <> quality_class
            OR LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY year) <> is_active
            OR LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year) IS NULL
            OR LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY year) IS NULL
            AS did_change
    FROM public.actors
),
streak_identified AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
            OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
    FROM streak_started
),
aggregated AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        streak_identifier,
        MIN(current_year) AS start_year,
        MAX(current_year) AS end_year
    FROM streak_identified
    GROUP BY actorid, actor, quality_class, is_active, streak_identifier
)
INSERT INTO public.actors_history_scd (actorid, actor, quality_class, is_active, start_year, end_year, current_year)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_year,
    end_year,
    end_year AS current_year
FROM aggregated
ORDER BY actorid, start_year;