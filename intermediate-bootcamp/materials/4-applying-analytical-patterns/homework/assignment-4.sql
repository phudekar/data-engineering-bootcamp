
-- Given the following database schema, write SQL queries to answer the following questions:
-- Table: public.player_seasons

-- DROP TABLE IF EXISTS public.player_seasons;

CREATE TABLE IF NOT EXISTS public.player_seasons
(
    player_name text COLLATE pg_catalog."default" NOT NULL,
    age integer,
    height text COLLATE pg_catalog."default",
    weight integer,
    college text COLLATE pg_catalog."default",
    country text COLLATE pg_catalog."default",
    draft_year text COLLATE pg_catalog."default",
    draft_round text COLLATE pg_catalog."default",
    draft_number text COLLATE pg_catalog."default",
    gp real,
    pts real,
    reb real,
    ast real,
    netrtg real,
    oreb_pct real,
    dreb_pct real,
    usg_pct real,
    ts_pct real,
    ast_pct real,
    season integer NOT NULL,
    CONSTRAINT player_seasons_pkey PRIMARY KEY (player_name, season)
)

--  Write a query that does state change tracking for `players`
--   - A player entering the league should be `New`
--   - A player leaving the league should be `Retired`
--   - A player staying in the league should be `Continued Playing`
--   - A player that comes out of retirement should be `Returned from Retirement`
--   - A player that stays out of the league should be `Stayed Retired`

WITH player_states AS (
    SELECT
        ps.player_name,
        ps.season,
        LAG(ps.season) OVER (PARTITION BY ps.player_name ORDER BY ps.season) AS previous_season
    FROM
        public.player_seasons ps
)
SELECT
    ps.player_name,
    ps.season,
    CASE
        WHEN ps.previous_season IS NULL THEN 'New'
        WHEN ps.previous_season = ps.season - 1 THEN 'Continued Playing'
        WHEN ps.previous_season < ps.season - 1 THEN 'Returned from Retirement'
        WHEN ps.previous_season = ps.season + 1 THEN 'Retired'
        ELSE 'Stayed Retired'
    END AS player_state
FROM
    player_states ps
ORDER BY
    ps.player_name, ps.season;      


-- - Write a query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
--   - Aggregate this dataset along the following dimensions
--     - player and team
--       - Answer questions like who scored the most points playing for one team?
--     - player and season
--       - Answer questions like who scored the most points in one season?
--     - team
--       - Answer questions like which team has won the most games?

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

-- Table: public.games

-- DROP TABLE IF EXISTS public.games;

CREATE TABLE IF NOT EXISTS public.games
(
    game_date_est date,
    game_id integer NOT NULL,
    game_status_text text COLLATE pg_catalog."default",
    home_team_id integer,
    visitor_team_id integer,
    season integer,
    team_id_home integer,
    pts_home real,
    fg_pct_home real,
    ft_pct_home real,
    fg3_pct_home real,
    ast_home real,
    reb_home real,
    team_id_away integer,
    pts_away real,
    fg_pct_away real,
    ft_pct_away real,
    fg3_pct_away real,
    ast_away real,
    reb_away real,
    home_team_wins integer,
    CONSTRAINT games_pkey PRIMARY KEY (game_id)
)


SELECT
    gd.player_name,
    gd.team_abbreviation,
    g.season,
    SUM(gd.pts) AS total_points
FROM
    public.game_details gd
JOIN
    public.games g ON gd.game_id = g.game_id
GROUP BY
    GROUPING SETS (
        (gd.player_name, gd.team_abbreviation),
        (gd.player_name, g.season),
        (gd.team_abbreviation)
)
ORDER BY
    total_points DESC;


-- - Write a query that uses window functions on `game_details` to find out the following things:
--   - What is the most games a team has won in a 90 game stretch? 
--   - How many games in a row did LeBron James score over 10 points a game?
WITH team_wins AS (
    SELECT
        gd.team_abbreviation,
        COUNT(gd.game_id) AS total_wins,
        ROW_NUMBER() OVER (PARTITION BY gd.team_abbreviation ORDER BY g.season DESC) AS rn
    FROM
        public.game_details gd
    JOIN
        public.games g ON gd.game_id = g.game_id
    WHERE
        g.home_team_wins = 1 AND gd.team_id = g.team_id_home
    GROUP BY
        gd.team_abbreviation, g.season
),
lebron_streak AS (
    SELECT
        gd.player_name,
        COUNT(*) AS streak
    FROM
        public.game_details gd
    WHERE
        gd.player_name = 'LeBron James' AND gd.pts > 10
    GROUP BY
        gd.player_name
)
SELECT
    (SELECT MAX(total_wins) FROM team_wins) AS most_wins_in_90_game_stretch,
    (SELECT MAX(streak) FROM lebron_streak) AS lebron_streak