
-- Given the following database schema, write SQL queries to answer the following questions:
-- Table: public.player_seasons

--  CREATE TYPE season_stats AS (
--                          season Integer,
--                          pts REAL,
--                          ast REAL,
--                          reb REAL,
--                          weight INTEGER
--                        );

--  CREATE TYPE scoring_class AS
--      ENUM ('bad', 'average', 'good', 'star');


-- Table: public.players

-- DROP TABLE IF EXISTS public.players;

CREATE TABLE IF NOT EXISTS public.players
(
    player_name text COLLATE pg_catalog."default" NOT NULL,
    height text COLLATE pg_catalog."default",
    college text COLLATE pg_catalog."default",
    country text COLLATE pg_catalog."default",
    draft_year text COLLATE pg_catalog."default",
    draft_round text COLLATE pg_catalog."default",
    draft_number text COLLATE pg_catalog."default",
    seasons season_stats[],
    scoring_class scoring_class,
    years_since_last_active integer,
    is_active boolean,
    current_season integer NOT NULL,
    CONSTRAINT players_pkey PRIMARY KEY (player_name, current_season)
)


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

    -- Table: public.players_scd_table

-- DROP TABLE IF EXISTS public.players_scd_table;

CREATE TABLE IF NOT EXISTS public.players_scd_table
(
    player_name text COLLATE pg_catalog."default",
    scoring_class scoring_class,
    is_active boolean,
    start_season integer,
    end_date integer,
    current_season integer
)

--  Write a query that does state change tracking for `players`
--   - A player entering the league should be `New`
--   - A player leaving the league should be `Retired`
--   - A player staying in the league should be `Continued Playing`
--   - A player that comes out of retirement should be `Returned from Retirement`
--   - A player that stays out of the league should be `Stayed Retired`

-- Using players_scd_table to track state changes more efficiently
WITH player_seasons_extended AS (
    -- Create a complete timeline using players_scd_table and player_seasons
    SELECT 
        pst.player_name,
        pst.start_season,
        pst.end_date,
        pst.is_active,
        gs.season,
        CASE WHEN ps.player_name IS NOT NULL THEN TRUE ELSE FALSE END AS played_this_season
    FROM public.players_scd_table pst
    CROSS JOIN LATERAL generate_series(pst.start_season, pst.end_date) AS gs(season)
    LEFT JOIN public.player_seasons ps ON ps.player_name = pst.player_name AND ps.season = gs.season
),
state_transitions AS (
    SELECT 
        player_name,
        season,
        played_this_season,
        LAG(played_this_season) OVER (PARTITION BY player_name ORDER BY season) AS played_prev_season,
        ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY season) AS season_rank
    FROM player_seasons_extended
)
SELECT 
    player_name,
    season,
    CASE 
        WHEN season_rank = 1 AND played_this_season THEN 'New'
        WHEN played_this_season AND played_prev_season THEN 'Continued Playing'
        WHEN played_this_season AND NOT COALESCE(played_prev_season, FALSE) AND season_rank > 1 THEN 'Returned from Retirement'
        WHEN NOT played_this_season AND COALESCE(played_prev_season, FALSE) THEN 'Retired'
        WHEN NOT played_this_season AND NOT COALESCE(played_prev_season, TRUE) THEN 'Stayed Retired'
        ELSE 'Unknown'
    END AS player_state
FROM state_transitions
WHERE played_this_season OR played_prev_season IS NOT NULL
ORDER BY player_name, season;
  


-- - Write a query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
--   - Aggregate this dataset along the following dimensions
--     - player and team
--       - Answer questions like who scored the most points playing for one team?
--     - player and season
--       - Answer questions like who scored the most points in one season?
--     - team
--       - Answer questions like which team has won the most games?

-- COALACESCE to handle NULLs in GROUPING SETS

SELECT
  gd.player_name,
  gd.team_abbreviation,
  g.season,
  SUM(COALESCE(gd.pts, 0)) AS total_points
FROM public.game_details AS gd
JOIN public.games AS g ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
  (gd.player_name, gd.team_abbreviation),
  (gd.player_name, g.season),
  (gd.team_abbreviation)
)
ORDER BY total_points DESC;

-- Player who scored the most points playing for one team using Grouping sets
WITH pts_by_player_team AS (
  SELECT
    gd.player_name,
    gd.team_abbreviation,
    SUM(COALESCE(gd.pts,0)) AS total_points
  FROM public.game_details gd
  GROUP BY gd.player_name, gd.team_abbreviation
)
SELECT player_name, team_abbreviation, total_points
FROM pts_by_player_team
ORDER BY total_points DESC, player_name
LIMIT 1;

-- Player with the most points in a single season
WITH pts_by_player_season AS (
  SELECT
    gd.player_name,
    g.season,
    SUM(COALESCE(gd.pts,0)) AS total_points
  FROM public.game_details gd
  JOIN public.games g ON g.game_id = gd.game_id
  GROUP BY gd.player_name, g.season
)
SELECT player_name, season, total_points
FROM pts_by_player_season
ORDER BY total_points DESC, player_name
LIMIT 1;

-- Team with the most total wins
WITH team_games AS (
  SELECT
    g.game_id,
    g.game_date_est,
    g.team_id_home AS team_id,
    (g.home_team_wins = 1) AS is_win
  FROM public.games g
  UNION ALL
  SELECT
    g.game_id,
    g.game_date_est,
    g.team_id_away AS team_id,
    (g.home_team_wins = 0) AS is_win
  FROM public.games g
)
SELECT team_id,
       COUNT(*) FILTER (WHERE is_win) AS total_wins
FROM team_games
GROUP BY team_id
ORDER BY total_wins DESC
LIMIT 1;


-- Most games a team has won in a 90-game stretch

WITH team_games AS (
  SELECT
    g.game_id,
    g.game_date_est,
    g.team_id_home AS team_id,
    (g.home_team_wins = 1) AS is_win
  FROM public.games g
  UNION ALL
  SELECT
    g.game_id,
    g.game_date_est,
    g.team_id_away AS team_id,
    (g.home_team_wins = 0) AS is_win
  FROM public.games g
),
wins_by_game AS (
  SELECT
    team_id,
    game_id,
    game_date_est,
    CASE WHEN is_win THEN 1 ELSE 0 END AS win_flag
  FROM team_games
),
rolling AS (
  SELECT
    team_id,
    game_id,
    game_date_est,
    SUM(win_flag) OVER (
      PARTITION BY team_id
      ORDER BY game_date_est, game_id
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS wins_last_90
  FROM wins_by_game
)
SELECT team_id, MAX(wins_last_90) AS max_wins_in_any_90_game_stretch
FROM rolling
GROUP BY team_id
ORDER BY max_wins_in_any_90_game_stretch DESC
LIMIT 1;

-- Longest streak of games where LeBron scored > 10

WITH lebron AS (
  SELECT
    g.game_id,
    g.game_date_est,
    (gd.pts > 10) AS gt10
  FROM public.game_details gd
  JOIN public.games g ON g.game_id = gd.game_id
  WHERE gd.player_name = 'LeBron James'
),
tag AS (
  SELECT
    *,
    CASE WHEN gt10 THEN 0 ELSE 1 END AS is_break,
    SUM(CASE WHEN gt10 THEN 0 ELSE 1 END) OVER (ORDER BY game_date_est, game_id) AS grp
  FROM lebron
),
streaks AS (
  SELECT grp, COUNT(*) FILTER (WHERE gt10) AS streak_len
  FROM tag
  GROUP BY grp
)
SELECT MAX(streak_len) AS longest_gt10_streak
FROM streaks;
