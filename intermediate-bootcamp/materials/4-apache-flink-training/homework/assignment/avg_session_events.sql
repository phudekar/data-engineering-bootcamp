-- Average number of web events per session for Tech Creator
-- If “Tech Creator” = all hosts under techcreator.io:

-- Average events per session across Tech Creator
  SELECT
    AVG(num_hits)::numeric(10,2) AS avg_events_per_session
  FROM processed_events_aggregated_session
  WHERE host ILIKE '%.techcreator.io' OR host = 'techcreator.io';


  -- Average per user (IP) over their sessions on Tech Creator
  WITH per_user AS (
    SELECT ip, AVG(num_hits) AS avg_per_user_session
    FROM processed_events_aggregated_session
    WHERE host ILIKE '%.techcreator.io' OR host = 'techcreator.io'
    GROUP BY ip
  )
  SELECT AVG(avg_per_user_session)::numeric(10,2) AS avg_events_per_session_per_user
  FROM per_user;


  -- Comparison across the specified hosts
  SELECT
    host,
    AVG(num_hits)::numeric(10,2) AS avg_events_per_session,
    COUNT(*) AS sessions
  FROM processed_events_aggregated_session
  WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
  GROUP BY host
  ORDER BY host;