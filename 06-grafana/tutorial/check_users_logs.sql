SELECT
  user,
  max(event_time) AS last_login,
  count() AS total_queries
FROM system.query_log
WHERE type = 'QueryStart'
GROUP BY user
ORDER BY last_login DESC;

-- А поскольку большинство современных компаний используют VPN для разрешения подключения,
-- то last_login всегда будет обновляться при фактическом подключении к БД.