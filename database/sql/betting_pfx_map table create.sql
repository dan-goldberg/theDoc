DROP TABLE IF EXISTS betting_pfx_map;
CREATE TABLE betting_pfx_map AS
SELECT
betting.betting_scrape_id,
game.gid
FROM
(
	SELECT b.betting_scrape_id, tba.name_translated as betting_away, tbh.name_translated as betting_home, DATE(b.game_date) as game_date_b, TIME(b.game_time_et) as game_time_et_b
	FROM betting_scrape as b
	LEFT JOIN teamname_translate as tba ON tba.source_table = 'betting_scrape' AND tba.name_original = b.team_away
	LEFT JOIN teamname_translate as tbh ON tbh.source_table = 'betting_scrape' AND tbh.name_original = b.team_home
) as betting
JOIN
(
	SELECT DISTINCT g.gid, tga.name_translated as game_away, tgh.name_translated as game_home, DATE(g.game_date) as game_date_g, TIME(g.game_time_et) as game_time_et_g
	FROM pfx_game as g
	LEFT JOIN teamname_translate as tga ON tga.source_table = 'pfx_game' AND tga.name_original = g.away_name_full
	LEFT JOIN teamname_translate as tgh ON tgh.source_table = 'pfx_game' AND tgh.name_original = g.home_name_full
) as game
ON 
betting.betting_away = game.game_away AND 
betting.betting_home = game.game_home AND
DATE(betting.game_date_b) = DATE(game.game_date_g)
AND TIME(betting.game_time_et_b) BETWEEN TIME(TIME(game.game_time_et_g) - interval '2' hour) AND TIME(TIME(game.game_time_et_g) + interval '2' hour)

;

delete from betting_scrape where date(createddate) = date(now())