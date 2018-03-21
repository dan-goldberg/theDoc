DROP TABLE IF EXISTS teamname_translate;
CREATE TABLE teamname_translate AS

SELECT
'pfx_game' as source_table,
home_name_full as name_original,
CASE
	WHEN home_name_full = 'Arizona D-backs' THEN 'Arizona Diamondbacks'
	WHEN home_name_full = 'Chi Cubs' THEN 'Chicago Cubs'
    WHEN home_name_full = 'Chi White Sox' THEN 'Chicago White Sox'
    WHEN home_name_full = 'Florida Marlins' THEN 'Miami Marlins'
    WHEN home_name_full = 'LA Angels' THEN 'Los Angeles Angels'
    WHEN home_name_full = 'LA Dodgers' THEN 'Los Angeles Dodgers'
    WHEN home_name_full = 'NY Mets' THEN 'New York Mets'
    WHEN home_name_full = 'NY Yankees' THEN 'New York Yankees'
    ELSE home_name_full
END as name_translated

FROM pfx_game
GROUP BY 1,2,3

UNION ALL

SELECT
'betting_scrape' as source_table,
team_home as name_original,
CASE
	WHEN team_home = 'CH Cubs' THEN 'Chicago Cubs'
    WHEN team_home = 'CH White Sox' THEN 'Chicago White Sox'
    WHEN team_home = 'L.A. Angels' THEN 'Los Angeles Angels'
    WHEN team_home = 'L.A. Dodgers' THEN 'Los Angeles Dodgers'
    WHEN team_home = 'N.Y. Mets' THEN 'New York Mets'
    WHEN team_home = 'N.Y. Yankees' THEN 'New York Yankees'
    ELSE team_home
END as name_translated

FROM betting_scrape
GROUP BY 1,2,3