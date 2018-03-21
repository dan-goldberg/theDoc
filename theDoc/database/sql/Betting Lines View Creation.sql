
DROP VIEW IF EXISTS betting_lines;

CREATE VIEW betting_lines AS (

SELECT  
    bet.book_id, 
    pfx_game.gid, 
    away_name_full, 
    home_name_full, 
    pfx_game.game_date as game_date, 
    pfx_game.game_time_et, 
    bet.spread_home_points, 
    bet.spread_home_odds, 
    CASE 
        WHEN bet.spread_home_odds > 0 THEN (bet.spread_home_odds / 100) + 1 
        WHEN bet.spread_home_odds < 0 THEN (100 / bet.spread_home_odds * -1) + 1 
        ELSE NULL 
    END as spread_home_oddsrat, 
    bet.spread_away_points, 
    bet.spread_away_odds, 
    CASE 
        WHEN bet.spread_away_odds > 0 THEN (bet.spread_away_odds / 100) + 1 
        WHEN bet.spread_away_odds < 0 THEN (100 / bet.spread_away_odds * -1) + 1 
        ELSE NULL 
    END as spread_away_oddsrat, 
    bet.money_home_odds, 
    CASE 
        WHEN bet.money_home_odds > 0 THEN (bet.money_home_odds / 100) + 1 
        WHEN bet.money_home_odds < 0 THEN (100 / bet.money_home_odds * -1) + 1 
        ELSE NULL 
    END as money_home_oddsrat, 
    bet.money_away_odds, 
    CASE 
        WHEN bet.money_away_odds > 0 THEN (bet.money_away_odds / 100) + 1 
        WHEN bet.money_away_odds < 0 THEN (100 / bet.money_away_odds * -1) + 1 
        ELSE NULL 
    END as money_away_oddsrat, 
    bet.total_points, 
    bet.total_over, 
    CASE 
        WHEN bet.total_over > 0 THEN (bet.total_over / 100) + 1 
        WHEN bet.total_over < 0 THEN (100 / bet.total_over * -1) + 1 
        ELSE NULL 
    END as total_over_oddsrat, 
    bet.total_under,
    CASE 
        WHEN bet.total_under > 0 THEN (bet.total_under / 100) + 1 
        WHEN bet.total_under < 0 THEN (100 / bet.total_under * -1) + 1 
        ELSE NULL 
    END as total_under_oddsrat
    FROM betting_pfx_map as map
    JOIN betting_scrape as bet ON bet.betting_scrape_id = map.betting_scrape_id
    JOIN pfx_game ON pfx_game.gid = map.gid
    
    WHERE bet.most_recent = 'Y'
)
    ;

