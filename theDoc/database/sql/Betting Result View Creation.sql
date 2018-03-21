
DROP VIEW IF EXISTS betting_results;

CREATE VIEW betting_results AS (

SELECT  
    bet.book_id, 
    pfx_game.gid, 
    away_name_full, 
    home_name_full, 
    pfx_game.game_date as game_date, 
    pfx_game.game_time_et, 
    pfx_miniscore.away_score, 
    pfx_miniscore.home_score, 
    pfx_miniscore.away_score + pfx_miniscore.home_score as total_score,
    pfx_miniscore.home_score - pfx_miniscore.away_score home_resultspread, 
    pfx_miniscore.away_score - pfx_miniscore.home_score away_resultspread, 
    pfx_miniscore.home_score - pfx_miniscore.away_score + bet.spread_home_points as homevsspread, 
    pfx_miniscore.away_score - pfx_miniscore.home_score + bet.spread_away_points as awayvsspread, 
    CASE 
        WHEN pfx_miniscore.home_score - pfx_miniscore.away_score + bet.spread_home_points > 0 THEN 'home' 
        WHEN pfx_miniscore.away_score - pfx_miniscore.home_score + bet.spread_away_points > 0 THEN 'away' 
        WHEN pfx_miniscore.home_score - pfx_miniscore.away_score + bet.spread_home_points = 0 THEN 'push' 
    END as spread_winner, 
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
    CASE 
        WHEN pfx_miniscore.home_score - pfx_miniscore.away_score > 0 THEN 'home' 
        WHEN pfx_miniscore.home_score - pfx_miniscore.away_score < 0 THEN 'away' 
        WHEN pfx_miniscore.home_score - pfx_miniscore.away_score = 0 THEN 'push' 
    END as money_winner, 
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
    pfx_miniscore.home_score + pfx_miniscore.away_score resulttotal, 
    bet.total_points, 
    pfx_miniscore.home_score + pfx_miniscore.away_score - total_points as total_diff, 
    CASE 
        WHEN pfx_miniscore.home_score + pfx_miniscore.away_score - total_points > 0 THEN 'over' 
        WHEN pfx_miniscore.home_score + pfx_miniscore.away_score - total_points < 0 THEN 'under' 
        WHEN pfx_miniscore.home_score + pfx_miniscore.away_score - total_points = 0 THEN 'push' 
    END as total_winner, 
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

    LEFT JOIN 
    pfx_miniscore
    ON pfx_miniscore.gid = pfx_game.gid 
    WHERE bet.book_id = 238 
    AND bet.most_recent = 'Y'
)
    ;
