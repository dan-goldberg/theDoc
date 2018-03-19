select 
bet.book_id,
pfx_game.gid,
away_name_full, 
home_name_full, 
pfx_game.game_date as game_date,
time(pfx_game.game_time_et) + interval '12' hour as game_time,
pfx_prob.away_score,
pfx_prob.home_score,
pfx_prob.home_score - pfx_prob.away_score home_resultspread,
pfx_prob.away_score - pfx_prob.home_score away_resultspread,
CASE 
	WHEN pfx_prob.home_score - pfx_prob.away_score + bet.spread_home_points > 0 THEN 'home'
    WHEN pfx_prob.away_score - pfx_prob.home_score + bet.spread_away_points > 0 THEN 'away'
    WHEN pfx_prob.home_score - pfx_prob.away_score + bet.spread_home_points = 0 THEN 'push'
END as spread_winner,
bet.spread_home_points,
bet.spread_home_odds,
bet.spread_away_points,
bet.spread_away_odds,
CASE 
	WHEN pfx_prob.home_score - pfx_prob.away_score > 0 THEN 'home'
    WHEN pfx_prob.home_score - pfx_prob.away_score < 0 THEN 'away'
    WHEN pfx_prob.home_score - pfx_prob.away_score = 0 THEN 'push'
END as money_winner,
bet.money_home_odds,
bet.money_away_odds,
pfx_prob.home_score + pfx_prob.away_score resulttotal,
bet.total_points,
pfx_prob.home_score + pfx_prob.away_score - total_points as total_diff,
CASE
	WHEN pfx_prob.home_score + pfx_prob.away_score - total_points > 0 THEN 'over'
    WHEN pfx_prob.home_score + pfx_prob.away_score - total_points < 0 THEN 'under'
    WHEN pfx_prob.home_score + pfx_prob.away_score - total_points = 0 THEN 'push'
END as total_winner,
bet.total_over,
bet.total_under

from betting_scrape as bet
join
pfx_game
on replace(replace(replace(team_away, 'CH', 'Chicago'), 'L.A.', 'Los Angeles'), 'N.Y.', 'New York') = pfx_game.away_name_full
and replace(replace(replace(team_home, 'CH', 'Chicago'), 'L.A.', 'Los Angeles'), 'N.Y.', 'New York') = pfx_game.home_name_full
and date(bet.game_date) = date(pfx_game.game_date)
and 
	(time(time(bet.game_time) + interval '1' hour) = time(pfx_game.game_time_et)
	or
	time_format(time(pfx_game.game_time_et) + interval '12' hour, '%r') = time_format(time(bet.game_time) + interval '1' hour, '%r')
	)

left join
pfx_prob
on pfx_prob.gid = pfx_game.gid
where bet.book_id = 238
and year(pfx_game.game_date) >= 2011

;


#select * from betting_scrape limit 100;
#select * from pfx_game limit 100;
#select * from pfx_prob limit 100;