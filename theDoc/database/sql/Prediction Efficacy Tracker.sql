SELECT
sum(bet_win) bets_won,
sum(1) bets,
sum(bet_win)/sum(1) bets_winp,
avg(CASE WHEN 1 = 1 THEN bet_payoutodds END) all_odds,
avg(CASE WHEN bet_win = 1 THEN bet_payoutodds END) win_odds,
sum(bet_win)/sum(1) * (avg(CASE WHEN 1 = 1 THEN bet_payoutodds END)) all_area,
sum(bet_win)/sum(1) * (avg(CASE WHEN bet_win = 1 THEN bet_payoutodds END)) win_area
FROM
(

SELECT 

subq.*,
CASE WHEN total_winner = bet THEN 1 ELSE 0 END bet_win

FROM

(

select preds.*, 
CASE WHEN preds.bet = 1 THEN probabilities1 WHEN preds.bet = 0 THEN probabilities0 END bet_probability,
CASE WHEN preds.bet = 1 THEN area1 WHEN preds.bet = 0 THEN area0 END bet_area,
CASE WHEN preds.bet = 1 THEN payoutodds1 WHEN preds.bet = 0 THEN payoutodds0 END bet_payoutodds,
br.away_score, 
br.home_score, 
br.total_score,
CASE WHEN br.money_winner = 'home' THEN 0 ELSE 1 END money_winner,
CASE WHEN br.spread_winner = 'home' THEN 0 ELSE 1 END spread_winner,
CASE WHEN br.total_winner = 'over' THEN 0 ELSE 1 END total_winner

from theDoc_logs_predstotal preds
JOIN betting_results br ON br.gid = preds.gid
WHERE br.away_score IS NOT NULL AND br.home_score IS NOT NULL
) as subq
WHERE subq.bet_area > 1.1
) as q
;
