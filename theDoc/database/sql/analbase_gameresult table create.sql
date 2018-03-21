DROP TABLE IF EXISTS anal_battergame_result;

CREATE TABLE anal_battergame_result AS
SELECT
gid,
game_date,
batter,

count(ab_num) as pa,
sum(outsmade) as outsmade,
sum(runsscored) as rbi,
sum(_1b_fl) as _1b,
sum(_2b_fl) as _2b,
sum(_3b_fl) as _3b,
sum(_hr_fl) as _hr,
sum(_bb_fl) as _bb,
sum(_k_fl) as _k,
sum(_hbp_fl) as _hbp,
sum(runnerbasesadv) as runnerbasesadv,
sum(selfbasesadv) as selfbasesadv,
sum(totbasesadv) as totbasesadv,

sum(outsmade)/count(ab_num) as outsmade_pa,
sum(runsscored)/count(ab_num) as rbi_pa,
sum(_1b_fl)/count(ab_num) as _1b_pa,
sum(_2b_fl)/count(ab_num) as _2b_pa,
sum(_3b_fl)/count(ab_num) as _3b_pa,
sum(_hr_fl)/count(ab_num) as _hr_pa,
sum(_bb_fl)/count(ab_num) as _bb_pa,
sum(_k_fl)/count(ab_num) as _k_pa,
sum(_hbp_fl)/count(ab_num) as _hbp_pa,
sum(runnerbasesadv)/count(ab_num) as runnerbasesadv_pa,
sum(selfbasesadv)/count(ab_num) as selfbasesadv_pa,
sum(totbasesadv)/count(ab_num) as totbasesadv_pa

FROM 
analbase_atbat

GROUP BY 
gid, 
game_date,
batter
;


DROP TABLE IF EXISTS anal_pitchergame_result;

CREATE TABLE anal_pitchergame_result AS
SELECT
gid,
game_date,
pitcher,
sp_fl,

count(ab_num) as pa,
sum(outsmade) as outsmade,
sum(runsscored) as rbi,
sum(_1b_fl) as _1b,
sum(_2b_fl) as _2b,
sum(_3b_fl) as _3b,
sum(_hr_fl) as _hr,
sum(_bb_fl) as _bb,
sum(_k_fl) as _k,
sum(_hbp_fl) as _hbp,
sum(runnerbasesadv) as runnerbasesadv,
sum(selfbasesadv) as selfbasesadv,
sum(totbasesadv) as totbasesadv,

sum(outsmade)/count(ab_num) as outsmade_pa,
sum(runsscored)/count(ab_num) as rbi_pa,
sum(_1b_fl)/count(ab_num) as _1b_pa,
sum(_2b_fl)/count(ab_num) as _2b_pa,
sum(_3b_fl)/count(ab_num) as _3b_pa,
sum(_hr_fl)/count(ab_num) as _hr_pa,
sum(_bb_fl)/count(ab_num) as _bb_pa,
sum(_k_fl)/count(ab_num) as _k_pa,
sum(_hbp_fl)/count(ab_num) as _hbp_pa,
sum(runnerbasesadv)/count(ab_num) as runnerbasesadv_pa,
sum(selfbasesadv)/count(ab_num) as selfbasesadv_pa,
sum(totbasesadv)/count(ab_num) as totbasesadv_pa

FROM 
analbase_atbat

GROUP BY 
gid, 
game_date,
pitcher,
sp_fl
;

UPDATE anal_pitchergame_result
SET sp_fl =
CASE 
    WHEN rbi > 2 AND sp_fl IS NULL THEN 1
    WHEN rbi <= 2 AND sp_fl IS NULL THEN 0
    ELSE sp_fl
END;

select * from anal_battergame_result;
select * from anal_pitchergame_result;

select gid,count(CASE WHEN sp_fl = 1 THEN 1 END) from anal_pitchergame_result
group by gid
having count(CASE WHEN sp_fl = 1 THEN 1 END) > 2
;


SELECT 


select 
CASE WHEN p.side = 'away' THEN g.away_id WHEN p.side = 'home' THEN g.home_id END as team,
a.*,
p.side
FROM
analbase_atbat AS a 
LEFT JOIN
pfx_player AS p
ON
a.gid = p.gid AND a.batter = p.id_
LEFT JOIN
pfx_game AS g
ON g.gid = a.gid
where g.home_id is null
order by 2,1

select * from 
pfx_player
where gid IN
(
select 
a.gid
FROM
analbase_atbat AS a 
LEFT JOIN
pfx_player AS p
ON
a.gid = p.gid AND a.batter = p.id_
LEFT JOIN
pfx_game AS g
ON g.gid = a.gid
where g.home_id is null
)

select * from anal_batter_counting limit 1000


use mlb;
select * from anal_battergame_result
where game_date > '2015-01-01'


select * from betting_scrape;


select 
bgr.gid
,game.game_time_et
,stad_id
,bgr.game_date
,bgr.batter
,bgr.pa
,bgr.outsmade
,bgr.rbi
,bgr._1b
,bgr._2b
,bgr._3b
,bgr._hr
,bgr._bb
,bgr._k
,bgr._hbp
,bgr.runnerbasesadv
,bgr.selfbasesadv
,bgr.totbasesadv
from anal_battergame_result bgr
left join pfx_game as game
on game.gid = bgr.gid
where batter = 408047
and game_type = 'R'
order by game_date asc, gid asc

show processlist;
kill 2194;
kill 1608;
kill 1627