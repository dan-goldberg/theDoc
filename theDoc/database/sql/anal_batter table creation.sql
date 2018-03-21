select * from analbase_atbat limit 1000;


DROP TABLE IF EXISTS anal_batter;

SET 
@analdate := '2016-09-30'
;

#CREATE VIEW anal_batter_std AS

SELECT
@analdate as anal_game_date,
a.batter,
p.pname,

###########  STD  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std,
@pa := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as pa_std,
/*
@pa_out := COUNT(CASE WHEN @logic_std = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std,
@outsmade := SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as outsmade_std,
@hits := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std,
@onbases := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std,
@singles := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std,
@doubles := COUNT(CASE WHEN @logic_std = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std,
@triples := COUNT(CASE WHEN @logic_std = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std,
@homeruns := COUNT(CASE WHEN @logic_std = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std,
@walks := COUNT(CASE WHEN @logic_std = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std,
@k := COUNT(CASE WHEN @logic_std = 1 AND a._k_fl > 0 THEN 1 END) as k_std,
@klook := COUNT(CASE WHEN @logic_std = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std,
@kswing := COUNT(CASE WHEN @logic_std = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std,
@hbp := COUNT(CASE WHEN @logic_std = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std,
@groundballs := COUNT(CASE WHEN @logic_std = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std,
@linedrives := COUNT(CASE WHEN @logic_std = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std,
@runnerbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std,
@selfbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std,
@totbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std,
@hits_pa := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std,
@avg_ := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std,
@obp :=  COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std,
@sba_o := SUM(CASE WHEN @logic_std = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as sba_o_std,
@rba_pa := SUM(CASE WHEN @logic_std = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std,
@rba_o := SUM(CASE WHEN @logic_std = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as rba_o_std,
@tba_pa := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std,
@tba_o := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as tba_o_std,
@k_pa := COUNT(CASE WHEN @logic_std = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std,
@klook_pa := COUNT(CASE WHEN @logic_std = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std,
@kswing_pa := COUNT(CASE WHEN @logic_std = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std,
@walk_pa := COUNT(CASE WHEN @logic_std = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std,
@singles_pa := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std,
@doubles_pa := COUNT(CASE WHEN @logic_std = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std,
@triples_pa := COUNT(CASE WHEN @logic_std = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std,
@homeruns_pa := COUNT(CASE WHEN @logic_std = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std,
@groundballs_pa := COUNT(CASE WHEN @logic_std = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std,
@linedrives_pa := COUNT(CASE WHEN @logic_std = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std,

###########  last60  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.gid END) as gp_last60,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60,
/*
@pa_out := COUNT(CASE WHEN @logic_last60 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60,
@outsmade := SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60,
@hits := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60,
@onbases := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60,
@singles := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last60,
@doubles := COUNT(CASE WHEN @logic_last60 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last60,
@triples := COUNT(CASE WHEN @logic_last60 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last60,
@homeruns := COUNT(CASE WHEN @logic_last60 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last60,
@walks := COUNT(CASE WHEN @logic_last60 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last60,
@k := COUNT(CASE WHEN @logic_last60 = 1 AND a._k_fl > 0 THEN 1 END) as k_last60,
@klook := COUNT(CASE WHEN @logic_last60 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last60,
@kswing := COUNT(CASE WHEN @logic_last60 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last60,
@hbp := COUNT(CASE WHEN @logic_last60 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last60,
@groundballs := COUNT(CASE WHEN @logic_last60 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last60,
@linedrives := COUNT(CASE WHEN @logic_last60 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last60,
@runnerbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last60,
@selfbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last60,
@totbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last60,
@hits_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last60,
@avg_ := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last60 = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last60 = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last60,
@obp :=  COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last60 = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last60,
@sba_o := SUM(CASE WHEN @logic_last60 = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as sba_o_last60,
@rba_pa := SUM(CASE WHEN @logic_last60 = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last60,
@rba_o := SUM(CASE WHEN @logic_last60 = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as rba_o_last60,
@tba_pa := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last60,
@tba_o := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as tba_o_last60,
@k_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last60,
@klook_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last60,
@kswing_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last60,
@walk_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last60,
@singles_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last60,
@doubles_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last60,
@triples_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last60,
@homeruns_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last60,
@groundballs_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last60,
@linedrives_pa := COUNT(CASE WHEN @logic_last60 = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last60,

###########  last30  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30 := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate) = 1 THEN a.gid END) as gp_last30,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30,
/*
@pa_out := COUNT(CASE WHEN @logic_last30 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30,
@outsmade := SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30,
@hits := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30,
@onbases := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30,
@singles := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last30,
@doubles := COUNT(CASE WHEN @logic_last30 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last30,
@triples := COUNT(CASE WHEN @logic_last30 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last30,
@homeruns := COUNT(CASE WHEN @logic_last30 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last30,
@walks := COUNT(CASE WHEN @logic_last30 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last30,
@k := COUNT(CASE WHEN @logic_last30 = 1 AND a._k_fl > 0 THEN 1 END) as k_last30,
@klook := COUNT(CASE WHEN @logic_last30 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last30,
@kswing := COUNT(CASE WHEN @logic_last30 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last30,
@hbp := COUNT(CASE WHEN @logic_last30 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last30,
@groundballs := COUNT(CASE WHEN @logic_last30 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last30,
@linedrives := COUNT(CASE WHEN @logic_last30 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last30,
@runnerbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last30,
@selfbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last30,
@totbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last30,
@hits_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last30,
@avg_ := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last30 = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last30 = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last30,
@obp :=  COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last30 = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last30,
@sba_o := SUM(CASE WHEN @logic_last30 = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as sba_o_last30,
@rba_pa := SUM(CASE WHEN @logic_last30 = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last30,
@rba_o := SUM(CASE WHEN @logic_last30 = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as rba_o_last30,
@tba_pa := SUM(CASE WHEN @logic_last30 = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last30,
@tba_o := SUM(CASE WHEN @logic_last30 = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as tba_o_last30,
@k_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last30,
@klook_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last30,
@kswing_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last30,
@walk_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last30,
@singles_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last30,
@doubles_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last30,
@triples_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last30,
@homeruns_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last30,
@groundballs_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last30,
@linedrives_pa := COUNT(CASE WHEN @logic_last30 = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last30,

###########  last10  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10 := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate) = 1 THEN a.gid END) as gp_last10,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10,
/*
@pa_out := COUNT(CASE WHEN @logic_last10 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10,
@outsmade := SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10,
@hits := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10,
@onbases := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10,
@singles := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last10,
@doubles := COUNT(CASE WHEN @logic_last10 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last10,
@triples := COUNT(CASE WHEN @logic_last10 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last10,
@homeruns := COUNT(CASE WHEN @logic_last10 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last10,
@walks := COUNT(CASE WHEN @logic_last10 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last10,
@k := COUNT(CASE WHEN @logic_last10 = 1 AND a._k_fl > 0 THEN 1 END) as k_last10,
@klook := COUNT(CASE WHEN @logic_last10 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last10,
@kswing := COUNT(CASE WHEN @logic_last10 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last10,
@hbp := COUNT(CASE WHEN @logic_last10 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last10,
@groundballs := COUNT(CASE WHEN @logic_last10 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last10,
@linedrives := COUNT(CASE WHEN @logic_last10 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last10,
@runnerbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last10,
@selfbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last10,
@totbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last10,
@hits_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last10,
@avg_ := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last10 = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last10 = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last10,
@obp :=  COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last10 = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last10,
@sba_o := SUM(CASE WHEN @logic_last10 = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as sba_o_last10,
@rba_pa := SUM(CASE WHEN @logic_last10 = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last10,
@rba_o := SUM(CASE WHEN @logic_last10 = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as rba_o_last10,
@tba_pa := SUM(CASE WHEN @logic_last10 = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last10,
@tba_o := SUM(CASE WHEN @logic_last10 = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as tba_o_last10,
@k_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last10,
@klook_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last10,
@kswing_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last10,
@walk_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last10,
@singles_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last10,
@doubles_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last10,
@triples_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last10,
@homeruns_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last10,
@groundballs_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last10,
@linedrives_pa := COUNT(CASE WHEN @logic_last10 = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last10,

###########  std2  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std2 := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std2,
@pa := COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as pa_std2,
/*
@pa_out := COUNT(CASE WHEN @logic_std2 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std2,
@outsmade := SUM(CASE WHEN @logic_std2 = 1 THEN a.outsmade ELSE 0 END) as outsmade_std2,
@hits := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std2,
@onbases := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std2,
@singles := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std2,
@doubles := COUNT(CASE WHEN @logic_std2 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std2,
@triples := COUNT(CASE WHEN @logic_std2 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std2,
@homeruns := COUNT(CASE WHEN @logic_std2 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std2,
@walks := COUNT(CASE WHEN @logic_std2 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std2,
@k := COUNT(CASE WHEN @logic_std2 = 1 AND a._k_fl > 0 THEN 1 END) as k_std2,
@klook := COUNT(CASE WHEN @logic_std2 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std2,
@kswing := COUNT(CASE WHEN @logic_std2 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std2,
@hbp := COUNT(CASE WHEN @logic_std2 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std2,
@groundballs := COUNT(CASE WHEN @logic_std2 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std2,
@linedrives := COUNT(CASE WHEN @logic_std2 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std2,
@runnerbasesadv := SUM(CASE WHEN @logic_std2 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std2,
@selfbasesadv := SUM(CASE WHEN @logic_std2 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std2,
@totbasesadv := SUM(CASE WHEN @logic_std2 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std2,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std2 = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std2,
@hits_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std2,
@avg_ := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std2 = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std2 = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std2,
@obp :=  COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std2 = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std2,
@sba_o := SUM(CASE WHEN @logic_std2 = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2 = 1 THEN a.outsmade ELSE 0 END) as sba_o_std2,
@rba_pa := SUM(CASE WHEN @logic_std2 = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std2,
@rba_o := SUM(CASE WHEN @logic_std2 = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2 = 1 THEN a.outsmade ELSE 0 END) as rba_o_std2,
@tba_pa := SUM(CASE WHEN @logic_std2 = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std2,
@tba_o := SUM(CASE WHEN @logic_std2 = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2 = 1 THEN a.outsmade ELSE 0 END) as tba_o_std2,
@k_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std2,
@klook_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std2,
@kswing_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std2,
@walk_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std2,
@singles_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std2,
@doubles_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std2,
@triples_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std2,
@homeruns_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std2,
@groundballs_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std2,
@linedrives_pa := COUNT(CASE WHEN @logic_std2 = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std2,

###########  std3  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std3 := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std3,
@pa := COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as pa_std3,
/*
@pa_out := COUNT(CASE WHEN @logic_std3 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std3,
@outsmade := SUM(CASE WHEN @logic_std3 = 1 THEN a.outsmade ELSE 0 END) as outsmade_std3,
@hits := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std3,
@onbases := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std3,
@singles := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std3,
@doubles := COUNT(CASE WHEN @logic_std3 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std3,
@triples := COUNT(CASE WHEN @logic_std3 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std3,
@homeruns := COUNT(CASE WHEN @logic_std3 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std3,
@walks := COUNT(CASE WHEN @logic_std3 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std3,
@k := COUNT(CASE WHEN @logic_std3 = 1 AND a._k_fl > 0 THEN 1 END) as k_std3,
@klook := COUNT(CASE WHEN @logic_std3 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std3,
@kswing := COUNT(CASE WHEN @logic_std3 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std3,
@hbp := COUNT(CASE WHEN @logic_std3 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std3,
@groundballs := COUNT(CASE WHEN @logic_std3 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std3,
@linedrives := COUNT(CASE WHEN @logic_std3 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std3,
@runnerbasesadv := SUM(CASE WHEN @logic_std3 = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std3,
@selfbasesadv := SUM(CASE WHEN @logic_std3 = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std3,
@totbasesadv := SUM(CASE WHEN @logic_std3 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std3,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std3 = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std3,
@hits_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std3,
@avg_ := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std3 = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std3 = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std3,
@obp :=  COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std3 = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std3,
@sba_o := SUM(CASE WHEN @logic_std3 = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3 = 1 THEN a.outsmade ELSE 0 END) as sba_o_std3,
@rba_pa := SUM(CASE WHEN @logic_std3 = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std3,
@rba_o := SUM(CASE WHEN @logic_std3 = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3 = 1 THEN a.outsmade ELSE 0 END) as rba_o_std3,
@tba_pa := SUM(CASE WHEN @logic_std3 = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std3,
@tba_o := SUM(CASE WHEN @logic_std3 = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3 = 1 THEN a.outsmade ELSE 0 END) as tba_o_std3,
@k_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std3,
@klook_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std3,
@kswing_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std3,
@walk_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std3,
@singles_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std3,
@doubles_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std3,
@triples_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std3,
@homeruns_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std3,
@groundballs_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std3,
@linedrives_pa := COUNT(CASE WHEN @logic_std3 = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std3,

###########  std_rh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_rh := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_std_rh,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_rh,
/*
@pa_out := COUNT(CASE WHEN @logic_std_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_rh,
@outsmade := SUM(CASE WHEN @logic_std_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_rh,
@hits := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_rh,
@onbases := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_rh,
@singles := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std_rh,
@doubles := COUNT(CASE WHEN @logic_std_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std_rh,
@triples := COUNT(CASE WHEN @logic_std_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std_rh,
@homeruns := COUNT(CASE WHEN @logic_std_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std_rh,
@walks := COUNT(CASE WHEN @logic_std_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std_rh,
@k := COUNT(CASE WHEN @logic_std_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_std_rh,
@klook := COUNT(CASE WHEN @logic_std_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std_rh,
@kswing := COUNT(CASE WHEN @logic_std_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std_rh,
@hbp := COUNT(CASE WHEN @logic_std_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std_rh,
@groundballs := COUNT(CASE WHEN @logic_std_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std_rh,
@linedrives := COUNT(CASE WHEN @logic_std_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std_rh,
@runnerbasesadv := SUM(CASE WHEN @logic_std_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std_rh,
@selfbasesadv := SUM(CASE WHEN @logic_std_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std_rh,
@totbasesadv := SUM(CASE WHEN @logic_std_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_rh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std_rh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std_rh,
@hits_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std_rh,
@avg_ := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std_rh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std_rh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std_rh,
@obp :=  COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std_rh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std_rh,
@sba_o := SUM(CASE WHEN @logic_std_rh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std_rh = 1 THEN a.outsmade ELSE 0 END) as sba_o_std_rh,
@rba_pa := SUM(CASE WHEN @logic_std_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std_rh,
@rba_o := SUM(CASE WHEN @logic_std_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std_rh = 1 THEN a.outsmade ELSE 0 END) as rba_o_std_rh,
@tba_pa := SUM(CASE WHEN @logic_std_rh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std_rh,
@tba_o := SUM(CASE WHEN @logic_std_rh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std_rh = 1 THEN a.outsmade ELSE 0 END) as tba_o_std_rh,
@k_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std_rh,
@klook_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std_rh,
@kswing_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std_rh,
@walk_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std_rh,
@singles_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std_rh,
@doubles_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std_rh,
@triples_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std_rh,
@homeruns_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std_rh,
@groundballs_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std_rh,
@linedrives_pa := COUNT(CASE WHEN @logic_std_rh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_rh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std_rh,

###########  last60_rh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60_rh := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_last60_rh,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_rh,
/*
@pa_out := COUNT(CASE WHEN @logic_last60_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_rh,
@outsmade := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_rh,
@hits := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_rh,
@onbases := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_rh,
@singles := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last60_rh,
@doubles := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last60_rh,
@triples := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last60_rh,
@homeruns := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last60_rh,
@walks := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last60_rh,
@k := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_last60_rh,
@klook := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last60_rh,
@kswing := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last60_rh,
@hbp := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last60_rh,
@groundballs := COUNT(CASE WHEN @logic_last60_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last60_rh,
@linedrives := COUNT(CASE WHEN @logic_last60_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last60_rh,
@runnerbasesadv := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last60_rh,
@selfbasesadv := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last60_rh,
@totbasesadv := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_rh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last60_rh,
@hits_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last60_rh,
@avg_ := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last60_rh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last60_rh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last60_rh,
@obp :=  COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last60_rh,
@sba_o := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60_rh = 1 THEN a.outsmade ELSE 0 END) as sba_o_last60_rh,
@rba_pa := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last60_rh,
@rba_o := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60_rh = 1 THEN a.outsmade ELSE 0 END) as rba_o_last60_rh,
@tba_pa := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last60_rh,
@tba_o := SUM(CASE WHEN @logic_last60_rh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60_rh = 1 THEN a.outsmade ELSE 0 END) as tba_o_last60_rh,
@k_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last60_rh,
@klook_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last60_rh,
@kswing_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last60_rh,
@walk_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last60_rh,
@singles_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last60_rh,
@doubles_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last60_rh,
@triples_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last60_rh,
@homeruns_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last60_rh,
@groundballs_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last60_rh,
@linedrives_pa := COUNT(CASE WHEN @logic_last60_rh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_rh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last60_rh,

###########  last30_rh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30_rh := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_last30_rh,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_rh,
/*
@pa_out := COUNT(CASE WHEN @logic_last30_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_rh,
@outsmade := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_rh,
@hits := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_rh,
@onbases := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_rh,
@singles := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last30_rh,
@doubles := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last30_rh,
@triples := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last30_rh,
@homeruns := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last30_rh,
@walks := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last30_rh,
@k := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_last30_rh,
@klook := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last30_rh,
@kswing := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last30_rh,
@hbp := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last30_rh,
@groundballs := COUNT(CASE WHEN @logic_last30_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last30_rh,
@linedrives := COUNT(CASE WHEN @logic_last30_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last30_rh,
@runnerbasesadv := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last30_rh,
@selfbasesadv := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last30_rh,
@totbasesadv := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_rh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last30_rh,
@hits_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last30_rh,
@avg_ := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last30_rh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last30_rh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last30_rh,
@obp :=  COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last30_rh,
@sba_o := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30_rh = 1 THEN a.outsmade ELSE 0 END) as sba_o_last30_rh,
@rba_pa := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last30_rh,
@rba_o := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30_rh = 1 THEN a.outsmade ELSE 0 END) as rba_o_last30_rh,
@tba_pa := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last30_rh,
@tba_o := SUM(CASE WHEN @logic_last30_rh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30_rh = 1 THEN a.outsmade ELSE 0 END) as tba_o_last30_rh,
@k_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last30_rh,
@klook_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last30_rh,
@kswing_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last30_rh,
@walk_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last30_rh,
@singles_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last30_rh,
@doubles_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last30_rh,
@triples_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last30_rh,
@homeruns_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last30_rh,
@groundballs_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last30_rh,
@linedrives_pa := COUNT(CASE WHEN @logic_last30_rh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_rh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last30_rh,

###########  last10_rh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10_rh := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_last10_rh,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_rh,
/*
@pa_out := COUNT(CASE WHEN @logic_last10_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_rh,
@outsmade := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_rh,
@hits := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_rh,
@onbases := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_rh,
@singles := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last10_rh,
@doubles := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last10_rh,
@triples := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last10_rh,
@homeruns := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last10_rh,
@walks := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last10_rh,
@k := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_last10_rh,
@klook := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last10_rh,
@kswing := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last10_rh,
@hbp := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last10_rh,
@groundballs := COUNT(CASE WHEN @logic_last10_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last10_rh,
@linedrives := COUNT(CASE WHEN @logic_last10_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last10_rh,
@runnerbasesadv := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last10_rh,
@selfbasesadv := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last10_rh,
@totbasesadv := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_rh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last10_rh,
@hits_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last10_rh,
@avg_ := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last10_rh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last10_rh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last10_rh,
@obp :=  COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last10_rh,
@sba_o := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10_rh = 1 THEN a.outsmade ELSE 0 END) as sba_o_last10_rh,
@rba_pa := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last10_rh,
@rba_o := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10_rh = 1 THEN a.outsmade ELSE 0 END) as rba_o_last10_rh,
@tba_pa := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last10_rh,
@tba_o := SUM(CASE WHEN @logic_last10_rh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10_rh = 1 THEN a.outsmade ELSE 0 END) as tba_o_last10_rh,
@k_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last10_rh,
@klook_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last10_rh,
@kswing_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last10_rh,
@walk_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last10_rh,
@singles_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last10_rh,
@doubles_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last10_rh,
@triples_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last10_rh,
@homeruns_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last10_rh,
@groundballs_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last10_rh,
@linedrives_pa := COUNT(CASE WHEN @logic_last10_rh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_rh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last10_rh,

###########  std2_rh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std2_rh := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_std2_rh,
@pa := COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as pa_std2_rh,
/*
@pa_out := COUNT(CASE WHEN @logic_std2_rh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std2_rh,
@outsmade := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.outsmade ELSE 0 END) as outsmade_std2_rh,
@hits := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std2_rh,
@onbases := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std2_rh,
@singles := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std2_rh,
@doubles := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std2_rh,
@triples := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std2_rh,
@homeruns := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std2_rh,
@walks := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std2_rh,
@k := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._k_fl > 0 THEN 1 END) as k_std2_rh,
@klook := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std2_rh,
@kswing := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std2_rh,
@hbp := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std2_rh,
@groundballs := COUNT(CASE WHEN @logic_std2_rh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std2_rh,
@linedrives := COUNT(CASE WHEN @logic_std2_rh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std2_rh,
@runnerbasesadv := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std2_rh,
@selfbasesadv := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std2_rh,
@totbasesadv := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std2_rh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std2_rh,
@hits_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std2_rh,
@avg_ := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std2_rh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std2_rh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std2_rh,
@obp :=  COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std2_rh,
@sba_o := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2_rh = 1 THEN a.outsmade ELSE 0 END) as sba_o_std2_rh,
@rba_pa := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std2_rh,
@rba_o := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2_rh = 1 THEN a.outsmade ELSE 0 END) as rba_o_std2_rh,
@tba_pa := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std2_rh,
@tba_o := SUM(CASE WHEN @logic_std2_rh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2_rh = 1 THEN a.outsmade ELSE 0 END) as tba_o_std2_rh,
@k_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std2_rh,
@klook_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std2_rh,
@kswing_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std2_rh,
@walk_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std2_rh,
@singles_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std2_rh,
@doubles_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std2_rh,
@triples_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std2_rh,
@homeruns_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std2_rh,
@groundballs_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std2_rh,
@linedrives_pa := COUNT(CASE WHEN @logic_std2_rh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_rh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std2_rh,

###########  std3_rh   ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std3_rh  := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate AND a.p_throws = 'R') = 1 THEN a.gid END) as gp_std3_rh ,
@pa := COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as pa_std3_rh ,
/*
@pa_out := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std3_rh ,
@outsmade := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.outsmade ELSE 0 END) as outsmade_std3_rh ,
@hits := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std3_rh ,
@onbases := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std3_rh ,
@singles := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std3_rh ,
@doubles := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std3_rh ,
@triples := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std3_rh ,
@homeruns := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std3_rh ,
@walks := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std3_rh ,
@k := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._k_fl > 0 THEN 1 END) as k_std3_rh ,
@klook := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std3_rh ,
@kswing := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std3_rh ,
@hbp := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std3_rh ,
@groundballs := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std3_rh ,
@linedrives := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std3_rh ,
@runnerbasesadv := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std3_rh ,
@selfbasesadv := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std3_rh ,
@totbasesadv := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std3_rh ,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std3_rh ,
@hits_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std3_rh ,
@avg_ := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std3_rh ,
@obp :=  COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std3_rh ,
@sba_o := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.outsmade ELSE 0 END) as sba_o_std3_rh ,
@rba_pa := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std3_rh ,
@rba_o := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.outsmade ELSE 0 END) as rba_o_std3_rh ,
@tba_pa := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std3_rh ,
@tba_o := SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3_rh  = 1 THEN a.outsmade ELSE 0 END) as tba_o_std3_rh ,
@k_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std3_rh ,
@klook_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std3_rh ,
@kswing_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std3_rh ,
@walk_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std3_rh ,
@singles_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std3_rh ,
@doubles_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std3_rh ,
@triples_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std3_rh ,
@homeruns_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std3_rh ,
@groundballs_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std3_rh ,
@linedrives_pa := COUNT(CASE WHEN @logic_std3_rh  = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_rh  = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std3_rh, 

###########  std_lh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_lh := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_std_lh,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_lh,
/*
@pa_out := COUNT(CASE WHEN @logic_std_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_lh,
@outsmade := SUM(CASE WHEN @logic_std_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_lh,
@hits := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_lh,
@onbases := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_lh,
@singles := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std_lh,
@doubles := COUNT(CASE WHEN @logic_std_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std_lh,
@triples := COUNT(CASE WHEN @logic_std_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std_lh,
@homeruns := COUNT(CASE WHEN @logic_std_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std_lh,
@walks := COUNT(CASE WHEN @logic_std_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std_lh,
@k := COUNT(CASE WHEN @logic_std_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_std_lh,
@klook := COUNT(CASE WHEN @logic_std_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std_lh,
@kswing := COUNT(CASE WHEN @logic_std_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std_lh,
@hbp := COUNT(CASE WHEN @logic_std_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std_lh,
@groundballs := COUNT(CASE WHEN @logic_std_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std_lh,
@linedrives := COUNT(CASE WHEN @logic_std_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std_lh,
@runnerbasesadv := SUM(CASE WHEN @logic_std_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std_lh,
@selfbasesadv := SUM(CASE WHEN @logic_std_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std_lh,
@totbasesadv := SUM(CASE WHEN @logic_std_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_lh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std_lh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std_lh,
@hits_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std_lh,
@avg_ := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std_lh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std_lh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std_lh,
@obp :=  COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std_lh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std_lh,
@sba_o := SUM(CASE WHEN @logic_std_lh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std_lh = 1 THEN a.outsmade ELSE 0 END) as sba_o_std_lh,
@rba_pa := SUM(CASE WHEN @logic_std_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std_lh,
@rba_o := SUM(CASE WHEN @logic_std_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std_lh = 1 THEN a.outsmade ELSE 0 END) as rba_o_std_lh,
@tba_pa := SUM(CASE WHEN @logic_std_lh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std_lh,
@tba_o := SUM(CASE WHEN @logic_std_lh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std_lh = 1 THEN a.outsmade ELSE 0 END) as tba_o_std_lh,
@k_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std_lh,
@klook_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std_lh,
@kswing_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std_lh,
@walk_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std_lh,
@singles_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std_lh,
@doubles_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std_lh,
@triples_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std_lh,
@homeruns_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std_lh,
@groundballs_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std_lh,
@linedrives_pa := COUNT(CASE WHEN @logic_std_lh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std_lh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std_lh,

###########  last60_lh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60_lh := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_last60_lh,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_lh,
/*
@pa_out := COUNT(CASE WHEN @logic_last60_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_lh,
@outsmade := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_lh,
@hits := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_lh,
@onbases := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_lh,
@singles := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last60_lh,
@doubles := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last60_lh,
@triples := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last60_lh,
@homeruns := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last60_lh,
@walks := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last60_lh,
@k := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_last60_lh,
@klook := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last60_lh,
@kswing := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last60_lh,
@hbp := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last60_lh,
@groundballs := COUNT(CASE WHEN @logic_last60_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last60_lh,
@linedrives := COUNT(CASE WHEN @logic_last60_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last60_lh,
@runnerbasesadv := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last60_lh,
@selfbasesadv := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last60_lh,
@totbasesadv := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_lh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last60_lh,
@hits_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last60_lh,
@avg_ := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last60_lh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last60_lh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last60_lh,
@obp :=  COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last60_lh,
@sba_o := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60_lh = 1 THEN a.outsmade ELSE 0 END) as sba_o_last60_lh,
@rba_pa := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last60_lh,
@rba_o := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60_lh = 1 THEN a.outsmade ELSE 0 END) as rba_o_last60_lh,
@tba_pa := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last60_lh,
@tba_o := SUM(CASE WHEN @logic_last60_lh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last60_lh = 1 THEN a.outsmade ELSE 0 END) as tba_o_last60_lh,
@k_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last60_lh,
@klook_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last60_lh,
@kswing_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last60_lh,
@walk_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last60_lh,
@singles_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last60_lh,
@doubles_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last60_lh,
@triples_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last60_lh,
@homeruns_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last60_lh,
@groundballs_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last60_lh,
@linedrives_pa := COUNT(CASE WHEN @logic_last60_lh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last60_lh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last60_lh,

###########  last30_lh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30_lh := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_last30_lh,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_lh,
/*
@pa_out := COUNT(CASE WHEN @logic_last30_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_lh,
@outsmade := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_lh,
@hits := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_lh,
@onbases := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_lh,
@singles := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last30_lh,
@doubles := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last30_lh,
@triples := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last30_lh,
@homeruns := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last30_lh,
@walks := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last30_lh,
@k := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_last30_lh,
@klook := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last30_lh,
@kswing := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last30_lh,
@hbp := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last30_lh,
@groundballs := COUNT(CASE WHEN @logic_last30_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last30_lh,
@linedrives := COUNT(CASE WHEN @logic_last30_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last30_lh,
@runnerbasesadv := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last30_lh,
@selfbasesadv := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last30_lh,
@totbasesadv := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_lh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last30_lh,
@hits_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last30_lh,
@avg_ := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last30_lh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last30_lh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last30_lh,
@obp :=  COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last30_lh,
@sba_o := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30_lh = 1 THEN a.outsmade ELSE 0 END) as sba_o_last30_lh,
@rba_pa := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last30_lh,
@rba_o := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30_lh = 1 THEN a.outsmade ELSE 0 END) as rba_o_last30_lh,
@tba_pa := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last30_lh,
@tba_o := SUM(CASE WHEN @logic_last30_lh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last30_lh = 1 THEN a.outsmade ELSE 0 END) as tba_o_last30_lh,
@k_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last30_lh,
@klook_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last30_lh,
@kswing_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last30_lh,
@walk_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last30_lh,
@singles_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last30_lh,
@doubles_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last30_lh,
@triples_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last30_lh,
@homeruns_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last30_lh,
@groundballs_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last30_lh,
@linedrives_pa := COUNT(CASE WHEN @logic_last30_lh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last30_lh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last30_lh,

###########  last10_lh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10_lh := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_last10_lh,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_lh,
/*
@pa_out := COUNT(CASE WHEN @logic_last10_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_lh,
@outsmade := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_lh,
@hits := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_lh,
@onbases := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_lh,
@singles := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last10_lh,
@doubles := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last10_lh,
@triples := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last10_lh,
@homeruns := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last10_lh,
@walks := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last10_lh,
@k := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_last10_lh,
@klook := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last10_lh,
@kswing := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last10_lh,
@hbp := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last10_lh,
@groundballs := COUNT(CASE WHEN @logic_last10_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last10_lh,
@linedrives := COUNT(CASE WHEN @logic_last10_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last10_lh,
@runnerbasesadv := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_last10_lh,
@selfbasesadv := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_last10_lh,
@totbasesadv := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_lh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_last10_lh,
@hits_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_last10_lh,
@avg_ := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_last10_lh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_last10_lh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_last10_lh,
@obp :=  COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_last10_lh,
@sba_o := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10_lh = 1 THEN a.outsmade ELSE 0 END) as sba_o_last10_lh,
@rba_pa := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_last10_lh,
@rba_o := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10_lh = 1 THEN a.outsmade ELSE 0 END) as rba_o_last10_lh,
@tba_pa := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_last10_lh,
@tba_o := SUM(CASE WHEN @logic_last10_lh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_last10_lh = 1 THEN a.outsmade ELSE 0 END) as tba_o_last10_lh,
@k_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_last10_lh,
@klook_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_last10_lh,
@kswing_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_last10_lh,
@walk_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_last10_lh,
@singles_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_last10_lh,
@doubles_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_last10_lh,
@triples_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_last10_lh,
@homeruns_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_last10_lh,
@groundballs_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_last10_lh,
@linedrives_pa := COUNT(CASE WHEN @logic_last10_lh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_last10_lh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_last10_lh,

###########  std2_lh  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std2_lh := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_std2_lh,
@pa := COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as pa_std2_lh,
/*
@pa_out := COUNT(CASE WHEN @logic_std2_lh = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std2_lh,
@outsmade := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.outsmade ELSE 0 END) as outsmade_std2_lh,
@hits := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std2_lh,
@onbases := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std2_lh,
@singles := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std2_lh,
@doubles := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std2_lh,
@triples := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std2_lh,
@homeruns := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std2_lh,
@walks := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std2_lh,
@k := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._k_fl > 0 THEN 1 END) as k_std2_lh,
@klook := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std2_lh,
@kswing := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std2_lh,
@hbp := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std2_lh,
@groundballs := COUNT(CASE WHEN @logic_std2_lh = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std2_lh,
@linedrives := COUNT(CASE WHEN @logic_std2_lh = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std2_lh,
@runnerbasesadv := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std2_lh,
@selfbasesadv := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std2_lh,
@totbasesadv := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std2_lh,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std2_lh,
@hits_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std2_lh,
@avg_ := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std2_lh = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std2_lh = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std2_lh,
@obp :=  COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std2_lh,
@sba_o := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2_lh = 1 THEN a.outsmade ELSE 0 END) as sba_o_std2_lh,
@rba_pa := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std2_lh,
@rba_o := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2_lh = 1 THEN a.outsmade ELSE 0 END) as rba_o_std2_lh,
@tba_pa := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std2_lh,
@tba_o := SUM(CASE WHEN @logic_std2_lh = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std2_lh = 1 THEN a.outsmade ELSE 0 END) as tba_o_std2_lh,
@k_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std2_lh,
@klook_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std2_lh,
@kswing_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std2_lh,
@walk_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std2_lh,
@singles_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std2_lh,
@doubles_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std2_lh,
@triples_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std2_lh,
@homeruns_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std2_lh,
@groundballs_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std2_lh,
@linedrives_pa := COUNT(CASE WHEN @logic_std2_lh = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std2_lh = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std2_lh,

###########  std3_lh   ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std3_lh  := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate AND a.p_throws = 'L') = 1 THEN a.gid END) as gp_std3_lh ,
@pa := COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as pa_std3_lh ,
/*
@pa_out := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std3_lh ,
@outsmade := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.outsmade ELSE 0 END) as outsmade_std3_lh ,
@hits := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std3_lh ,
@onbases := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std3_lh ,
@singles := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl > 0 THEN 1 END) as singles_std3_lh ,
@doubles := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_std3_lh ,
@triples := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._3b_fl > 0 THEN 1 END) as triples_std3_lh ,
@homeruns := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_std3_lh ,
@walks := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._bb_fl > 0 THEN 1 END) as walks_std3_lh ,
@k := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._k_fl > 0 THEN 1 END) as k_std3_lh ,
@klook := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._klook_fl > 0 THEN 1 END) as klook_std3_lh ,
@kswing := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_std3_lh ,
@hbp := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_std3_lh ,
@groundballs := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_std3_lh ,
@linedrives := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_std3_lh ,
@runnerbasesadv := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.runnerbasesadv ELSE 0 END) as runnerbasesadv_std3_lh ,
@selfbasesadv := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.selfbasesadv ELSE 0 END) as selfbasesadv_std3_lh ,
@totbasesadv := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std3_lh ,
*/
-- RATE STATS 

@outsmade_pa := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.outsmade ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as outsmade_pa_std3_lh ,
@hits_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as hits_pa_std3_lh ,
@avg_ := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END)/(COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END)-COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._bb_fl > 0 THEN 1 END)-COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._hbp_fl > 0 THEN 1 END)) as avg_std3_lh ,
@obp :=  COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as obp,
@sba_pa := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.selfbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as sba_pa_std3_lh ,
@sba_o := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.selfbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.outsmade ELSE 0 END) as sba_o_std3_lh ,
@rba_pa := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.runnerbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as rba_pa_std3_lh ,
@rba_o := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.runnerbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.outsmade ELSE 0 END) as rba_o_std3_lh ,
@tba_pa := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.totbasesadv ELSE 0 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as tba_pa_std3_lh ,
@tba_o := SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.totbasesadv ELSE 0 END)/SUM(CASE WHEN @logic_std3_lh  = 1 THEN a.outsmade ELSE 0 END) as tba_o_std3_lh ,
@k_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._k_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as k_pa_std3_lh ,
@klook_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._klook_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as klook_pa_std3_lh ,
@kswing_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._kswing_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as kswing_pa_std3_lh ,
@walk_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._bb_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as walk_pa_std3_lh ,
@singles_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._1b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as singles_pa_std3_lh ,
@doubles_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._2b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as doubles_pa_std3_lh ,
@triples_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._3b_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as triples_pa_std3_lh ,
@homeruns_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a._hr_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as homeruns_pa_std3_lh ,
@groundballs_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a.groundball_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as groundballs_pa_std3_lh ,
@linedrives_pa := COUNT(CASE WHEN @logic_std3_lh  = 1 AND a.linedrive_fl > 0 THEN 1 END)/COUNT(DISTINCT CASE WHEN @logic_std3_lh  = 1 THEN concat(a.gid,a.ab_num) END) as linedrives_pa_std3_lh 


-- -- -- -- 

FROM
analbase_atbat as a
left join
(SELECT distinct id_, concat(first_,' ',last_) as pname FROM pfx_player
WHERE player_record_id IN (SELECT max(player_record_id) FROM pfx_player group by id_)
) as p
ON p.id_ = a.batter

GROUP BY
@analdate,
a.batter

ORDER BY tba_o_std desc
;