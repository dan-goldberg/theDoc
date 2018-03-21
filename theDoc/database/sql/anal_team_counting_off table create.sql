DROP TABLE IF EXISTS anal_team_counting_off;

SET 
@analdate := '2013-07-02'
;

CREATE TABLE anal_team_counting_off AS

SELECT
@analdate + interval '1' day as anal_game_date_off,
@team_id := CASE WHEN a.home_bat_fl = 0 THEN g.away_id ELSE g.home_id END as team_id_off,
@team_name := CASE WHEN a.home_bat_fl = 0 THEN g.away_name_full ELSE g.home_name_full END as team_name_off,

###########  STD  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std_off,
@i := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.inn_num) END) as i_std_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_off,

@pa_out := COUNT(CASE WHEN @logic_std = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_off,
@outsmade := SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_off,
@hits := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_off,
@onbases := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_off,
@totbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_off,
@runsscored := SUM(CASE WHEN @logic_std = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_off,

###########  std_home  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_home := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.home_bat_fl = 1) = 1 THEN a.gid END) as gp_std_home_off,
@i := COUNT(DISTINCT CASE WHEN @logic_std_home = 1 THEN concat(a.gid,a.inn_num) END) as i_std_home_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_home = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_home_off,

@pa_out := COUNT(CASE WHEN @logic_std_home = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_home_off,
@outsmade := SUM(CASE WHEN @logic_std_home = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_home_off,
@hits := COUNT(CASE WHEN @logic_std_home = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_home_off,
@onbases := COUNT(CASE WHEN @logic_std_home = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_home_off,
@totbasesadv := SUM(CASE WHEN @logic_std_home = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_home_off,
@runsscored := SUM(CASE WHEN @logic_std_home = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_home_off,

###########  std_away  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_away := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.home_bat_fl = 0) = 1 THEN a.gid END) as gp_std_away_off,
@i := COUNT(DISTINCT CASE WHEN @logic_std_away = 1 THEN concat(a.gid,a.inn_num) END) as i_std_away_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_away = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_away_off,

@pa_out := COUNT(CASE WHEN @logic_std_away = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_away_off,
@outsmade := SUM(CASE WHEN @logic_std_away = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_away_off,
@hits := COUNT(CASE WHEN @logic_std_away = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_away_off,
@onbases := COUNT(CASE WHEN @logic_std_away = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_away_off,
@totbasesadv := SUM(CASE WHEN @logic_std_away = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_away_off,
@runsscored := SUM(CASE WHEN @logic_std_away = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_away_off,

###########  last60  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.gid END) as gp_last60_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_off,

@pa_out := COUNT(CASE WHEN @logic_last60 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_off,
@outsmade := SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_off,
@hits := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_off,
@onbases := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_off,
@totbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_off,
@runsscored := SUM(CASE WHEN @logic_last60 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_off,

###########  last30  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30 := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate) = 1 THEN a.gid END) as gp_last30_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_off,

@pa_out := COUNT(CASE WHEN @logic_last30 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_off,
@outsmade := SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_off,
@hits := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_off,
@onbases := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_off,
@totbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_off,
@runsscored := SUM(CASE WHEN @logic_last30 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_off,

###########  last10  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10 := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate) = 1 THEN a.gid END) as gp_last10_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_off,

@pa_out := COUNT(CASE WHEN @logic_last10 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_off,
@outsmade := SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_off,
@hits := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_off,
@onbases := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_off,
@totbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_off,
@runsscored := SUM(CASE WHEN @logic_last10 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_off,

###########  std_sp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_sp := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND sp_fl = 1) = 1 THEN a.gid END) as gp_std_sp_off,
@i := COUNT(DISTINCT CASE WHEN @logic_std_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_std_sp_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_sp_off,

@pa_out := COUNT(CASE WHEN @logic_std_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_sp_off,
@outsmade := SUM(CASE WHEN @logic_std_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_sp_off,
@hits := COUNT(CASE WHEN @logic_std_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_sp_off,
@onbases := COUNT(CASE WHEN @logic_std_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_sp_off,
@totbasesadv := SUM(CASE WHEN @logic_std_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_sp_off,
@runsscored := SUM(CASE WHEN @logic_std_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_sp_off,

###########  std_rp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_rp := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND sp_fl = 0) = 1 THEN a.gid END) as gp_std_rp_off,
@i := COUNT(DISTINCT CASE WHEN @logic_std_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_std_rp_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_rp_off,

@pa_out := COUNT(CASE WHEN @logic_std_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_rp_off,
@outsmade := SUM(CASE WHEN @logic_std_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_rp_off,
@hits := COUNT(CASE WHEN @logic_std_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_rp_off,
@onbases := COUNT(CASE WHEN @logic_std_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_rp_off,
@totbasesadv := SUM(CASE WHEN @logic_std_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_rp_off,
@runsscored := SUM(CASE WHEN @logic_std_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_rp_off,

###########  last60_sp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60_sp := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last60_sp_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last60_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_sp_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_sp_off,

@pa_out := COUNT(CASE WHEN @logic_last60_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_sp_off,
@outsmade := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_sp_off,
@hits := COUNT(CASE WHEN @logic_last60_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_sp_off,
@onbases := COUNT(CASE WHEN @logic_last60_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_sp_off,
@totbasesadv := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_sp_off,
@runsscored := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_sp_off,

###########  last30_sp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30_sp := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last30_sp_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last30_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_sp_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_sp_off,

@pa_out := COUNT(CASE WHEN @logic_last30_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_sp_off,
@outsmade := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_sp_off,
@hits := COUNT(CASE WHEN @logic_last30_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_sp_off,
@onbases := COUNT(CASE WHEN @logic_last30_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_sp_off,
@totbasesadv := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_sp_off,
@runsscored := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_sp_off,

###########  last10_sp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10_sp := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last10_sp_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last10_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_sp_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_sp_off,

@pa_out := COUNT(CASE WHEN @logic_last10_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_sp_off,
@outsmade := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_sp_off,
@hits := COUNT(CASE WHEN @logic_last10_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_sp_off,
@onbases := COUNT(CASE WHEN @logic_last10_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_sp_off,
@totbasesadv := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_sp_off,
@runsscored := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_sp_off,

###########  last60_rp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60_rp := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last60_rp_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last60_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_rp_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_rp_off,

@pa_out := COUNT(CASE WHEN @logic_last60_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_rp_off,
@outsmade := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_rp_off,
@hits := COUNT(CASE WHEN @logic_last60_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_rp_off,
@onbases := COUNT(CASE WHEN @logic_last60_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_rp_off,
@totbasesadv := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_rp_off,
@runsscored := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_rp_off,

###########  last30_rp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30_rp := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last30_rp_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last30_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_rp_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_rp_off,

@pa_out := COUNT(CASE WHEN @logic_last30_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_rp_off,
@outsmade := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_rp_off,
@hits := COUNT(CASE WHEN @logic_last30_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_rp_off,
@onbases := COUNT(CASE WHEN @logic_last30_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_rp_off,
@totbasesadv := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_rp_off,
@runsscored := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_rp_off,

###########  last10_rp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10_rp := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last10_rp_off,
@i := COUNT(DISTINCT CASE WHEN @logic_last10_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_rp_off,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_rp_off,

@pa_out := COUNT(CASE WHEN @logic_last10_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_rp_off,
@outsmade := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_rp_off,
@hits := COUNT(CASE WHEN @logic_last10_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_rp_off,
@onbases := COUNT(CASE WHEN @logic_last10_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_rp_off,
@totbasesadv := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_rp_off,
@runsscored := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_rp_off

FROM
analbase_atbat as a
JOIN pfx_game as g
ON g.gid = a.gid

WHERE g.game_type = 'R'

GROUP BY
@analdate,
CASE WHEN a.home_bat_fl = 0 THEN g.away_id ELSE g.home_id END
;



DROP TABLE IF EXISTS anal_team_counting_def;

CREATE TABLE anal_team_counting_def AS

SELECT
@analdate + interval '1' day as anal_game_date_def,
@team_id := CASE WHEN a.home_bat_fl = 0 THEN g.away_id ELSE g.home_id END as team_id_def,
@team_name := CASE WHEN a.home_bat_fl = 1 THEN g.away_name_full ELSE g.home_name_full END as team_name_def,

###########  STD  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std_def,
@i := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.inn_num) END) as i_std_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_def,

@pa_out := COUNT(CASE WHEN @logic_std = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_def,
@outsmade := SUM(CASE WHEN @logic_std = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_def,
@hits := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_def,
@onbases := COUNT(CASE WHEN @logic_std = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_def,
@totbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_def,
@runsscored := SUM(CASE WHEN @logic_std = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_def,

###########  std_home  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_home := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.home_bat_fl = 1) = 1 THEN a.gid END) as gp_std_home_def,
@i := COUNT(DISTINCT CASE WHEN @logic_std_home = 1 THEN concat(a.gid,a.inn_num) END) as i_std_home_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_home = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_home_def,

@pa_out := COUNT(CASE WHEN @logic_std_home = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_home_def,
@outsmade := SUM(CASE WHEN @logic_std_home = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_home_def,
@hits := COUNT(CASE WHEN @logic_std_home = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_home_def,
@onbases := COUNT(CASE WHEN @logic_std_home = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_home_def,
@totbasesadv := SUM(CASE WHEN @logic_std_home = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_home_def,
@runsscored := SUM(CASE WHEN @logic_std_home = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_home_def,

###########  std_away  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_away := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND a.home_bat_fl = 0) = 1 THEN a.gid END) as gp_std_away_def,
@i := COUNT(DISTINCT CASE WHEN @logic_std_away = 1 THEN concat(a.gid,a.inn_num) END) as i_std_away_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_away = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_away_def,

@pa_out := COUNT(CASE WHEN @logic_std_away = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_away_def,
@outsmade := SUM(CASE WHEN @logic_std_away = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_away_def,
@hits := COUNT(CASE WHEN @logic_std_away = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_away_def,
@onbases := COUNT(CASE WHEN @logic_std_away = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_away_def,
@totbasesadv := SUM(CASE WHEN @logic_std_away = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_away_def,
@runsscored := SUM(CASE WHEN @logic_std_away = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_away_def,

###########  last60  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.gid END) as gp_last60_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_def,

@pa_out := COUNT(CASE WHEN @logic_last60 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_def,
@outsmade := SUM(CASE WHEN @logic_last60 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_def,
@hits := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_def,
@onbases := COUNT(CASE WHEN @logic_last60 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_def,
@totbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_def,
@runsscored := SUM(CASE WHEN @logic_last60 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_def,

###########  last30  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30 := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate) = 1 THEN a.gid END) as gp_last30_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_def,

@pa_out := COUNT(CASE WHEN @logic_last30 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_def,
@outsmade := SUM(CASE WHEN @logic_last30 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_def,
@hits := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_def,
@onbases := COUNT(CASE WHEN @logic_last30 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_def,
@totbasesadv := SUM(CASE WHEN @logic_last30 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_def,
@runsscored := SUM(CASE WHEN @logic_last30 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_def,

###########  last10  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10 := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate) = 1 THEN a.gid END) as gp_last10_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_def,

@pa_out := COUNT(CASE WHEN @logic_last10 = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_def,
@outsmade := SUM(CASE WHEN @logic_last10 = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_def,
@hits := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_def,
@onbases := COUNT(CASE WHEN @logic_last10 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_def,
@totbasesadv := SUM(CASE WHEN @logic_last10 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_def,
@runsscored := SUM(CASE WHEN @logic_last10 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_def,

###########  std_sp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_sp := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND sp_fl = 1) = 1 THEN a.gid END) as gp_std_sp_def,
@i := COUNT(DISTINCT CASE WHEN @logic_std_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_std_sp_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_sp_def,

@pa_out := COUNT(CASE WHEN @logic_std_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_sp_def,
@outsmade := SUM(CASE WHEN @logic_std_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_sp_def,
@hits := COUNT(CASE WHEN @logic_std_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_sp_def,
@onbases := COUNT(CASE WHEN @logic_std_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_sp_def,
@totbasesadv := SUM(CASE WHEN @logic_std_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_sp_def,
@runsscored := SUM(CASE WHEN @logic_std_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_sp_def,

###########  std_rp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std_rp := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate AND sp_fl = 0) = 1 THEN a.gid END) as gp_std_rp_def,
@i := COUNT(DISTINCT CASE WHEN @logic_std_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_std_rp_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_std_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_std_rp_def,

@pa_out := COUNT(CASE WHEN @logic_std_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std_rp_def,
@outsmade := SUM(CASE WHEN @logic_std_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_std_rp_def,
@hits := COUNT(CASE WHEN @logic_std_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_std_rp_def,
@onbases := COUNT(CASE WHEN @logic_std_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_std_rp_def,
@totbasesadv := SUM(CASE WHEN @logic_std_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std_rp_def,
@runsscored := SUM(CASE WHEN @logic_std_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_std_rp_def,

###########  last60_sp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60_sp := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last60_sp_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last60_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_sp_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_sp_def,

@pa_out := COUNT(CASE WHEN @logic_last60_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_sp_def,
@outsmade := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_sp_def,
@hits := COUNT(CASE WHEN @logic_last60_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_sp_def,
@onbases := COUNT(CASE WHEN @logic_last60_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_sp_def,
@totbasesadv := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_sp_def,
@runsscored := SUM(CASE WHEN @logic_last60_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_sp_def,

###########  last30_sp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30_sp := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last30_sp_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last30_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_sp_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_sp_def,

@pa_out := COUNT(CASE WHEN @logic_last30_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_sp_def,
@outsmade := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_sp_def,
@hits := COUNT(CASE WHEN @logic_last30_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_sp_def,
@onbases := COUNT(CASE WHEN @logic_last30_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_sp_def,
@totbasesadv := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_sp_def,
@runsscored := SUM(CASE WHEN @logic_last30_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_sp_def,

###########  last10_sp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10_sp := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.sp_fl = 1) = 1 THEN a.gid END) as gp_last10_sp_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last10_sp = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_sp_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10_sp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_sp_def,

@pa_out := COUNT(CASE WHEN @logic_last10_sp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_sp_def,
@outsmade := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_sp_def,
@hits := COUNT(CASE WHEN @logic_last10_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_sp_def,
@onbases := COUNT(CASE WHEN @logic_last10_sp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_sp_def,
@totbasesadv := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_sp_def,
@runsscored := SUM(CASE WHEN @logic_last10_sp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_sp_def,

###########  last60_rp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60_rp := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last60_rp_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last60_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last60_rp_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60_rp_def,

@pa_out := COUNT(CASE WHEN @logic_last60_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60_rp_def,
@outsmade := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60_rp_def,
@hits := COUNT(CASE WHEN @logic_last60_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last60_rp_def,
@onbases := COUNT(CASE WHEN @logic_last60_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last60_rp_def,
@totbasesadv := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60_rp_def,
@runsscored := SUM(CASE WHEN @logic_last60_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60_rp_def,

###########  last30_rp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last30_rp := (a.game_date BETWEEN @analdate - interval '30' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last30_rp_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last30_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last30_rp_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last30_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last30_rp_def,

@pa_out := COUNT(CASE WHEN @logic_last30_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last30_rp_def,
@outsmade := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last30_rp_def,
@hits := COUNT(CASE WHEN @logic_last30_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last30_rp_def,
@onbases := COUNT(CASE WHEN @logic_last30_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last30_rp_def,
@totbasesadv := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last30_rp_def,
@runsscored := SUM(CASE WHEN @logic_last30_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last30_rp_def,

###########  last10_rp  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last10_rp := (a.game_date BETWEEN @analdate - interval '10' day AND @analdate AND a.sp_fl = 0) = 1 THEN a.gid END) as gp_last10_rp_def,
@i := COUNT(DISTINCT CASE WHEN @logic_last10_rp = 1 THEN concat(a.gid,a.inn_num) END) as i_last10_rp_def,
@pa := COUNT(DISTINCT CASE WHEN @logic_last10_rp = 1 THEN concat(a.gid,a.ab_num) END) as pa_last10_rp_def,

@pa_out := COUNT(CASE WHEN @logic_last10_rp = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last10_rp_def,
@outsmade := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.outsmade ELSE 0 END) as outsmade_last10_rp_def,
@hits := COUNT(CASE WHEN @logic_last10_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last10_rp_def,
@onbases := COUNT(CASE WHEN @logic_last10_rp = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last10_rp_def,
@totbasesadv := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last10_rp_def,
@runsscored := SUM(CASE WHEN @logic_last10_rp = 1 THEN a.runsscored ELSE 0 END) as runsscored_last10_rp_def

FROM
analbase_atbat as a
JOIN pfx_game as g
ON g.gid = a.gid

WHERE g.game_type = 'R'

GROUP BY
@analdate,
CASE WHEN a.home_bat_fl = 1 THEN g.away_id ELSE g.home_id END
;

DELETE FROM anal_team_counting_off;
DELETE FROM anal_team_counting_def;

ALTER TABLE anal_team_counting_off ADD UNIQUE KEY (anal_game_date_off,team_id_off);
ALTER TABLE anal_team_counting_def ADD UNIQUE KEY (anal_game_date_def,team_id_def);

ALTER TABLE anal_team_counting_off ADD COLUMN createddate timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE anal_team_counting_off ADD COLUMN lastmodifiedddate timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE anal_team_counting_off ADD COLUMN anal_pitcher_counting_off_PK int auto_increment primary key;

ALTER TABLE anal_team_counting_def ADD COLUMN createddate timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE anal_team_counting_def ADD COLUMN lastmodifiedddate timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE anal_team_counting_def ADD COLUMN anal_pitcher_counting_def_PK int auto_increment primary key;


select * from anal_team_counting_def as d
LEFT JOIN anal_team_counting_off as o
ON d.anal_game_date_def = o.anal_game_date_off
AND d.team_id_def = o.team_id_off;