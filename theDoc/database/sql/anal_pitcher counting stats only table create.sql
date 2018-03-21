SET 
@analdate := '2016-07-07'
;

DROP TABLE IF EXISTS anal_pitcher_counting_p;
CREATE TABLE anal_pitcher_counting_p AS

SELECT
@analdate + interval '1' day as anal_game_date,
a.pitcher,
player.pname,

###########  STD  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std,
@pa := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,a.ab_num) END) as pa_std,
@pitches := COUNT(DISTINCT CASE WHEN @logic_std = 1 THEN concat(a.gid,p.pitch_id) END) as p_std,

@ball := SUM(CASE WHEN @logic_std = 1 THEN p.ball_fl ELSE 0 END) as ball_std,
@strike := SUM(CASE WHEN @logic_std = 1 THEN p.strike_fl ELSE 0 END) as strike_std,
@calledstrike := SUM(CASE WHEN @logic_std = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_std,
@whiffstrike := SUM(CASE WHEN @logic_std = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_std,
@foulstrike := SUM(CASE WHEN @logic_std = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_std,
@inplay := SUM(CASE WHEN @logic_std = 1 THEN p.inplay_fl ELSE 0 END) as inplay_std,
@swing := SUM(CASE WHEN @logic_std = 1 THEN p.swing_fl ELSE 0 END) as swing_std,
@take := SUM(CASE WHEN @logic_std = 1 THEN p.take_fl ELSE 0 END) as take_std,
@firstpstrike := SUM(CASE WHEN @logic_std = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_std,
@firstpnotinplay := SUM(CASE WHEN @logic_std = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_std,
@secondpstrike := SUM(CASE WHEN @logic_std = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_std,

@zoneedge_in2 := SUM(CASE WHEN @logic_std = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_std,
@zoneedge_out2 := SUM(CASE WHEN @logic_std = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_std,
@zoneedge_in4 := SUM(CASE WHEN @logic_std = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_std,
@zoneedge_out4 := SUM(CASE WHEN @logic_std = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_std,
@zonecorn_in2 := SUM(CASE WHEN @logic_std = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_std,
@zonecorn_out2 := SUM(CASE WHEN @logic_std = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_std,
@zonecorn_in4 := SUM(CASE WHEN @logic_std = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_std,
@zonecorn_out4 := SUM(CASE WHEN @logic_std = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_std,
@zone_mid3 := SUM(CASE WHEN @logic_std = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_std,
@zone_mid6 := SUM(CASE WHEN @logic_std = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_std,
@zone_bigmiss4 := SUM(CASE WHEN @logic_std = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_std,

@fastball := COUNT(CASE WHEN @logic_std = 1 THEN p.fastball_endspeed END) as fastball_std,
@fastball_endspeed := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_endspeed END) as fastball_endspeed_std,
@fastball_spinrate := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_spinrate END) as fastball_spinrate_std,
@fastball_spindir := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_spindir END) as fastball_spindir_std,
@fastball_pfx_x := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_std,
@fastball_pfx_z := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_std,
@fastball_mnorm := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_mnorm END) as fastball_mnorm_std,
@fastball_adjmnorm := SUM(CASE WHEN @logic_std = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_std,

@curveball := COUNT(CASE WHEN @logic_std = 1 THEN p.curveball_endspeed END) as curveball_std,
@curveball_endspeed := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_endspeed END) as curveball_endspeed_std,
@curveball_spinrate := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_spinrate END) as curveball_spinrate_std,
@curveball_spindir := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_spindir END) as curveball_spindir_std,
@curveball_pfx_x := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_std,
@curveball_pfx_z := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_std,
@curveball_mnorm := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_mnorm END) as curveball_mnorm_std,
@curveball_adjmnorm := SUM(CASE WHEN @logic_std = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_std,

@slider := COUNT(CASE WHEN @logic_std = 1 THEN p.slider_endspeed END) as slider_std,
@slider_endspeed := SUM(CASE WHEN @logic_std = 1 THEN p.slider_endspeed END) as slider_endspeed_std,
@slider_spinrate := SUM(CASE WHEN @logic_std = 1 THEN p.slider_spinrate END) as slider_spinrate_std,
@slider_spindir := SUM(CASE WHEN @logic_std = 1 THEN p.slider_spindir END) as slider_spindir_std,
@slider_pfx_x := SUM(CASE WHEN @logic_std = 1 THEN p.slider_pfx_x END) as slider_pfx_x_std,
@slider_pfx_z := SUM(CASE WHEN @logic_std = 1 THEN p.slider_pfx_z END) as slider_pfx_z_std,
@slider_mnorm := SUM(CASE WHEN @logic_std = 1 THEN p.slider_mnorm END) as slider_mnorm_std,
@slider_adjmnorm := SUM(CASE WHEN @logic_std = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_std,

@changeup := COUNT(CASE WHEN @logic_std = 1 THEN p.changeup_endspeed END) as changeup_std,
@changeup_endspeed := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_endspeed END) as changeup_endspeed_std,
@changeup_spinrate := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_spinrate END) as changeup_spinrate_std,
@changeup_spindir := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_spindir END) as changeup_spindir_std,
@changeup_pfx_x := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_std,
@changeup_pfx_z := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_std,
@changeup_mnorm := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_mnorm END) as changeup_mnorm_std,
@changeup_adjmnorm := SUM(CASE WHEN @logic_std = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_std,

###########  std2  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std2 := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std2,
@pa := COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,a.ab_num) END) as pa_std2,
@pitches := COUNT(DISTINCT CASE WHEN @logic_std2 = 1 THEN concat(a.gid,p.pitch_id) END) as p_std2,

@ball := SUM(CASE WHEN @logic_std2 = 1 THEN p.ball_fl ELSE 0 END) as ball_std2,
@strike := SUM(CASE WHEN @logic_std2 = 1 THEN p.strike_fl ELSE 0 END) as strike_std2,
@calledstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_std2,
@whiffstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_std2,
@foulstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_std2,
@inplay := SUM(CASE WHEN @logic_std2 = 1 THEN p.inplay_fl ELSE 0 END) as inplay_std2,
@swing := SUM(CASE WHEN @logic_std2 = 1 THEN p.swing_fl ELSE 0 END) as swing_std2,
@take := SUM(CASE WHEN @logic_std2 = 1 THEN p.take_fl ELSE 0 END) as take_std2,
@firstpstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_std2,
@firstpnotinplay := SUM(CASE WHEN @logic_std2 = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_std2,
@secondpstrike := SUM(CASE WHEN @logic_std2 = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_std2,

@zoneedge_in2 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_std2,
@zoneedge_out2 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_std2,
@zoneedge_in4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_std2,
@zoneedge_out4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_std2,
@zonecorn_in2 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_std2,
@zonecorn_out2 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_std2,
@zonecorn_in4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_std2,
@zonecorn_out4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_std2,
@zone_mid3 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_std2,
@zone_mid6 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_std2,
@zone_bigmiss4 := SUM(CASE WHEN @logic_std2 = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_std2,

@fastball := COUNT(CASE WHEN @logic_std2 = 1 THEN p.fastball_endspeed END) as fastball_std2,
@fastball_endspeed := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_endspeed END) as fastball_endspeed_std2,
@fastball_spinrate := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_spinrate END) as fastball_spinrate_std2,
@fastball_spindir := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_spindir END) as fastball_spindir_std2,
@fastball_pfx_x := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_std2,
@fastball_pfx_z := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_std2,
@fastball_mnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_mnorm END) as fastball_mnorm_std2,
@fastball_adjmnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_std2,

@curveball := COUNT(CASE WHEN @logic_std2 = 1 THEN p.curveball_endspeed END) as curveball_std2,
@curveball_endspeed := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_endspeed END) as curveball_endspeed_std2,
@curveball_spinrate := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_spinrate END) as curveball_spinrate_std2,
@curveball_spindir := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_spindir END) as curveball_spindir_std2,
@curveball_pfx_x := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_std2,
@curveball_pfx_z := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_std2,
@curveball_mnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_mnorm END) as curveball_mnorm_std2,
@curveball_adjmnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_std2,

@slider := COUNT(CASE WHEN @logic_std2 = 1 THEN p.slider_endspeed END) as slider_std2,
@slider_endspeed := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_endspeed END) as slider_endspeed_std2,
@slider_spinrate := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_spinrate END) as slider_spinrate_std2,
@slider_spindir := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_spindir END) as slider_spindir_std2,
@slider_pfx_x := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_pfx_x END) as slider_pfx_x_std2,
@slider_pfx_z := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_pfx_z END) as slider_pfx_z_std2,
@slider_mnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_mnorm END) as slider_mnorm_std2,
@slider_adjmnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_std2,

@changeup := COUNT(CASE WHEN @logic_std2 = 1 THEN p.changeup_endspeed END) as changeup_std2,
@changeup_endspeed := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_endspeed END) as changeup_endspeed_std2,
@changeup_spinrate := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_spinrate END) as changeup_spinrate_std2,
@changeup_spindir := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_spindir END) as changeup_spindir_std2,
@changeup_pfx_x := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_std2,
@changeup_pfx_z := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_std2,
@changeup_mnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_mnorm END) as changeup_mnorm_std2,
@changeup_adjmnorm := SUM(CASE WHEN @logic_std2 = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_std2,

###########  std3  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_std3 := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN a.gid END) as gp_std3,
@pa := COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,a.ab_num) END) as pa_std3,
@pitches := COUNT(DISTINCT CASE WHEN @logic_std3 = 1 THEN concat(a.gid,p.pitch_id) END) as p_std3,

@ball := SUM(CASE WHEN @logic_std3 = 1 THEN p.ball_fl ELSE 0 END) as ball_std3,
@strike := SUM(CASE WHEN @logic_std3 = 1 THEN p.strike_fl ELSE 0 END) as strike_std3,
@calledstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_std3,
@whiffstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_std3,
@foulstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_std3,
@inplay := SUM(CASE WHEN @logic_std3 = 1 THEN p.inplay_fl ELSE 0 END) as inplay_std3,
@swing := SUM(CASE WHEN @logic_std3 = 1 THEN p.swing_fl ELSE 0 END) as swing_std3,
@take := SUM(CASE WHEN @logic_std3 = 1 THEN p.take_fl ELSE 0 END) as take_std3,
@firstpstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_std3,
@firstpnotinplay := SUM(CASE WHEN @logic_std3 = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_std3,
@secondpstrike := SUM(CASE WHEN @logic_std3 = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_std3,

@zoneedge_in2 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_std3,
@zoneedge_out2 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_std3,
@zoneedge_in4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_std3,
@zoneedge_out4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_std3,
@zonecorn_in2 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_std3,
@zonecorn_out2 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_std3,
@zonecorn_in4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_std3,
@zonecorn_out4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_std3,
@zone_mid3 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_std3,
@zone_mid6 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_std3,
@zone_bigmiss4 := SUM(CASE WHEN @logic_std3 = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_std3,

@fastball := COUNT(CASE WHEN @logic_std3 = 1 THEN p.fastball_endspeed END) as fastball_std3,
@fastball_endspeed := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_endspeed END) as fastball_endspeed_std3,
@fastball_spinrate := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_spinrate END) as fastball_spinrate_std3,
@fastball_spindir := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_spindir END) as fastball_spindir_std3,
@fastball_pfx_x := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_std3,
@fastball_pfx_z := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_std3,
@fastball_mnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_mnorm END) as fastball_mnorm_std3,
@fastball_adjmnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_std3,

@curveball := COUNT(CASE WHEN @logic_std3 = 1 THEN p.curveball_endspeed END) as curveball_std3,
@curveball_endspeed := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_endspeed END) as curveball_endspeed_std3,
@curveball_spinrate := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_spinrate END) as curveball_spinrate_std3,
@curveball_spindir := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_spindir END) as curveball_spindir_std3,
@curveball_pfx_x := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_std3,
@curveball_pfx_z := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_std3,
@curveball_mnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_mnorm END) as curveball_mnorm_std3,
@curveball_adjmnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_std3,

@slider := COUNT(CASE WHEN @logic_std3 = 1 THEN p.slider_endspeed END) as slider_std3,
@slider_endspeed := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_endspeed END) as slider_endspeed_std3,
@slider_spinrate := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_spinrate END) as slider_spinrate_std3,
@slider_spindir := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_spindir END) as slider_spindir_std3,
@slider_pfx_x := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_pfx_x END) as slider_pfx_x_std3,
@slider_pfx_z := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_pfx_z END) as slider_pfx_z_std3,
@slider_mnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_mnorm END) as slider_mnorm_std3,
@slider_adjmnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_std3,

@changeup := COUNT(CASE WHEN @logic_std3 = 1 THEN p.changeup_endspeed END) as changeup_std3,
@changeup_endspeed := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_endspeed END) as changeup_endspeed_std3,
@changeup_spinrate := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_spinrate END) as changeup_spinrate_std3,
@changeup_spindir := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_spindir END) as changeup_spindir_std3,
@changeup_pfx_x := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_std3,
@changeup_pfx_z := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_std3,
@changeup_mnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_mnorm END) as changeup_mnorm_std3,
@changeup_adjmnorm := SUM(CASE WHEN @logic_std3 = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_std3,

###########  last60  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.gid END) as gp_last60,
@pa := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last60,
@pitches := COUNT(DISTINCT CASE WHEN @logic_last60 = 1 THEN concat(a.gid,p.pitch_id) END) as p_last60,

@ball := SUM(CASE WHEN @logic_last60 = 1 THEN p.ball_fl ELSE 0 END) as ball_last60,
@strike := SUM(CASE WHEN @logic_last60 = 1 THEN p.strike_fl ELSE 0 END) as strike_last60,
@calledstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_last60,
@whiffstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_last60,
@foulstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_last60,
@inplay := SUM(CASE WHEN @logic_last60 = 1 THEN p.inplay_fl ELSE 0 END) as inplay_last60,
@swing := SUM(CASE WHEN @logic_last60 = 1 THEN p.swing_fl ELSE 0 END) as swing_last60,
@take := SUM(CASE WHEN @logic_last60 = 1 THEN p.take_fl ELSE 0 END) as take_last60,
@firstpstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_last60,
@firstpnotinplay := SUM(CASE WHEN @logic_last60 = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_last60,
@secondpstrike := SUM(CASE WHEN @logic_last60 = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_last60,

@zoneedge_in2 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_last60,
@zoneedge_out2 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_last60,
@zoneedge_in4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_last60,
@zoneedge_out4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_last60,
@zonecorn_in2 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_last60,
@zonecorn_out2 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_last60,
@zonecorn_in4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_last60,
@zonecorn_out4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_last60,
@zone_mid3 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_last60,
@zone_mid6 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_last60,
@zone_bigmiss4 := SUM(CASE WHEN @logic_last60 = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_last60,

@fastball := COUNT(CASE WHEN @logic_last60 = 1 THEN p.fastball_endspeed END) as fastball_last60,
@fastball_endspeed := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_endspeed END) as fastball_endspeed_last60,
@fastball_spinrate := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_spinrate END) as fastball_spinrate_last60,
@fastball_spindir := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_spindir END) as fastball_spindir_last60,
@fastball_pfx_x := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_last60,
@fastball_pfx_z := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_last60,
@fastball_mnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_mnorm END) as fastball_mnorm_last60,
@fastball_adjmnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_last60,

@curveball := COUNT(CASE WHEN @logic_last60 = 1 THEN p.curveball_endspeed END) as curveball_last60,
@curveball_endspeed := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_endspeed END) as curveball_endspeed_last60,
@curveball_spinrate := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_spinrate END) as curveball_spinrate_last60,
@curveball_spindir := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_spindir END) as curveball_spindir_last60,
@curveball_pfx_x := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_last60,
@curveball_pfx_z := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_last60,
@curveball_mnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_mnorm END) as curveball_mnorm_last60,
@curveball_adjmnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_last60,

@slider := COUNT(CASE WHEN @logic_last60 = 1 THEN p.slider_endspeed END) as slider_last60,
@slider_endspeed := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_endspeed END) as slider_endspeed_last60,
@slider_spinrate := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_spinrate END) as slider_spinrate_last60,
@slider_spindir := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_spindir END) as slider_spindir_last60,
@slider_pfx_x := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_pfx_x END) as slider_pfx_x_last60,
@slider_pfx_z := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_pfx_z END) as slider_pfx_z_last60,
@slider_mnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_mnorm END) as slider_mnorm_last60,
@slider_adjmnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_last60,

@changeup := COUNT(CASE WHEN @logic_last60 = 1 THEN p.changeup_endspeed END) as changeup_last60,
@changeup_endspeed := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_endspeed END) as changeup_endspeed_last60,
@changeup_spinrate := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_spinrate END) as changeup_spinrate_last60,
@changeup_spindir := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_spindir END) as changeup_spindir_last60,
@changeup_pfx_x := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_last60,
@changeup_pfx_z := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_last60,
@changeup_mnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_mnorm END) as changeup_mnorm_last60,
@changeup_adjmnorm := SUM(CASE WHEN @logic_last60 = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_last60,


###########  last20  ############

-- COUNTING STATS 

@gp := COUNT(DISTINCT CASE WHEN @logic_last20 := (a.game_date BETWEEN @analdate - interval '20' day AND @analdate) = 1 THEN a.gid END) as gp_last20,
@pa := COUNT(DISTINCT CASE WHEN @logic_last20 = 1 THEN concat(a.gid,a.ab_num) END) as pa_last20,
@pitches := COUNT(DISTINCT CASE WHEN @logic_last20 = 1 THEN concat(a.gid,p.pitch_id) END) as p_last20,

@ball := SUM(CASE WHEN @logic_last20 = 1 THEN p.ball_fl ELSE 0 END) as ball_last20,
@strike := SUM(CASE WHEN @logic_last20 = 1 THEN p.strike_fl ELSE 0 END) as strike_last20,
@calledstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.calledstrike_fl ELSE 0 END) as calledstrike_last20,
@whiffstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.swingmissstrike_fl ELSE 0 END) as whiffstrike_last20,
@foulstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.foulstrike_fl ELSE 0 END) as foulstrike_last20,
@inplay := SUM(CASE WHEN @logic_last20 = 1 THEN p.inplay_fl ELSE 0 END) as inplay_last20,
@swing := SUM(CASE WHEN @logic_last20 = 1 THEN p.swing_fl ELSE 0 END) as swing_last20,
@take := SUM(CASE WHEN @logic_last20 = 1 THEN p.take_fl ELSE 0 END) as take_last20,
@firstpstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.firstpstrike_fl ELSE 0 END) as firstpstrike_last20,
@firstpnotinplay := SUM(CASE WHEN @logic_last20 = 1 THEN p.firstpnotinplay_fl ELSE 0 END) as firstpnotinplay_fl_last20,
@secondpstrike := SUM(CASE WHEN @logic_last20 = 1 THEN p.secondpstrike_fl ELSE 0 END) as secondpstrike_last20,

@zoneedge_in2 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zoneedge_in2 ELSE 0 END) as zoneedge_in2_last20,
@zoneedge_out2 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zoneedge_out2 ELSE 0 END) as zoneedge_out2_last20,
@zoneedge_in4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zoneedge_in4 ELSE 0 END) as zoneedge_in4_last20,
@zoneedge_out4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zoneedge_out4 ELSE 0 END) as zoneedge_out4_last20,
@zonecorn_in2 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zonecorn_in2 ELSE 0 END) as zonecorn_in2_last20,
@zonecorn_out2 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zonecorn_out2 ELSE 0 END) as zonecorn_out2_last20,
@zonecorn_in4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zonecorn_in4 ELSE 0 END) as zonecorn_in4_last20,
@zonecorn_out4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zonecorn_out4 ELSE 0 END) as zonecorn_out4_last20,
@zone_mid3 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zone_mid3 ELSE 0 END) as zone_mid3_last20,
@zone_mid6 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zone_mid6 ELSE 0 END) as zone_mid6_last20,
@zone_bigmiss4 := SUM(CASE WHEN @logic_last20 = 1 THEN p.zone_bigmiss4 ELSE 0 END) as zone_bigmiss4_last20,

@fastball := COUNT(CASE WHEN @logic_last20 = 1 THEN p.fastball_endspeed END) as fastball_last20,
@fastball_endspeed := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_endspeed END) as fastball_endspeed_last20,
@fastball_spinrate := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_spinrate END) as fastball_spinrate_last20,
@fastball_spindir := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_spindir END) as fastball_spindir_last20,
@fastball_pfx_x := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_pfx_x END) as fastball_pfx_x_last20,
@fastball_pfx_z := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_pfx_z END) as fastball_pfx_z_last20,
@fastball_mnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_mnorm END) as fastball_mnorm_last20,
@fastball_adjmnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.fastball_adjmnorm END) as fastball_adjmnorm_last20,

@curveball := COUNT(CASE WHEN @logic_last20 = 1 THEN p.curveball_endspeed END) as curveball_last20,
@curveball_endspeed := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_endspeed END) as curveball_endspeed_last20,
@curveball_spinrate := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_spinrate END) as curveball_spinrate_last20,
@curveball_spindir := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_spindir END) as curveball_spindir_last20,
@curveball_pfx_x := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_pfx_x END) as curveball_pfx_x_last20,
@curveball_pfx_z := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_pfx_z END) as curveball_pfx_z_last20,
@curveball_mnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_mnorm END) as curveball_mnorm_last20,
@curveball_adjmnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.curveball_adjmnorm END) as curveball_adjmnorm_last20,

@slider := COUNT(CASE WHEN @logic_last20 = 1 THEN p.slider_endspeed END) as slider_last20,
@slider_endspeed := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_endspeed END) as slider_endspeed_last20,
@slider_spinrate := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_spinrate END) as slider_spinrate_last20,
@slider_spindir := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_spindir END) as slider_spindir_last20,
@slider_pfx_x := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_pfx_x END) as slider_pfx_x_last20,
@slider_pfx_z := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_pfx_z END) as slider_pfx_z_last20,
@slider_mnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_mnorm END) as slider_mnorm_last20,
@slider_adjmnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.slider_adjmnorm END) as slider_adjmnorm_last20,

@changeup := COUNT(CASE WHEN @logic_last20 = 1 THEN p.changeup_endspeed END) as changeup_last20,
@changeup_endspeed := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_endspeed END) as changeup_endspeed_last20,
@changeup_spinrate := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_spinrate END) as changeup_spinrate_last20,
@changeup_spindir := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_spindir END) as changeup_spindir_last20,
@changeup_pfx_x := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_pfx_x END) as changeup_pfx_x_last20,
@changeup_pfx_z := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_pfx_z END) as changeup_pfx_z_last20,
@changeup_mnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_mnorm END) as changeup_mnorm_last20,
@changeup_adjmnorm := SUM(CASE WHEN @logic_last20 = 1 THEN p.changeup_adjmnorm END) as changeup_adjmnorm_last20

#,NULL, -- for createddate
#NULL, -- for lastmodifieddate
#NULL -- for autoincrement PK

FROM
analbase_pitch as p
LEFT JOIN
analbase_atbat as a
ON a.gid = p.gid AND a.ab_num = p.ab_num
JOIN pfx_game as g
ON g.gid = a.gid
LEFT JOIN
	(SELECT distinct id_, concat(first_,' ',last_) as pname FROM pfx_player
	WHERE player_record_id IN (SELECT max(player_record_id) FROM pfx_player group by id_)
	) as player
ON player.id_ = a.pitcher

WHERE g.game_type = 'R'
AND YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) 
AND a.game_date <= @analdate

GROUP BY
@analdate,
a.pitcher

HAVING COUNT(DISTINCT CASE WHEN (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN concat(a.gid,a.ab_num) END) > 0
        
ORDER BY zonecorn_in4_std desc
;

DROP TABLE IF EXISTS anal_pitcher_counting_ab;
CREATE TABLE anal_pitcher_counting_ab AS
SELECT
@analdate + interval '1' day as anal_game_date,
a.pitcher,
player.pname,

###########  STD  ############

@pa_out := COUNT(CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std,
@outsmade := SUM(CASE WHEN @logic_std := (YEAR(@analdate) = YEAR(a.game_date) AND a.game_date <= @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_std,
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
@totbasesadv := SUM(CASE WHEN @logic_std = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std,
@runsscored := SUM(CASE WHEN @logic_std = 1 THEN a.runsscored ELSE 0 END) as runsscored_std,

###########  std2  ############

@pa_out := COUNT(CASE WHEN @logic_std2 := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std2,
@outsmade := SUM(CASE WHEN @logic_std2 := (YEAR(@analdate) <= YEAR(a.game_date + interval '1' year) AND a.game_date <= @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_std2,
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
@totbasesadv := SUM(CASE WHEN @logic_std2 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std2,
@runsscored := SUM(CASE WHEN @logic_std2 = 1 THEN a.runsscored ELSE 0 END) as runsscored_std2,

###########  std3  ############

@pa_out := COUNT(CASE WHEN @logic_std3 := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_std3,
@outsmade := SUM(CASE WHEN @logic_std3 := (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_std3,
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
@totbasesadv := SUM(CASE WHEN @logic_std3 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_std3,
@runsscored := SUM(CASE WHEN @logic_std3 = 1 THEN a.runsscored ELSE 0 END) as runsscored_std3,

###########  last60  ############

@pa_out := COUNT(CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last60,
@outsmade := SUM(CASE WHEN @logic_last60 := (a.game_date BETWEEN @analdate - interval '60' day AND @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_last60,
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
@totbasesadv := SUM(CASE WHEN @logic_last60 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last60,
@runsscored := SUM(CASE WHEN @logic_last60 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last60,

###########  last20  ############

@pa_out := COUNT(CASE WHEN @logic_last20 := (a.game_date BETWEEN @analdate - interval '20' day AND @analdate) = 1 AND a.outsmade > 0 THEN 1 END) as pa_out_last20,
@outsmade := SUM(CASE WHEN @logic_last20 := (a.game_date BETWEEN @analdate - interval '20' day AND @analdate) = 1 THEN a.outsmade ELSE 0 END) as outsmade_last20,
@hits := COUNT(CASE WHEN @logic_last20 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl > 0 THEN 1 END) as hits_last20,
@onbases := COUNT(CASE WHEN @logic_last20 = 1 AND a._1b_fl + a._2b_fl + a._3b_fl + a._hr_fl + a._bb_fl + a._hbp_fl + a._e_fl > 0 THEN 1 END) as onbases_last20,
@singles := COUNT(CASE WHEN @logic_last20 = 1 AND a._1b_fl > 0 THEN 1 END) as singles_last20,
@doubles := COUNT(CASE WHEN @logic_last20 = 1 AND a._2b_fl > 0 THEN 1 END) as doubles_last20,
@triples := COUNT(CASE WHEN @logic_last20 = 1 AND a._3b_fl > 0 THEN 1 END) as triples_last20,
@homeruns := COUNT(CASE WHEN @logic_last20 = 1 AND a._hr_fl > 0 THEN 1 END) as homeruns_last20,
@walks := COUNT(CASE WHEN @logic_last20 = 1 AND a._bb_fl > 0 THEN 1 END) as walks_last20,
@k := COUNT(CASE WHEN @logic_last20 = 1 AND a._k_fl > 0 THEN 1 END) as k_last20,
@klook := COUNT(CASE WHEN @logic_last20 = 1 AND a._klook_fl > 0 THEN 1 END) as klook_last20,
@kswing := COUNT(CASE WHEN @logic_last20 = 1 AND a._kswing_fl > 0 THEN 1 END) as kswing_last20,
@hbp := COUNT(CASE WHEN @logic_last20 = 1 AND a._hbp_fl > 0 THEN 1 END) as hbp_last20,
@groundballs := COUNT(CASE WHEN @logic_last20 = 1 AND a.groundball_fl > 0 THEN 1 END) as groundballs_last20,
@linedrives := COUNT(CASE WHEN @logic_last20 = 1 AND a.linedrive_fl > 0 THEN 1 END) as linedrives_last20,
@totbasesadv := SUM(CASE WHEN @logic_last20 = 1 THEN a.totbasesadv ELSE 0 END) as totbasesadv_last20,
@runsscored := SUM(CASE WHEN @logic_last20 = 1 THEN a.runsscored ELSE 0 END) as runsscored_last20

#,NULL, -- for createddate
#NULL, -- for lastmodifieddate
#NULL -- for autoincrement PK

FROM
analbase_atbat as a
JOIN pfx_game as g
ON g.gid = a.gid
LEFT JOIN
	(SELECT distinct id_, concat(first_,' ',last_) as pname FROM pfx_player
	WHERE player_record_id IN (SELECT max(player_record_id) FROM pfx_player group by id_)
	) as player
ON player.id_ = a.pitcher

WHERE g.game_type = 'R'
AND YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) 
AND a.game_date <= @analdate

GROUP BY
@analdate,
a.pitcher

HAVING COUNT(DISTINCT CASE WHEN (YEAR(@analdate) <= YEAR(a.game_date + interval '2' year) AND a.game_date <= @analdate) = 1 THEN concat(a.gid,a.ab_num) END) > 0

ORDER BY k_std desc;

DELETE FROM anal_pitcher_counting_p;
DELETE FROM anal_pitcher_counting_ab;

ALTER TABLE anal_pitcher_counting_p ADD COLUMN createddate timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE anal_pitcher_counting_p ADD COLUMN lastmodifiedddate timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE anal_pitcher_counting_p ADD COLUMN anal_pitcher_counting_p_PK int auto_increment primary key;

ALTER TABLE anal_pitcher_counting_ab ADD COLUMN createddate timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE anal_pitcher_counting_ab ADD COLUMN lastmodifiedddate timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE anal_pitcher_counting_ab ADD COLUMN anal_pitcher_counting_ab_PK int auto_increment primary key;

ALTER TABLE anal_pitcher_counting_p ADD UNIQUE KEY (anal_game_date, pitcher);
ALTER TABLE anal_pitcher_counting_ab ADD UNIQUE KEY (anal_game_date, pitcher);

select * from anal_pitcher_counting_p;

select anal_game_date, count(1) from anal_pitcher_counting_p group by 1 order by 1 desc

show processlist;
