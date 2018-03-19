use mlb;

#pitch table opps and events
DROP TABLE IF EXISTS analbase_pitch ;
CREATE TABLE analbase_pitch AS

SELECT DISTINCT

(@runsscored := CASE
	when ab.des LIKE '%scores%scores%scores%scores%'
		or ab.des LIKE '%homers%scores%scores%scores%'
        or ab.des LIKE '%grand slam%scores%scores%scores%' 
        or ab.des LIKE '%hits an inside-the-park home run%scores%scores%scores%' 
	then 4
    when ab.des LIKE '%scores%scores%scores%' 
		or ab.des LIKE '%homers%scores%scores%' 
        or ab.des LIKE '%hits an inside-the-park home run%scores%scores%' 
	then 3
    when ab.des LIKE '%scores%scores%' 
		or ab.des LIKE '%homers%scores%' 
        or ab.des LIKE '%hits an inside-the-park home run%scores%' 
	then 2
    when ab.des LIKE '%scores%' 
		or ab.des LIKE '%homers%' 
        or ab.des LIKE '%hits an inside-the-park home run%' 
        or (ab.event_ = 'Grounded Into DP' AND ab.score = 'T')
	then 1
	else 0
END) as runsscored_p,

##### ids
p.gid,
p.game_pk,
p.id_ as pitch_id,
p.ab_num,
p.event_num,
##### opportunities
p.game_date,
p.inn_num,
p.home_bat_fl,
p.des,
p.ab_pitch_num,

ab.stand as bat_side,
ab.p_throws,
CASE WHEN ab.stand = ab.p_throws THEN 1 ELSE 0 END as p_same_hand,
o - CASE 
	WHEN (ab.event_ LIKE '% Out' AND ab.event_ <> 'Runner Out') OR ab.event_ LIKE '%out' THEN 1
	WHEN ab.event_ LIKE 'Double Play' OR ab.event_ LIKE '% DP' THEN 2
    WHEN ab.event_ LIKE 'Triple Play' THEN 3
    ELSE 0
END - CASE WHEN ab.event2 LIKE '%Out' THEN 1 ELSE 0 END
as o_sit_p,
CASE 
	WHEN ab.home_bat_fl = 1 THEN (ab.home_teamruns - @runsscored) - ab.away_teamruns
    ELSE (ab.away_teamruns - @runsscored)- ab.home_teamruns
END as team_lead_p,

CASE WHEN p.on_1b IS NOT NULL THEN 1 ELSE 0 END as r1b_fl,
CASE WHEN p.on_2b IS NOT NULL THEN 1 ELSE 0 END as r2b_fl,
CASE WHEN p.on_3b IS NOT NULL THEN 1 ELSE 0 END as r3b_fl,

##### outcomes

CASE WHEN p.des IN ('Ball','Ball In Dirt','Intent Ball','Pitchout') THEN 1 ELSE 0 END as ball_fl,
CASE WHEN p.des LIKE '%Strike%' OR p.des LIKE 'Foul%' OR p.des LIKE 'In play%' THEN 1 ELSE 0 END as strike_fl,
CASE WHEN p.des IN ('Hit By Pitch') THEN 1 ELSE 0 END as hbp_fl,
CASE WHEN p.des IN ('Called Strike') THEN 1 ELSE 0 END as calledstrike_fl,
CASE WHEN p.des IN ('Swinging Strike','Swinging Strike (Blocked)','Missed Bunt','Swinging Pitchout') THEN 1 ELSE 0 END as swingmissstrike_fl,
CASE WHEN p.des LIKE 'Foul%' THEN 1 ELSE 0 END as foulstrike_fl,
CASE WHEN p.des LIKE 'In play%' THEN 1 ELSE 0 END as inplay_fl,
CASE WHEN p.des IN ('Swinging Strike','Swinging Strike (Blocked)','Missed Bunt','Swinging Pitchout') OR p.des LIKE 'Foul%' OR p.des LIKE 'In play%' THEN 1 ELSE 0 END as swing_fl,
CASE WHEN p.des IN ('Ball','Ball In Dirt','Intent Ball','Pitchout','Hit By Pitch','Called Strike') THEN 1 ELSE 0 END as take_fl,
CASE WHEN p.ab_pitch_num = 1 AND p.type_ = 'S' THEN 1 ELSE 0 END as firstpstrike_fl,
CASE WHEN p.ab_pitch_num = 1 AND p.type_ != 'X' THEN 1 ELSE 0 END as firstpnotinplay_fl,
CASE WHEN p.ab_pitch_num = 2 AND p.type_ = 'S' THEN 1 ELSE 0 END as secondpstrike_fl,

p.pitch_type,
p.spin_rate,
p.start_speed,
p.end_speed,
p.zone,
p.nasty,

CASE WHEN ( px BETWEEN (@xmin := -0.708) AND (@xmax := 0.708) AND (pz BETWEEN (@zmin := p.sz_bot) AND @zmin + (2/12) OR pz BETWEEN (@zmax := p.sz_top) - (2/12) AND @zmax) ) OR ( (px BETWEEN @xmin AND @xmin + (2/12) OR px BETWEEN @xmax - (2/12) AND @xmax) AND pz BETWEEN @zmin AND @zmax ) THEN 1 ELSE 0 END as zoneedge_in2,
CASE WHEN ( px BETWEEN @xmin AND @xmax AND (pz BETWEEN @zmin - (2/12) AND @zmin OR pz BETWEEN @zmax AND @zmax + (2/12)) ) OR ( (px BETWEEN @xmin - (2/12) AND @xmin OR px BETWEEN @xmax AND @xmax + (2/12)) AND pz BETWEEN @zmin AND @zmax ) THEN 1 ELSE 0 END as zoneedge_out2,
CASE WHEN ( px BETWEEN @xmin AND @xmax AND (pz BETWEEN @zmin AND @zmin + (4/12) OR pz BETWEEN @zmax - (4/12) AND @zmax) ) OR ( (px BETWEEN @xmin AND @xmin + (4/12) OR px BETWEEN @xmax - (4/12) AND @xmax) AND pz BETWEEN @zmin AND @zmax ) THEN 1 ELSE 0 END as zoneedge_in4,
CASE WHEN ( px BETWEEN @xmin AND @xmax AND (pz BETWEEN @zmin - (4/12) AND @zmin OR pz BETWEEN @zmax AND @zmax + (4/12)) ) OR ( (px BETWEEN @xmin - (4/12) AND @xmin OR px BETWEEN @xmax AND @xmax + (4/12)) AND pz BETWEEN @zmin AND @zmax ) THEN 1 ELSE 0 END as zoneedge_out4,
CASE WHEN ( px BETWEEN @xmin AND @xmax AND pz BETWEEN @zmin AND @zmax AND (px <= @xmin + (2/12) OR px >= @xmax - (2/12)) AND (pz <= @zmin + (2/12) OR pz >= @zmax - (2/12)) ) THEN 1 ELSE 0 END as zonecorn_in2,
CASE WHEN ( ( px BETWEEN @xmin - (2/12) AND @xmin + (2/12) AND pz BETWEEN @zmin - (2/12) AND @zmin + (2/12) ) OR ( px BETWEEN @xmax - (2/12) AND @xmax + (2/12) AND pz BETWEEN @zmin - (2/12) AND @zmin + (2/12) ) OR ( px BETWEEN @xmin - (2/12) AND @xmin + (2/12) AND pz BETWEEN @zmax - (2/12) AND @zmax + (2/12) ) OR ( px BETWEEN @xmax - (2/12) AND @xmax + (2/12) AND pz BETWEEN @zmax - (2/12) AND @zmax + (2/12) ) ) AND px NOT BETWEEN @xmin AND @xmax AND pz NOT BETWEEN @zmin AND @zmax THEN 1 ELSE 0 END as zonecorn_out2,
CASE WHEN ( px BETWEEN @xmin AND @xmax AND pz BETWEEN @zmin AND @zmax AND (px <= @xmin + (4/12) OR px >= @xmax - (4/12)) AND (pz <= @zmin + (4/12) OR pz >= @zmax - (4/12)) ) THEN 1 ELSE 0 END as zonecorn_in4,
CASE WHEN ( ( px BETWEEN @xmin - (4/12) AND @xmin + (4/12) AND pz BETWEEN @zmin - (4/12) AND @zmin + (4/12) ) OR ( px BETWEEN @xmax - (4/12) AND @xmax + (4/12) AND pz BETWEEN @zmin - (4/12) AND @zmin + (4/12) ) OR ( px BETWEEN @xmin - (4/12) AND @xmin + (4/12) AND pz BETWEEN @zmax - (4/12) AND @zmax + (4/12) ) OR ( px BETWEEN @xmax - (4/12) AND @xmax + (4/12) AND pz BETWEEN @zmax - (4/12) AND @zmax + (4/12) ) ) AND px NOT BETWEEN @xmin AND @xmax AND pz NOT BETWEEN @zmin AND @zmax THEN 1 ELSE 0 END as zonecorn_out4,
CASE WHEN (px BETWEEN @xmin + (@xmax-@xmin)/2 - (1.5/12) AND @xmin + (@xmax-@xmin)/2 + (1.5/12) AND pz BETWEEN @zmin + (@zmax-@zmin)/2 - (1.5/12) AND @zmin + (@zmax-@zmin)/2 + (1.5/12)) THEN 1 ELSE 0 END as zone_mid3,
CASE WHEN (px BETWEEN @xmin + (@xmax-@xmin)/2 - (3/12) AND @xmin + (@xmax-@xmin)/2 + (3/12) AND pz BETWEEN @zmin + (@zmax-@zmin)/2 - (3/12) AND @zmin + (@zmax-@zmin)/2 + (3/12)) THEN 1 ELSE 0 END as zone_mid6,
CASE WHEN ( (px NOT BETWEEN @xmin - (4/12) AND @xmax + (4/12) ) OR ( pz NOT BETWEEN @zmin - (4/12) AND @zmax + (4/12) ) ) THEN 1 ELSE 0 END as zone_bigmiss4,

CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN end_speed END as fastball_endspeed,
CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN spin_rate END as fastball_spinrate,
CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN spin_dir END as fastball_spindir,
CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN pfx_x END as fastball_pfx_x,
CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN pfx_z END as fastball_pfx_z,
CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z,2)) END as fastball_mnorm,
CASE WHEN p.pitch_type IN ('FF','FT','FC','FS') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z-9,2)) END as fastball_adjmnorm,

CASE WHEN p.pitch_type IN ('CU','KC') THEN end_speed END as curveball_endspeed,
CASE WHEN p.pitch_type IN ('CU','KC') THEN spin_rate END as curveball_spinrate,
CASE WHEN p.pitch_type IN ('CU','KC') THEN spin_dir END as curveball_spindir,
CASE WHEN p.pitch_type IN ('CU','KC') THEN pfx_x END as curveball_pfx_x,
CASE WHEN p.pitch_type IN ('CU','KC') THEN pfx_z END as curveball_pfx_z,
CASE WHEN p.pitch_type IN ('CU','KC') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z,2)) END as curveball_mnorm,
CASE WHEN p.pitch_type IN ('CU','KC') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z-9,2)) END as curveball_adjmnorm,

CASE WHEN p.pitch_type IN ('SL','SC') THEN end_speed END as slider_endspeed,
CASE WHEN p.pitch_type IN ('SL','SC') THEN spin_rate END as slider_spinrate,
CASE WHEN p.pitch_type IN ('SL','SC') THEN spin_dir END as slider_spindir,
CASE WHEN p.pitch_type IN ('SL','SC') THEN pfx_x END as slider_pfx_x,
CASE WHEN p.pitch_type IN ('SL','SC') THEN pfx_z END as slider_pfx_z,
CASE WHEN p.pitch_type IN ('SL','SC') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z,2)) END as slider_mnorm,
CASE WHEN p.pitch_type IN ('SL','SC') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z-9,2)) END as slider_adjmnorm,

CASE WHEN p.pitch_type IN ('CH','SI') THEN end_speed END as changeup_endspeed,
CASE WHEN p.pitch_type IN ('CH','SI') THEN spin_rate END as changeup_spinrate,
CASE WHEN p.pitch_type IN ('CH','SI') THEN spin_dir END as changeup_spindir,
CASE WHEN p.pitch_type IN ('CH','SI') THEN pfx_x END as changeup_pfx_x,
CASE WHEN p.pitch_type IN ('CH','SI') THEN pfx_z END as changeup_pfx_z,
CASE WHEN p.pitch_type IN ('CH','SI') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z,2)) END as changeup_mnorm,
CASE WHEN p.pitch_type IN ('CH','SI') THEN sqrt(POWER(pfx_x,2) + POWER(pfx_z-9,2)) END as changeup_adjmnorm

FROM pfx_pitch AS p
LEFT JOIN pfx_atbat AS ab
ON p.gid = ab.gid AND p.ab_num = ab.ab_num
LEFT JOIN pfx_game AS g
ON p.gid = g.gid

;

ALTER TABLE analbase_pitch ADD UNIQUE KEY (gid,game_date,pitch_id);
ALTER TABLE analbase_pitch ADD INDEX (ab_num);
ALTER TABLE analbase_pitch ADD INDEX (game_date);
ALTER TABLE analbase_pitch ADD COLUMN lastmodifieddate TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE analbase_pitch ADD COLUMN createddate timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;


ALTER TABLE analbase_pitch 
PARTITION BY RANGE ( YEAR(game_date) ) (
    PARTITION p2008 VALUES LESS THAN (2009),
    PARTITION p2009 VALUES LESS THAN (2010),
    PARTITION p2010 VALUES LESS THAN (2011),
    PARTITION p2011 VALUES LESS THAN (2012),
    PARTITION p2012 VALUES LESS THAN (2013),
    PARTITION p2013 VALUES LESS THAN (2014),
    PARTITION p2014 VALUES LESS THAN (2015),
    PARTITION p2015 VALUES LESS THAN (2016),
    PARTITION p2016 VALUES LESS THAN (2017),
    PARTITION p2017 VALUES LESS THAN (2018),
    PARTITION p2018 VALUES LESS THAN (2019),
    PARTITION p2019 VALUES LESS THAN (2020),
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022),
    PARTITION p2022 VALUES LESS THAN MAXVALUE
    );

select * from analbase_pitch limit 2000

