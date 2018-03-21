use mlb;

#select * from pfx_atbat order by rand();


#atbat table opps and events

DROP TABLE IF EXISTS analbase_atbat ;
CREATE TABLE analbase_atbat AS

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
END) as runsscored,

##### ids
ab.gid,
ab.game_pk,
ab.ab_num,
batter,
pitcher,
ab.event_num,
##### opportunities
ab.game_date,
ab.inn_num,
ab.home_bat_fl,
des,
stand as bat_side,
p_throws,
CASE WHEN stand = p_throws THEN 1 ELSE 0 END as p_same_hand,
o - CASE 
	WHEN (ab.event_ LIKE '% Out' AND ab.event_ <> 'Runner Out') OR ab.event_ LIKE '%out' THEN 1
	WHEN ab.event_ LIKE 'Double Play' OR ab.event_ LIKE '% DP' THEN 2
    WHEN ab.event_ LIKE 'Triple Play' THEN 3
    ELSE 0
END - CASE WHEN event2 LIKE '%Out' THEN 1 ELSE 0 END
as o_sit,
CASE 
	WHEN ab.home_bat_fl = 1 THEN (ab.home_teamruns - @runsscored) - ab.away_teamruns
    ELSE (ab.away_teamruns - @runsscored)- ab.home_teamruns
END as team_lead,

CASE WHEN r1.id_ IS NOT NULL THEN 1 ELSE 0 END as r1b_fl,
CASE WHEN r2.id_ IS NOT NULL THEN 1 ELSE 0 END as r2b_fl,
CASE WHEN r3.id_ IS NOT NULL THEN 1 ELSE 0 END as r3b_fl,


##### events


CASE 
	WHEN (ab.event_ LIKE '% Out' AND ab.event_ <> 'Runner Out') OR ab.event_ LIKE '%out' THEN 1
	WHEN ab.event_ LIKE 'Double Play' OR ab.event_ LIKE '% DP' THEN 2
    WHEN ab.event_ LIKE 'Triple Play' THEN 3
    ELSE 0
END as outsmade,
CASE WHEN ab.event_ = 'Single' THEN 1 ELSE 0 END as _1b_fl,
CASE WHEN ab.event_ = 'Double' THEN 1 ELSE 0 END as _2b_fl,
CASE WHEN ab.event_ = 'Triple' THEN 1 ELSE 0 END as _3b_fl,
CASE WHEN ab.event_ = 'Home Run' OR ab.des LIKE '%home run%' OR ab.des LIKE '%grand slam%' THEN 1 ELSE 0 END as _hr_fl,
CASE WHEN ab.event_ = 'Walk' OR ab.event_ = 'Intent Walk' THEN 1 ELSE 0 END as _bb_fl,
CASE WHEN ab.event_ = 'Strikeout' THEN 1 ELSE 0 END as _k_fl,
CASE WHEN ab.des LIKE '%strikes out%' THEN 1 ELSE 0 END as _kswing_fl,
CASE WHEN ab.des LIKE '%called out on strikes%' THEN 1 ELSE 0 END as _klook_fl,
CASE WHEN ab.event_ = 'Hit By Pitch' THEN 1 ELSE 0 END as _hbp_fl,
CASE WHEN ab.event_ IN ('Sac Bunt','Sac Fly','Sac Fly DP','Sacrifice Bunt DP') THEN 1 ELSE 0 END as _sac_fl,
CASE WHEN ab.event_ = 'Field Error' THEN 1 ELSE 0 END as _e_fl,
CASE WHEN 
	des LIKE '%ground%' OR 
    ab.des LIKE '%reaches on a throwing error%' OR
    ab.event_ LIKE '%Bunt Groundout%'
THEN 1 ELSE 0 END as groundball_fl,
CASE WHEN 
	des LIKE '%line drive%' OR
    ab.des LIKE '%lines%'
THEN 1 ELSE 0 END as linedrive_fl,
CASE WHEN ab.score = 'T' THEN 1 ELSE 0 END as _score_fl,
b,
s,
case when o = 3 THEN 1 ELSE 0 END as endinn_fl,
@runnerbasesadv := CASE 
	WHEN r1.end_base = '2B' THEN 1
	WHEN r1.end_base = '3B' THEN 2
    WHEN r1.end_base = '' AND r1.score = 'T' THEN 3
    WHEN r1.end_base = '' AND r1.score = '' THEN -1
    WHEN r1.end_base IS NULL THEN 0
END 
+
CASE 
	WHEN r2.end_base = '3B' THEN 1
    WHEN r2.end_base = '' AND r2.score = 'T' THEN 2
    WHEN r2.end_base = '' AND r2.score = '' THEN -2
    WHEN r2.end_base IS NULL THEN 0
END 
+
CASE 
    WHEN r3.end_base = '' AND r3.score = 'T' THEN 1
    WHEN r3.end_base = '' AND r3.score = '' THEN -3
    WHEN r3.end_base IS NULL THEN 0
END
as runnerbasesadv,
@selfbasesadv := CASE
	WHEN r0.end_base = '1B' THEN 1 
	WHEN r0.end_base = '2B' THEN 2
	WHEN r0.end_base = '3B' THEN 3
    WHEN r0.end_base = '' AND r0.score = 'T' THEN 4
    ELSE 0
END
as selfbasesadv,
@runnerbasesadv + @selfbasesadv as totbasesadv,
CASE WHEN ab.home_bat_fl = 0 THEN (ab.pitcher = prob.home_player_id) ELSE (ab.pitcher = prob.away_player_id) END AS sp_fl


FROM pfx_atbat as ab
LEFT JOIN pfx_runner as r0
ON r0.gid = ab.gid AND r0.ab_num = ab.ab_num AND (r0.event_num = ab.event_num OR r0.event_ = ab.event_) AND r0.start_base = ''
LEFT JOIN pfx_runner as r1
ON r1.gid = ab.gid AND r1.ab_num = ab.ab_num AND (r1.event_num = ab.event_num OR r1.event_ = ab.event_) AND r1.start_base = '1B'
LEFT JOIN pfx_runner as r2
ON r2.gid = ab.gid AND r2.ab_num = ab.ab_num AND (r2.event_num = ab.event_num OR r2.event_ = ab.event_) AND r2.start_base = '2B'
LEFT JOIN pfx_runner as r3
ON r3.gid = ab.gid AND r3.ab_num = ab.ab_num AND (r3.event_num = ab.event_num OR r3.event_ = ab.event_) AND r3.start_base = '3B'
LEFT JOIN pfx_prob as prob
ON prob.gid = ab.gid

;

ALTER TABLE analbase_atbat ADD INDEX (gid);
ALTER TABLE analbase_atbat ADD INDEX (ab_num);
ALTER TABLE analbase_atbat ADD INDEX (game_date);
ALTER TABLE analbase_atbat ADD COLUMN lastmodifieddate TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP;
ALTER TABLE analbase_atbat ADD COLUMN createddate timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;


ALTER TABLE analbase_atbat
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
