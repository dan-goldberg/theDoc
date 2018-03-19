
/*
for side in ['home','away']:
    for n in range(10):
        print(
            "CREATE VIEW sl_"+side+"_"+str(n)+" AS \n(SELECT \ngid \n,id_ as "+side+"_"+str(n)+"_id \n,game_position as "+side+"_"+str(n)+"_pos \n,avg_ as "+side+"_"+str(n)+"_avg \n,hr as "+side+"_"+str(n)+"_hr \n,rbi as "+side+"_"+str(n)+"_rbi \nFROM \npfx_player \nWHERE side = '"+side+"' AND bat_order = "+str(n)+" \n);\n"
            #) as "+side+"_"+str(n)+" \nON a.gid = home_"+str(n)+".gid \n"
        )

*/

DROP VIEW IF EXISTS sl_home_sp;

CREATE VIEW sl_home_sp AS 
(SELECT 
gid 
,id_ as home_sp_id 
,wins as home_sp_wins
,losses as home_sp_losses 
,era as home_sp_era
FROM 
pfx_player 
WHERE side = 'home' AND game_position = 'P'
);

DROP VIEW IF EXISTS sl_away_sp;

CREATE VIEW sl_away_sp AS 
(SELECT 
gid 
,id_ as away_sp_id 
,wins as away_sp_wins
,losses as away_sp_losses 
,era as away_sp_era
FROM 
pfx_player 
WHERE side = 'away' AND game_position = 'P'
);

DROP VIEW IF EXISTS sl_home_1; 
 
 CREATE VIEW sl_home_1 AS 
(SELECT 
gid 
,id_ as home_1_id 
,game_position as home_1_pos 
,avg_ as home_1_avg 
,hr as home_1_hr 
,rbi as home_1_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 1 
);

DROP VIEW IF EXISTS sl_home_2; 
 
 CREATE VIEW sl_home_2 AS 
(SELECT 
gid 
,id_ as home_2_id 
,game_position as home_2_pos 
,avg_ as home_2_avg 
,hr as home_2_hr 
,rbi as home_2_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 2 
);

DROP VIEW IF EXISTS sl_home_3; 
 
 CREATE VIEW sl_home_3 AS 
(SELECT 
gid 
,id_ as home_3_id 
,game_position as home_3_pos 
,avg_ as home_3_avg 
,hr as home_3_hr 
,rbi as home_3_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 3 
);

DROP VIEW IF EXISTS sl_home_4; 
 
 CREATE VIEW sl_home_4 AS 
(SELECT 
gid 
,id_ as home_4_id 
,game_position as home_4_pos 
,avg_ as home_4_avg 
,hr as home_4_hr 
,rbi as home_4_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 4 
);

DROP VIEW IF EXISTS sl_home_5; 
 
 CREATE VIEW sl_home_5 AS 
(SELECT 
gid 
,id_ as home_5_id 
,game_position as home_5_pos 
,avg_ as home_5_avg 
,hr as home_5_hr 
,rbi as home_5_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 5 
);

DROP VIEW IF EXISTS sl_home_6; 
 
 CREATE VIEW sl_home_6 AS 
(SELECT 
gid 
,id_ as home_6_id 
,game_position as home_6_pos 
,avg_ as home_6_avg 
,hr as home_6_hr 
,rbi as home_6_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 6 
);

DROP VIEW IF EXISTS sl_home_7; 
 
 CREATE VIEW sl_home_7 AS 
(SELECT 
gid 
,id_ as home_7_id 
,game_position as home_7_pos 
,avg_ as home_7_avg 
,hr as home_7_hr 
,rbi as home_7_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 7 
);

DROP VIEW IF EXISTS sl_home_8; 
 
 CREATE VIEW sl_home_8 AS 
(SELECT 
gid 
,id_ as home_8_id 
,game_position as home_8_pos 
,avg_ as home_8_avg 
,hr as home_8_hr 
,rbi as home_8_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 8 
);

DROP VIEW IF EXISTS sl_home_9; 
 
 CREATE VIEW sl_home_9 AS 
(SELECT 
gid 
,id_ as home_9_id 
,game_position as home_9_pos 
,avg_ as home_9_avg 
,hr as home_9_hr 
,rbi as home_9_rbi 
FROM 
pfx_player 
WHERE side = 'home' AND bat_order = 9 
);

DROP VIEW IF EXISTS sl_away_1; 
 
 CREATE VIEW sl_away_1 AS 
(SELECT 
gid 
,id_ as away_1_id 
,game_position as away_1_pos 
,avg_ as away_1_avg 
,hr as away_1_hr 
,rbi as away_1_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 1 
);

DROP VIEW IF EXISTS sl_away_2; 
 
 CREATE VIEW sl_away_2 AS 
(SELECT 
gid 
,id_ as away_2_id 
,game_position as away_2_pos 
,avg_ as away_2_avg 
,hr as away_2_hr 
,rbi as away_2_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 2 
);

DROP VIEW IF EXISTS sl_away_3; 
 
 CREATE VIEW sl_away_3 AS 
(SELECT 
gid 
,id_ as away_3_id 
,game_position as away_3_pos 
,avg_ as away_3_avg 
,hr as away_3_hr 
,rbi as away_3_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 3 
);

DROP VIEW IF EXISTS sl_away_4; 
 
 CREATE VIEW sl_away_4 AS 
(SELECT 
gid 
,id_ as away_4_id 
,game_position as away_4_pos 
,avg_ as away_4_avg 
,hr as away_4_hr 
,rbi as away_4_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 4 
);

DROP VIEW IF EXISTS sl_away_5; 
 
 CREATE VIEW sl_away_5 AS 
(SELECT 
gid 
,id_ as away_5_id 
,game_position as away_5_pos 
,avg_ as away_5_avg 
,hr as away_5_hr 
,rbi as away_5_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 5 
);

DROP VIEW IF EXISTS sl_away_6; 
 
 CREATE VIEW sl_away_6 AS 
(SELECT 
gid 
,id_ as away_6_id 
,game_position as away_6_pos 
,avg_ as away_6_avg 
,hr as away_6_hr 
,rbi as away_6_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 6 
);

DROP VIEW IF EXISTS sl_away_7; 
 
 CREATE VIEW sl_away_7 AS 
(SELECT 
gid 
,id_ as away_7_id 
,game_position as away_7_pos 
,avg_ as away_7_avg 
,hr as away_7_hr 
,rbi as away_7_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 7 
);

DROP VIEW IF EXISTS sl_away_8; 
 
 CREATE VIEW sl_away_8 AS 
(SELECT 
gid 
,id_ as away_8_id 
,game_position as away_8_pos 
,avg_ as away_8_avg 
,hr as away_8_hr 
,rbi as away_8_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 8 
);

DROP VIEW IF EXISTS sl_away_9; 
 
 CREATE VIEW sl_away_9 AS 
(SELECT 
gid 
,id_ as away_9_id 
,game_position as away_9_pos 
,avg_ as away_9_avg 
,hr as away_9_hr 
,rbi as away_9_rbi 
FROM 
pfx_player 
WHERE side = 'away' AND bat_order = 9 
);



DROP VIEW IF EXISTS mlb_starting_lineup;

CREATE VIEW mlb_starting_lineup AS (
SELECT
a.gid
,a.game_date
,a.game_pk

,home_sp_id
,home_sp_wins
,home_sp_losses
,home_sp_era

,away_sp_id
,away_sp_wins
,away_sp_losses
,away_sp_era

,home_1_id
,home_1_pos
,home_1_avg
,home_1_hr
,home_1_rbi

,home_2_id
,home_2_pos
,home_2_avg
,home_2_hr
,home_2_rbi

,home_3_id
,home_3_pos
,home_3_avg
,home_3_hr
,home_3_rbi

,home_4_id
,home_4_pos
,home_4_avg
,home_4_hr
,home_4_rbi

,home_5_id
,home_5_pos
,home_5_avg
,home_5_hr
,home_5_rbi

,home_6_id
,home_6_pos
,home_6_avg
,home_6_hr
,home_6_rbi

,home_7_id
,home_7_pos
,home_7_avg
,home_7_hr
,home_7_rbi

,home_8_id
,home_8_pos
,home_8_avg
,home_8_hr
,home_8_rbi

,home_9_id
,home_9_pos
,home_9_avg
,home_9_hr
,home_9_rbi

,away_1_id
,away_1_pos
,away_1_avg
,away_1_hr
,away_1_rbi

,away_2_id
,away_2_pos
,away_2_avg
,away_2_hr
,away_2_rbi

,away_3_id
,away_3_pos
,away_3_avg
,away_3_hr
,away_3_rbi

,away_4_id
,away_4_pos
,away_4_avg
,away_4_hr
,away_4_rbi

,away_5_id
,away_5_pos
,away_5_avg
,away_5_hr
,away_5_rbi

,away_6_id
,away_6_pos
,away_6_avg
,away_6_hr
,away_6_rbi

,away_7_id
,away_7_pos
,away_7_avg
,away_7_hr
,away_7_rbi

,away_8_id
,away_8_pos
,away_8_avg
,away_8_hr
,away_8_rbi

,away_9_id
,away_9_pos
,away_9_avg
,away_9_hr
,away_9_rbi
FROM
pfx_game as a
LEFT JOIN
sl_home_sp ON sl_home_sp.gid = a.gid
LEFT JOIN
sl_away_sp ON sl_away_sp.gid = a.gid
LEFT JOIN
sl_home_1 ON sl_home_1.gid = a.gid
LEFT JOIN
sl_home_2 ON sl_home_2.gid = a.gid
LEFT JOIN
sl_home_3 ON sl_home_3.gid = a.gid
LEFT JOIN
sl_home_4 ON sl_home_4.gid = a.gid
LEFT JOIN
sl_home_5 ON sl_home_5.gid = a.gid
LEFT JOIN
sl_home_6 ON sl_home_6.gid = a.gid
LEFT JOIN
sl_home_7 ON sl_home_7.gid = a.gid
LEFT JOIN
sl_home_8 ON sl_home_8.gid = a.gid
LEFT JOIN
sl_home_9 ON sl_home_9.gid = a.gid
LEFT JOIN
sl_away_1 ON sl_away_1.gid = a.gid
LEFT JOIN
sl_away_2 ON sl_away_2.gid = a.gid
LEFT JOIN
sl_away_3 ON sl_away_3.gid = a.gid
LEFT JOIN
sl_away_4 ON sl_away_4.gid = a.gid
LEFT JOIN
sl_away_5 ON sl_away_5.gid = a.gid
LEFT JOIN
sl_away_6 ON sl_away_6.gid = a.gid
LEFT JOIN
sl_away_7 ON sl_away_7.gid = a.gid
LEFT JOIN
sl_away_8 ON sl_away_8.gid = a.gid
LEFT JOIN
sl_away_9 ON sl_away_9.gid = a.gid

)
;
use mlb;
select * from mlb_game as a 
LEFT JOIN mlb_starting_lineup as b 
ON a.gid = b.gid 
WHERE year(a.game_date) = 2016 
AND (home_abbrev = 'TOR' OR away_abbrev = 'TOR')
;