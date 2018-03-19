use mlb;

DROP TRIGGER IF EXISTS trig_prob_1;
CREATE TRIGGER trig_prob_1
BEFORE INSERT
ON pfx_prob
FOR EACH ROW
SET
NEW.away_player_id = CASE WHEN NEW.away_player_id = 0 THEN NULL ELSE NEW.away_player_id END,
NEW.away_number = CASE WHEN NEW.away_number = 0 THEN NULL ELSE NEW.away_number END,
NEW.away_wins = CASE WHEN NEW.away_wins = 0 THEN NULL ELSE NEW.away_wins END,
NEW.away_losses = CASE WHEN NEW.away_losses = 0 THEN NULL ELSE NEW.away_losses END,
NEW.away_so = CASE WHEN NEW.away_so = 0 THEN NULL ELSE NEW.away_so END,
NEW.away_era = CASE WHEN NEW.away_era = '-.--' THEN NULL ELSE NEW.away_era END,
NEW.away_std_wins = CASE WHEN NEW.away_std_wins = 0 THEN NULL ELSE NEW.away_std_wins END,
NEW.away_std_losses = CASE WHEN NEW.away_std_losses = 0 THEN NULL ELSE NEW.away_std_losses END,
NEW.away_std_so = CASE WHEN NEW.away_std_so = 0 THEN NULL ELSE NEW.away_std_so END,
NEW.away_std_era = CASE WHEN NEW.away_std_era = '-.--' THEN NULL ELSE NEW.away_std_era END,

NEW.home_player_id = CASE WHEN NEW.home_player_id = 0 THEN NULL ELSE NEW.home_player_id END,
NEW.home_number = CASE WHEN NEW.home_number = 0 THEN NULL ELSE NEW.home_number END,
NEW.home_wins = CASE WHEN NEW.home_wins = 0 THEN NULL ELSE NEW.home_wins END,
NEW.home_losses = CASE WHEN NEW.home_losses = 0 THEN NULL ELSE NEW.home_losses END,
NEW.home_so = CASE WHEN NEW.home_so = 0 THEN NULL ELSE NEW.home_so END,
NEW.home_era = CASE WHEN NEW.home_era = '-.--' THEN NULL ELSE NEW.home_era END,
NEW.home_std_wins = CASE WHEN NEW.home_std_wins = 0 THEN NULL ELSE NEW.home_std_wins END,
NEW.home_std_losses = CASE WHEN NEW.home_std_losses = 0 THEN NULL ELSE NEW.home_std_losses END,
NEW.home_std_so = CASE WHEN NEW.home_std_so = 0 THEN NULL ELSE NEW.home_std_so END,
NEW.home_std_era = CASE WHEN NEW.home_std_era = '-.--' THEN NULL ELSE NEW.home_std_era END
/*
WHERE NEW.away_player_id = 0
OR NEW.home_player_id = 0
OR NEW.away_std_era = '-.--'
OR NEW.away_era = '-.--'
OR NEW.home_std_era = '-.--'
OR NEW.home_era = '-.--'
*/
;

DROP TRIGGER IF EXISTS trig_pitch_1;
CREATE TRIGGER trig_pitch_1
BEFORE INSERT
ON pfx_pitch
FOR EACH ROW
SET
NEW.start_speed = CASE WHEN NEW.start_speed = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.start_speed END ,
NEW.end_speed = CASE WHEN NEW.end_speed = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.end_speed END ,
NEW.sz_top = CASE WHEN NEW.sz_top = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.sz_top END ,
NEW.sz_bot = CASE WHEN NEW.sz_bot = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.sz_bot END ,
NEW.pfx_x = CASE WHEN NEW.pfx_x = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.pfx_x END ,
NEW.pfx_z = CASE WHEN NEW.pfx_z = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.pfx_z END ,
NEW.px = CASE WHEN NEW.px = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.px END ,
NEW.pz = CASE WHEN NEW.pz = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.pz END ,
NEW.x0 = CASE WHEN NEW.x0 = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.x0 END ,
NEW.z0 = CASE WHEN NEW.z0 = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.z0 END ,
NEW.y0 = CASE WHEN NEW.y0 = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.y0 END ,
NEW.vx0 = CASE WHEN NEW.vx0 = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.vx0 END ,
NEW.vy0 = CASE WHEN NEW.vy0 = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.vy0 END ,
NEW.vz0 = CASE WHEN NEW.vz0 = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.vz0 END ,
NEW.ax = CASE WHEN NEW.ax = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.ax END ,
NEW.ay = CASE WHEN NEW.ay = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.ay END ,
NEW.az = CASE WHEN NEW.az = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.az END ,
NEW.break_y = CASE WHEN NEW.break_y = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.break_y END ,
NEW.break_angle = CASE WHEN NEW.break_angle = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.break_angle END ,
NEW.break_length = CASE WHEN NEW.break_length = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.break_length END ,
NEW.type_confidence = CASE WHEN NEW.type_confidence = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.type_confidence END ,
NEW.zone = CASE WHEN NEW.zone = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.zone END ,
NEW.nasty = CASE WHEN NEW.nasty = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.nasty END ,
NEW.spin_dir = CASE WHEN NEW.spin_dir = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.spin_dir END ,
NEW.spin_rate = CASE WHEN NEW.spin_rate = 0 AND NEW.sv_id = '' THEN NULL ELSE NEW.spin_rate END,
NEW.event_num = CASE WHEN NEW.event_num = 0 THEN NULL ELSE NEW.event_num END,
NEW.on_1b = CASE WHEN NEW.on_1b = 0 THEN NULL ELSE NEW.on_1b END,
NEW.on_2b = CASE WHEN NEW.on_2b = 0 THEN NULL ELSE NEW.on_2b END,
NEW.on_3b = CASE WHEN NEW.on_3b = 0 THEN NULL ELSE NEW.on_3b END
/*
WHERE
(NEW.start_speed = 0 AND NEW.sv_id = '' ) OR (
NEW.end_speed = 0 AND NEW.sv_id = '' ) OR (
NEW.sz_top = 0 AND NEW.sv_id = '' ) OR (
NEW.sz_bot = 0 AND NEW.sv_id = '' ) OR (
NEW.pfx_x = 0 AND NEW.sv_id = '' ) OR (
NEW.pfx_z = 0 AND NEW.sv_id = '' ) OR (
NEW.px = 0 AND NEW.sv_id = '' ) OR (
NEW.pz = 0 AND NEW.sv_id = '' ) OR (
NEW.x0 = 0 AND NEW.sv_id = '' ) OR (
NEW.z0 = 0 AND NEW.sv_id = '' ) OR (
NEW.y0 = 0 AND NEW.sv_id = '' ) OR (
NEW.vx0 = 0 AND NEW.sv_id = '' ) OR (
NEW.vy0 = 0 AND NEW.sv_id = '' ) OR (
NEW.vz0 = 0 AND NEW.sv_id = '' ) OR (
NEW.ax = 0 AND NEW.sv_id = '' ) OR (
NEW.ay = 0 AND NEW.sv_id = '' ) OR (
NEW.az = 0 AND NEW.sv_id = '' ) OR (
NEW.break_y = 0 AND NEW.sv_id = '' ) OR (
NEW.break_angle = 0 AND NEW.sv_id = '' ) OR (
NEW.break_length = 0 AND NEW.sv_id = '' ) OR (
NEW.type_confidence = 0 AND NEW.sv_id = '' ) OR (
NEW.zone = 0 AND NEW.sv_id = '' ) OR (
NEW.nasty = 0 AND NEW.sv_id = '' ) OR (
NEW.spin_dir = 0 AND NEW.sv_id = '' ) OR (
NEW.spin_rate = 0 AND NEW.sv_id = '')
OR NEW.event_num = 0
OR NEW.on_1b = 0
OR NEW.on_2b = 0
OR NEW.on_3b = 0
*/
;

DROP TRIGGER IF EXISTS trig_betting_1;
CREATE TRIGGER trig_betting_1
BEFORE INSERT
ON betting_scrape
FOR EACH ROW
SET
	NEW.spread_away_points = CASE WHEN NEW.spread_away_points = 0 THEN NULL ELSE NEW.spread_away_points END,
	NEW.spread_away_odds = CASE WHEN NEW.spread_away_odds = 0 THEN NULL ELSE NEW.spread_away_odds END,
	NEW.spread_home_points = CASE WHEN NEW.spread_home_points = 0 THEN NULL ELSE NEW.spread_home_points END,
	NEW.spread_home_odds = CASE WHEN NEW.spread_home_odds = 0 THEN NULL ELSE NEW.spread_home_odds END,
	NEW.money_away_odds = CASE WHEN NEW.money_away_odds = 0 THEN NULL ELSE NEW.money_away_odds END,
	NEW.money_home_odds = CASE WHEN NEW.money_home_odds = 0 THEN NULL ELSE NEW.money_home_odds END,
	NEW.total_points = CASE WHEN NEW.total_points = 0 THEN NULL ELSE NEW.total_points END,
	NEW.total_over = CASE WHEN NEW.total_over = 0 THEN NULL ELSE NEW.total_over END,
	NEW.total_under = CASE WHEN NEW.total_under = 0 THEN NULL ELSE NEW.total_under END,
    NEW.most_recent = 'Y'
/*
WHERE 
    NEW.spread_away_points = 0 OR
    NEW.spread_away_odds = 0 OR
    NEW.spread_home_points = 0 OR
    NEW.spread_home_odds = 0 OR
    NEW.money_away_odds = 0 OR
    NEW.money_home_odds = 0 OR
    NEW.total_points = 0 OR
    NEW.total_over = 0 OR
    NEW.total_under = 0
*/
    ;

DROP TRIGGER IF EXISTS trig_atbat_1;
CREATE TRIGGER trig_atbat_1
BEFORE INSERT
ON pfx_atbat
FOR EACH ROW
SET NEW.event_num = CASE WHEN NEW.event_num = 0 THEN NULL ELSE NEW.event_num END;


DROP TRIGGER IF EXISTS trig_action_1;
CREATE TRIGGER trig_action_1
BEFORE INSERT
ON pfx_action
FOR EACH ROW
SET NEW.event_num = CASE WHEN NEW.event_num = 0 THEN NULL ELSE NEW.event_num END;


DROP TRIGGER IF EXISTS trig_pickoff_1;
CREATE TRIGGER trig_pickoff_1
BEFORE INSERT
ON pfx_pickoff
FOR EACH ROW
SET NEW.event_num = CASE WHEN NEW.event_num = 0 THEN NULL ELSE NEW.event_num END;


DROP TRIGGER IF EXISTS trig_runner_1;
CREATE TRIGGER trig_runner_1
BEFORE INSERT
ON pfx_runner
FOR EACH ROW
SET NEW.event_num = CASE WHEN NEW.event_num = 0 THEN NULL ELSE NEW.event_num END;

DROP TRIGGER IF EXISTS trig_basesav_1;
CREATE TRIGGER trig_basesav_1
BEFORE INSERT
ON pfx_basesav
FOR EACH ROW
SET
NEW.hit_distance_sc = CASE WHEN NEW.hit_distance_sc = 0 THEN NULL ELSE NEW.hit_distance_sc END,
NEW.hit_angle = CASE WHEN NEW.hit_angle = 0 THEN NULL ELSE NEW.hit_angle END
;

DROP TRIGGER IF EXISTS trig_miniscore_1;
CREATE TRIGGER trig_miniscore_1
BEFORE INSERT
ON pfx_miniscore
FOR EACH ROW
SET
NEW.home_games_back_CLEAN = CASE WHEN NEW.home_games_back = '-' or NEW.home_games_back = '' or NEW.home_games_back LIKE '+%' THEN 0 ELSE CAST(NEW.home_games_back AS DECIMAL) END,
NEW.away_games_back_CLEAN = CASE WHEN NEW.away_games_back = '-' or NEW.away_games_back = '' or NEW.away_games_back LIKE '+%' THEN 0 ELSE CAST(NEW.away_games_back AS DECIMAL) END,
NEW.home_games_back_wildcard_CLEAN = CASE WHEN NEW.home_games_back_wildcard = '-' or NEW.home_games_back_wildcard = '' or NEW.home_games_back_wildcard LIKE '+%' THEN 0 ELSE CAST(NEW.home_games_back_wildcard AS DECIMAL) END,
NEW.away_games_back_wildcard_CLEAN = CASE WHEN NEW.away_games_back_wildcard = '-' or NEW.away_games_back_wildcard = '' or NEW.away_games_back_wildcard LIKE '+%' THEN 0 ELSE CAST(NEW.away_games_back_wildcard AS DECIMAL) END
;

#TRIGGER NOT CREATED YET
DROP TRIGGER IF EXISTS trig_game_1;
/*
CREATE TRIGGER trig_game_1
BEFORE INSERT
ON pfx_game
FOR EACH ROW
SET
NEW.home_games_back_CLEAN = CASE WHEN NEW.home_games_back = '-' or NEW.home_games_back = '' or NEW.home_games_back LIKE '+%' THEN 0 ELSE CAST(NEW.home_games_back AS DECIMAL) END,
NEW.away_games_back_CLEAN = CASE WHEN NEW.away_games_back = '-' or NEW.away_games_back = '' or NEW.away_games_back LIKE '+%' THEN 0 ELSE CAST(NEW.away_games_back AS DECIMAL) END,
NEW.home_games_back_wildcard_CLEAN = CASE WHEN NEW.home_games_back_wildcard = '-' or NEW.home_games_back_wildcard = '' or NEW.home_games_back_wildcard LIKE '+%' THEN 0 ELSE CAST(NEW.home_games_back_wildcard AS DECIMAL) END,
NEW.away_games_back_wildcard_CLEAN = CASE WHEN NEW.away_games_back_wildcard = '-' or NEW.away_games_back_wildcard = '' or NEW.away_games_back_wildcard LIKE '+%' THEN 0 ELSE CAST(NEW.away_games_back_wildcard AS DECIMAL) END
;
*/



