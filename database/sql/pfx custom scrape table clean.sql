UPDATE pfx_prob
SET
away_score = NULL, 
home_score = NULL
WHERE away_score = 0 AND home_score = 0;

UPDATE pfx_prob
SET
away_player_id = NULL,
away_number = NULL,
away_wins = NULL,
away_losses = NULL,
away_so = NULL,
away_std_wins = NULL,
away_std_losses = NULL,
away_std_so = NULL
WHERE away_player_id = 0;

UPDATE pfx_prob
SET
home_player_id = NULL,
home_number = NULL,
home_wins = NULL,
home_losses = NULL,
home_so = NULL,
home_std_wins = NULL,
home_std_losses = NULL,
home_std_so = NULL
WHERE home_player_id = 0;

UPDATE pfx_prob
SET
away_std_era = NULL
WHERE away_std_era = '-.--';

UPDATE pfx_prob
SET
away_era = NULL
WHERE away_era = '-.--';

UPDATE pfx_prob
SET
home_era = NULL
WHERE home_era = '-.--';

UPDATE pfx_prob
SET
home_std_era = NULL
WHERE home_std_era = '-.--';

UPDATE pfx_pitch
SET event_num = NULL
WHERE event_num = 0;

UPDATE pfx_pitch
SET on_1b = NULL
WHERE on_1b = 0;

UPDATE pfx_pitch
SET on_2b = NULL
WHERE on_2b = 0;

UPDATE pfx_pitch
SET on_3b = NULL
WHERE on_3b = 0;


UPDATE pfx_pitch
SET
start_speed = CASE WHEN start_speed = 0 AND sv_id = '' THEN NULL ELSE start_speed END ,
end_speed = CASE WHEN end_speed = 0 AND sv_id = '' THEN NULL ELSE end_speed END ,
sz_top = CASE WHEN sz_top = 0 AND sv_id = '' THEN NULL ELSE sz_top END ,
sz_bot = CASE WHEN sz_bot = 0 AND sv_id = '' THEN NULL ELSE sz_bot END ,
pfx_x = CASE WHEN pfx_x = 0 AND sv_id = '' THEN NULL ELSE pfx_x END ,
pfx_z = CASE WHEN pfx_z = 0 AND sv_id = '' THEN NULL ELSE pfx_z END ,
px = CASE WHEN px = 0 AND sv_id = '' THEN NULL ELSE px END ,
pz = CASE WHEN pz = 0 AND sv_id = '' THEN NULL ELSE pz END ,
x0 = CASE WHEN x0 = 0 AND sv_id = '' THEN NULL ELSE x0 END ,
z0 = CASE WHEN z0 = 0 AND sv_id = '' THEN NULL ELSE z0 END ,
y0 = CASE WHEN y0 = 0 AND sv_id = '' THEN NULL ELSE y0 END ,
vx0 = CASE WHEN vx0 = 0 AND sv_id = '' THEN NULL ELSE vx0 END ,
vy0 = CASE WHEN vy0 = 0 AND sv_id = '' THEN NULL ELSE vy0 END ,
vz0 = CASE WHEN vz0 = 0 AND sv_id = '' THEN NULL ELSE vz0 END ,
ax = CASE WHEN ax = 0 AND sv_id = '' THEN NULL ELSE ax END ,
ay = CASE WHEN ay = 0 AND sv_id = '' THEN NULL ELSE ay END ,
az = CASE WHEN az = 0 AND sv_id = '' THEN NULL ELSE az END ,
break_y = CASE WHEN break_y = 0 AND sv_id = '' THEN NULL ELSE break_y END ,
break_angle = CASE WHEN break_angle = 0 AND sv_id = '' THEN NULL ELSE break_angle END ,
break_length = CASE WHEN break_length = 0 AND sv_id = '' THEN NULL ELSE break_length END ,
type_confidence = CASE WHEN type_confidence = 0 AND sv_id = '' THEN NULL ELSE type_confidence END ,
zone = CASE WHEN zone = 0 AND sv_id = '' THEN NULL ELSE zone END ,
nasty = CASE WHEN nasty = 0 AND sv_id = '' THEN NULL ELSE nasty END ,
spin_dir = CASE WHEN spin_dir = 0 AND sv_id = '' THEN NULL ELSE spin_dir END ,
spin_rate = CASE WHEN spin_rate = 0 AND sv_id = '' THEN NULL ELSE spin_rate END
;

UPDATE pfx_atbat
SET event_num = NULL
WHERE event_num = 0;

UPDATE pfx_action
SET event_num = NULL
WHERE event_num = 0;

UPDATE pfx_pickoff
SET event_num = NULL
WHERE event_num = 0;

UPDATE pfx_runner
SET event_num = NULL
WHERE event_num = 0;

select 
* from pfx_runner where year(game_date) = 2010 order by rand() limit 10000
