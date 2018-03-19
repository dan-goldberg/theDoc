CREATE VIEW mlb_pitch AS
(SELECT 
a.*
, b.batter
, b.stand
, b.pitcher
, b.p_throws
, b.catcher
, b.umpire
, b.effective_speed
, b.release_spin_rate
, b.release_extension
, b.hc_x
, b.hc_y
, b.hit_distance_sc
, b.hit_speed
, b.hit_angle
FROM pfx_pitch as a
LEFT JOIN pfx_basesav as b
ON a.game_pk = b.game_pk
AND a.id_ = b.pitch_id
);


CREATE VIEW mlb_lastpitch AS
(Select 
game_pk, 
ab_num, 
max(id_) as id_ 
FROM pfx_pitch 
GROUP BY 1,2);


drop view mlb_atbat;
CREATE VIEW mlb_atbat AS
(
SELECT 
#fields from pfx_atbat
c.gid
,c.game_pk
,c.game_date                                       
,c.inn_num
,c.inn_half
,c.home_bat_fl
,c.ab_num
,c.b
,c.s
,c.o
,c.start_tfs
,c.start_tfs_zulu
,c.batter as batter
,c.stand as stand
,c.b_height as b_height
,c.pitcher as pitcher
,c.p_throws as pitcher_throws
,c.des as des
,c.event_num as event_num
,c.event_
,c.event2
,c.score
,c.home_team_runs
,c.away_team_runs
,c.play_guid as play_guid
,c.x_atbat_id
#fields from baseball savant
, a.batter as batter_f
, a.stand as stand_f
, a.pitcher as pitcher_f
, a.p_throws as p_throws_f
, a.catcher
, a.umpire
, a.effective_speed
, a.release_spin_rate
, a.release_extension
, a.hc_x
, a.hc_y
, a.hit_distance_sc
, a.hit_speed
, a.hit_angle
#fields from pfx_pitch
,a.ab_pitch_num                        
,a.des as des_p 
,a.id_
,a.type_ 
,a.tfs
,a.tfs_zulu 
,a.x 
,a.y 
,a.event_num as event_num_p
,a.on_1b
,a.on_2b
,a.on_3b
,a.sv_id 
,a.start_speed 
,a.end_speed
,a.sz_top
,a.sz_bot
,a.pfx_x
,a.pfx_z
,a.px 
,a.pz 
,a.x0 
,a.z0 
,a.y0 
,a.vx0 
,a.vy0 
,a.vz0 
,a.ax 
,a.ay 
,a.az 
,a.break_y 
,a.break_angle 
,a.break_length 
,a.pitch_type 
,a.type_confidence 
,a.zone
,a.nasty
,a.spin_dir 
,a.spin_rate 
,a.cc 
,a.mt 
,a.play_guid as play_guid_p
,a.x_pitch_id

FROM mlb_pitch as a 
JOIN mlb_lastpitch as b 
ON a.game_pk = b.game_pk 
and a.ab_num = b.ab_num
and a.id_ = b.id_
RIGHT JOIN pfx_atbat as c
ON c.game_pk = a.game_pk
AND c.ab_num = a.ab_num
);

DROP VIEW IF EXISTS mlb_game;
CREATE VIEW mlb_game AS (
SELECT
a.gid as gid_g
,a.game_type
,a.game_pk
,a.game_date as game_date_g
,a.game_time_et
,a.local_game_time
,a.gameday_sw
,a.stad_id
,a.stad_name
,a.stad_location
,a.venue_w_chan_loc
,a.home_id
,a.home_name_full
,a.home_abbrev
,a.home_code
,a.home_division_id
,a.home_w
,a.home_l
,a.home_league_id
,a.home_league
,a.away_id
,a.away_name_full
,a.away_abbrev
,a.away_code
,a.away_division_id
,a.away_w
,a.away_l
,a.away_league_id
,a.away_league

,b.game_status
,b.away_score
,b.home_score
,b.away_player_id as away_prob_player_id
,b.away_throwinghand as away_prob_throwinghand
,b.away_wins as away_prob_wins
,b.away_losses as away_prob_losses
,b.away_era as away_prob_era
,b.away_so as away_prob_so
,b.home_player_id as home_prob_player_id
,b.home_throwinghand as home_prob_throwinghand
,b.home_wins as home_prob_wins
,b.home_losses as home_prob_losses
,b.home_era as home_prob_era
,b.home_so as home_prob_so


FROM pfx_game as a
JOIN pfx_prob as b
ON b.gid = a.gid
WHERE b.game_status = 'F'

);

select distinct game_status from pfx_prob

select * from pfx_prob where game_status = 'FR'