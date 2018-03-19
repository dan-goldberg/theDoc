DROP TABLE IF EXISTS anal_team_rate;

CREATE TABLE anal_team_rate AS

SELECT 

anal_game_date_off as anal_game_date,
team_id_off as team_id,
team_name_off as team_name,

##### std_off ######

@outsmade_pa := outsmade_std_off/pa_std_off as outsmade_pa_std_off,
@hits_pa := hits_std_off/pa_std_off as hits_pa_std_off,
@obp := onbases_std_off/pa_std_off as obp_std_off,
@tba_pa := totbasesadv_std_off/pa_std_off as tba_pa_std_off,
@tba_o := totbasesadv_std_off/outsmade_std_off as tba_o_std_off,
@runsscored_pa := runsscored_std_off/pa_std_off as runsscored_pa_std_off,

##### std_def ######

@outsmade_pa := outsmade_std_def/pa_std_def as outsmade_pa_std_def,
@hits_pa := hits_std_def/pa_std_def as hits_pa_std_def,
@obp := onbases_std_def/pa_std_def as obp_std_def,
@tba_pa := totbasesadv_std_def/pa_std_def as tba_pa_std_def,
@tba_o := totbasesadv_std_def/outsmade_std_def as tba_o_std_def,
@runsscored_pa := runsscored_std_def/pa_std_def as runsscored_pa_std_def,

##### std_sp_off ######

@outsmade_pa := outsmade_std_sp_off/pa_std_sp_off as outsmade_pa_std_sp_off,
@hits_pa := hits_std_sp_off/pa_std_sp_off as hits_pa_std_sp_off,
@obp := onbases_std_sp_off/pa_std_sp_off as obp_std_sp_off,
@tba_pa := totbasesadv_std_sp_off/pa_std_sp_off as tba_pa_std_sp_off,
@tba_o := totbasesadv_std_sp_off/outsmade_std_sp_off as tba_o_std_sp_off,
@runsscored_pa := runsscored_std_sp_off/pa_std_sp_off as runsscored_pa_std_sp_off,

##### std_sp_def ######

@outsmade_pa := outsmade_std_sp_def/pa_std_sp_def as outsmade_pa_std_sp_def,
@hits_pa := hits_std_sp_def/pa_std_sp_def as hits_pa_std_sp_def,
@obp := onbases_std_sp_def/pa_std_sp_def as obp_std_sp_def,
@tba_pa := totbasesadv_std_sp_def/pa_std_sp_def as tba_pa_std_sp_def,
@tba_o := totbasesadv_std_sp_def/outsmade_std_sp_def as tba_o_std_sp_def,
@runsscored_pa := runsscored_std_sp_def/pa_std_sp_def as runsscored_pa_std_sp_def,

##### std_rp_off ######

@outsmade_pa := outsmade_std_rp_off/pa_std_rp_off as outsmade_pa_std_rp_off,
@hits_pa := hits_std_rp_off/pa_std_rp_off as hits_pa_std_rp_off,
@obp := onbases_std_rp_off/pa_std_rp_off as obp_std_rp_off,
@tba_pa := totbasesadv_std_rp_off/pa_std_rp_off as tba_pa_std_rp_off,
@tba_o := totbasesadv_std_rp_off/outsmade_std_rp_off as tba_o_std_rp_off,
@runsscored_pa := runsscored_std_rp_off/pa_std_rp_off as runsscored_pa_std_rp_off,

##### std_rp_def ######

@outsmade_pa := outsmade_std_rp_def/pa_std_rp_def as outsmade_pa_std_rp_def,
@hits_pa := hits_std_rp_def/pa_std_rp_def as hits_pa_std_rp_def,
@obp := onbases_std_rp_def/pa_std_rp_def as obp_std_rp_def,
@tba_pa := totbasesadv_std_rp_def/pa_std_rp_def as tba_pa_std_rp_def,
@tba_o := totbasesadv_std_rp_def/outsmade_std_rp_def as tba_o_std_rp_def,
@runsscored_pa := runsscored_std_rp_def/pa_std_rp_def as runsscored_pa_std_rp_def,

##### std_home_off ######

@outsmade_pa := outsmade_std_home_off/pa_std_home_off as outsmade_pa_std_home_off,
@hits_pa := hits_std_home_off/pa_std_home_off as hits_pa_std_home_off,
@obp := onbases_std_home_off/pa_std_home_off as obp_std_home_off,
@tba_pa := totbasesadv_std_home_off/pa_std_home_off as tba_pa_std_home_off,
@tba_o := totbasesadv_std_home_off/outsmade_std_home_off as tba_o_std_home_off,
@runsscored_pa := runsscored_std_home_off/pa_std_home_off as runsscored_pa_std_home_off,

##### std_home_def ######

@outsmade_pa := outsmade_std_home_def/pa_std_home_def as outsmade_pa_std_home_def,
@hits_pa := hits_std_home_def/pa_std_home_def as hits_pa_std_home_def,
@obp := onbases_std_home_def/pa_std_home_def as obp_std_home_def,
@tba_pa := totbasesadv_std_home_def/pa_std_home_def as tba_pa_std_home_def,
@tba_o := totbasesadv_std_home_def/outsmade_std_home_def as tba_o_std_home_def,
@runsscored_pa := runsscored_std_home_def/pa_std_home_def as runsscored_pa_std_home_def,

##### std_away_off ######

@outsmade_pa := outsmade_std_away_off/pa_std_away_off as outsmade_pa_std_away_off,
@hits_pa := hits_std_away_off/pa_std_away_off as hits_pa_std_away_off,
@obp := onbases_std_away_off/pa_std_away_off as obp_std_away_off,
@tba_pa := totbasesadv_std_away_off/pa_std_away_off as tba_pa_std_away_off,
@tba_o := totbasesadv_std_away_off/outsmade_std_away_off as tba_o_std_away_off,
@runsscored_pa := runsscored_std_away_off/pa_std_away_off as runsscored_pa_std_away_off,

##### std_away_def ######

@outsmade_pa := outsmade_std_away_def/pa_std_away_def as outsmade_pa_std_away_def,
@hits_pa := hits_std_away_def/pa_std_away_def as hits_pa_std_away_def,
@obp := onbases_std_away_def/pa_std_away_def as obp_std_away_def,
@tba_pa := totbasesadv_std_away_def/pa_std_away_def as tba_pa_std_away_def,
@tba_o := totbasesadv_std_away_def/outsmade_std_away_def as tba_o_std_away_def,
@runsscored_pa := runsscored_std_away_def/pa_std_away_def as runsscored_pa_std_away_def,

##### last60_off ######

@outsmade_pa := outsmade_last60_off/pa_last60_off as outsmade_pa_last60_off,
@hits_pa := hits_last60_off/pa_last60_off as hits_pa_last60_off,
@obp := onbases_last60_off/pa_last60_off as obp_last60_off,
@tba_pa := totbasesadv_last60_off/pa_last60_off as tba_pa_last60_off,
@tba_o := totbasesadv_last60_off/outsmade_last60_off as tba_o_last60_off,
@runsscored_pa := runsscored_last60_off/pa_last60_off as runsscored_pa_last60_off,

##### last60_def ######

@outsmade_pa := outsmade_last60_def/pa_last60_def as outsmade_pa_last60_def,
@hits_pa := hits_last60_def/pa_last60_def as hits_pa_last60_def,
@obp := onbases_last60_def/pa_last60_def as obp_last60_def,
@tba_pa := totbasesadv_last60_def/pa_last60_def as tba_pa_last60_def,
@tba_o := totbasesadv_last60_def/outsmade_last60_def as tba_o_last60_def,
@runsscored_pa := runsscored_last60_def/pa_last60_def as runsscored_pa_last60_def,

##### last30_off ######

@outsmade_pa := outsmade_last30_off/pa_last30_off as outsmade_pa_last30_off,
@hits_pa := hits_last30_off/pa_last30_off as hits_pa_last30_off,
@obp := onbases_last30_off/pa_last30_off as obp_last30_off,
@tba_pa := totbasesadv_last30_off/pa_last30_off as tba_pa_last30_off,
@tba_o := totbasesadv_last30_off/outsmade_last30_off as tba_o_last30_off,
@runsscored_pa := runsscored_last30_off/pa_last30_off as runsscored_pa_last30_off,

##### last30_def ######

@outsmade_pa := outsmade_last30_def/pa_last30_def as outsmade_pa_last30_def,
@hits_pa := hits_last30_def/pa_last30_def as hits_pa_last30_def,
@obp := onbases_last30_def/pa_last30_def as obp_last30_def,
@tba_pa := totbasesadv_last30_def/pa_last30_def as tba_pa_last30_def,
@tba_o := totbasesadv_last30_def/outsmade_last30_def as tba_o_last30_def,
@runsscored_pa := runsscored_last30_def/pa_last30_def as runsscored_pa_last30_def,

##### last10_off ######

@outsmade_pa := outsmade_last10_off/pa_last10_off as outsmade_pa_last10_off,
@hits_pa := hits_last10_off/pa_last10_off as hits_pa_last10_off,
@obp := onbases_last10_off/pa_last10_off as obp_last10_off,
@tba_pa := totbasesadv_last10_off/pa_last10_off as tba_pa_last10_off,
@tba_o := totbasesadv_last10_off/outsmade_last10_off as tba_o_last10_off,
@runsscored_pa := runsscored_last10_off/pa_last10_off as runsscored_pa_last10_off,

##### last10_def ######

@outsmade_pa := outsmade_last10_def/pa_last10_def as outsmade_pa_last10_def,
@hits_pa := hits_last10_def/pa_last10_def as hits_pa_last10_def,
@obp := onbases_last10_def/pa_last10_def as obp_last10_def,
@tba_pa := totbasesadv_last10_def/pa_last10_def as tba_pa_last10_def,
@tba_o := totbasesadv_last10_def/outsmade_last10_def as tba_o_last10_def,
@runsscored_pa := runsscored_last10_def/pa_last10_def as runsscored_pa_last10_def,

##### last60_sp_off ######

@outsmade_pa := outsmade_last60_sp_off/pa_last60_sp_off as outsmade_pa_last60_sp_off,
@hits_pa := hits_last60_sp_off/pa_last60_sp_off as hits_pa_last60_sp_off,
@obp := onbases_last60_sp_off/pa_last60_sp_off as obp_last60_sp_off,
@tba_pa := totbasesadv_last60_sp_off/pa_last60_sp_off as tba_pa_last60_sp_off,
@tba_o := totbasesadv_last60_sp_off/outsmade_last60_sp_off as tba_o_last60_sp_off,
@runsscored_pa := runsscored_last60_sp_off/pa_last60_sp_off as runsscored_pa_last60_sp_off,

##### last60_sp_def ######

@outsmade_pa := outsmade_last60_sp_def/pa_last60_sp_def as outsmade_pa_last60_sp_def,
@hits_pa := hits_last60_sp_def/pa_last60_sp_def as hits_pa_last60_sp_def,
@obp := onbases_last60_sp_def/pa_last60_sp_def as obp_last60_sp_def,
@tba_pa := totbasesadv_last60_sp_def/pa_last60_sp_def as tba_pa_last60_sp_def,
@tba_o := totbasesadv_last60_sp_def/outsmade_last60_sp_def as tba_o_last60_sp_def,
@runsscored_pa := runsscored_last60_sp_def/pa_last60_sp_def as runsscored_pa_last60_sp_def,

##### last60_rp_off ######

@outsmade_pa := outsmade_last60_rp_off/pa_last60_rp_off as outsmade_pa_last60_rp_off,
@hits_pa := hits_last60_rp_off/pa_last60_rp_off as hits_pa_last60_rp_off,
@obp := onbases_last60_rp_off/pa_last60_rp_off as obp_last60_rp_off,
@tba_pa := totbasesadv_last60_rp_off/pa_last60_rp_off as tba_pa_last60_rp_off,
@tba_o := totbasesadv_last60_rp_off/outsmade_last60_rp_off as tba_o_last60_rp_off,
@runsscored_pa := runsscored_last60_rp_off/pa_last60_rp_off as runsscored_pa_last60_rp_off,

##### last60_rp_def ######

@outsmade_pa := outsmade_last60_rp_def/pa_last60_rp_def as outsmade_pa_last60_rp_def,
@hits_pa := hits_last60_rp_def/pa_last60_rp_def as hits_pa_last60_rp_def,
@obp := onbases_last60_rp_def/pa_last60_rp_def as obp_last60_rp_def,
@tba_pa := totbasesadv_last60_rp_def/pa_last60_rp_def as tba_pa_last60_rp_def,
@tba_o := totbasesadv_last60_rp_def/outsmade_last60_rp_def as tba_o_last60_rp_def,
@runsscored_pa := runsscored_last60_rp_def/pa_last60_rp_def as runsscored_pa_last60_rp_def,

##### last30_sp_off ######

@outsmade_pa := outsmade_last30_sp_off/pa_last30_sp_off as outsmade_pa_last30_sp_off,
@hits_pa := hits_last30_sp_off/pa_last30_sp_off as hits_pa_last30_sp_off,
@obp := onbases_last30_sp_off/pa_last30_sp_off as obp_last30_sp_off,
@tba_pa := totbasesadv_last30_sp_off/pa_last30_sp_off as tba_pa_last30_sp_off,
@tba_o := totbasesadv_last30_sp_off/outsmade_last30_sp_off as tba_o_last30_sp_off,
@runsscored_pa := runsscored_last30_sp_off/pa_last30_sp_off as runsscored_pa_last30_sp_off,

##### last30_sp_def ######

@outsmade_pa := outsmade_last30_sp_def/pa_last30_sp_def as outsmade_pa_last30_sp_def,
@hits_pa := hits_last30_sp_def/pa_last30_sp_def as hits_pa_last30_sp_def,
@obp := onbases_last30_sp_def/pa_last30_sp_def as obp_last30_sp_def,
@tba_pa := totbasesadv_last30_sp_def/pa_last30_sp_def as tba_pa_last30_sp_def,
@tba_o := totbasesadv_last30_sp_def/outsmade_last30_sp_def as tba_o_last30_sp_def,
@runsscored_pa := runsscored_last30_sp_def/pa_last30_sp_def as runsscored_pa_last30_sp_def,

##### last30_rp_off ######

@outsmade_pa := outsmade_last30_rp_off/pa_last30_rp_off as outsmade_pa_last30_rp_off,
@hits_pa := hits_last30_rp_off/pa_last30_rp_off as hits_pa_last30_rp_off,
@obp := onbases_last30_rp_off/pa_last30_rp_off as obp_last30_rp_off,
@tba_pa := totbasesadv_last30_rp_off/pa_last30_rp_off as tba_pa_last30_rp_off,
@tba_o := totbasesadv_last30_rp_off/outsmade_last30_rp_off as tba_o_last30_rp_off,
@runsscored_pa := runsscored_last30_rp_off/pa_last30_rp_off as runsscored_pa_last30_rp_off,

##### last30_rp_def ######

@outsmade_pa := outsmade_last30_rp_def/pa_last30_rp_def as outsmade_pa_last30_rp_def,
@hits_pa := hits_last30_rp_def/pa_last30_rp_def as hits_pa_last30_rp_def,
@obp := onbases_last30_rp_def/pa_last30_rp_def as obp_last30_rp_def,
@tba_pa := totbasesadv_last30_rp_def/pa_last30_rp_def as tba_pa_last30_rp_def,
@tba_o := totbasesadv_last30_rp_def/outsmade_last30_rp_def as tba_o_last30_rp_def,
@runsscored_pa := runsscored_last30_rp_def/pa_last30_rp_def as runsscored_pa_last30_rp_def,

##### last10_sp_off ######

@outsmade_pa := outsmade_last10_sp_off/pa_last10_sp_off as outsmade_pa_last10_sp_off,
@hits_pa := hits_last10_sp_off/pa_last10_sp_off as hits_pa_last10_sp_off,
@obp := onbases_last10_sp_off/pa_last10_sp_off as obp_last10_sp_off,
@tba_pa := totbasesadv_last10_sp_off/pa_last10_sp_off as tba_pa_last10_sp_off,
@tba_o := totbasesadv_last10_sp_off/outsmade_last10_sp_off as tba_o_last10_sp_off,
@runsscored_pa := runsscored_last10_sp_off/pa_last10_sp_off as runsscored_pa_last10_sp_off,

##### last10_sp_def ######

@outsmade_pa := outsmade_last10_sp_def/pa_last10_sp_def as outsmade_pa_last10_sp_def,
@hits_pa := hits_last10_sp_def/pa_last10_sp_def as hits_pa_last10_sp_def,
@obp := onbases_last10_sp_def/pa_last10_sp_def as obp_last10_sp_def,
@tba_pa := totbasesadv_last10_sp_def/pa_last10_sp_def as tba_pa_last10_sp_def,
@tba_o := totbasesadv_last10_sp_def/outsmade_last10_sp_def as tba_o_last10_sp_def,
@runsscored_pa := runsscored_last10_sp_def/pa_last10_sp_def as runsscored_pa_last10_sp_def,

##### last10_rp_off ######

@outsmade_pa := outsmade_last10_rp_off/pa_last10_rp_off as outsmade_pa_last10_rp_off,
@hits_pa := hits_last10_rp_off/pa_last10_rp_off as hits_pa_last10_rp_off,
@obp := onbases_last10_rp_off/pa_last10_rp_off as obp_last10_rp_off,
@tba_pa := totbasesadv_last10_rp_off/pa_last10_rp_off as tba_pa_last10_rp_off,
@tba_o := totbasesadv_last10_rp_off/outsmade_last10_rp_off as tba_o_last10_rp_off,
@runsscored_pa := runsscored_last10_rp_off/pa_last10_rp_off as runsscored_pa_last10_rp_off,

##### last10_rp_def ######

@outsmade_pa := outsmade_last10_rp_def/pa_last10_rp_def as outsmade_pa_last10_rp_def,
@hits_pa := hits_last10_rp_def/pa_last10_rp_def as hits_pa_last10_rp_def,
@obp := onbases_last10_rp_def/pa_last10_rp_def as obp_last10_rp_def,
@tba_pa := totbasesadv_last10_rp_def/pa_last10_rp_def as tba_pa_last10_rp_def,
@tba_o := totbasesadv_last10_rp_def/outsmade_last10_rp_def as tba_o_last10_rp_def,
@runsscored_pa := runsscored_last10_rp_def/pa_last10_rp_def as runsscored_pa_last10_rp_def

FROM anal_team_counting_def as d
JOIN anal_team_counting_off as o
ON o.anal_game_date_off = d.anal_game_date_def
AND o.team_id_off = d.team_id_def

ORDER BY anal_game_date_off DESC, runsscored_pa_std_off DESC
;

#ALTER TABLE anal_team_rate ADD COLUMN createddate timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
#ALTER TABLE anal_team_rate ADD COLUMN lastmodifiedddate timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP;
#ALTER TABLE anal_team_rate ADD COLUMN anal_pitcher_counting_def_PK int auto_increment primary key;

ALTER TABLE anal_team_rate ADD UNIQUE KEY (anal_game_date,team_id);
ALTER TABLE anal_team_rate ADD INDEX (team_id);



select * FROM anal_team_rate order by anal_game_date asc;