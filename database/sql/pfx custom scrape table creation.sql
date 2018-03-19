USE mlb;


DROP TABLE IF EXISTS pfx_game;

CREATE TABLE pfx_game (
gid varchar(31) primary key
,game_type varchar(1)
,game_pk int
,game_date date
,game_time_et time
,local_game_time time
,gameday_sw varchar(3)

,home_time time
,home_timezone varchar(5)
,away_time time
,away_timezone varchar(5)

,stad_id int
,stad_name varchar(30)
,stad_location varchar(30)
,venue_w_chan_loc varchar(8)

,home_name_full varchar(30)
,home_id int
,home_abbrev varchar(3)
,home_name_brief varchar(20)
,home_name varchar(20)
,home_division_id int
,home_file_code varchar(3)
,home_w int
,home_l int
,home_league varchar(2)
,home_type varchar(4)
,home_league_id int
,home_code varchar(3)

,away_name_full varchar(30)
,away_id int
,away_abbrev varchar(3)
,away_name_brief varchar(20)
,away_name varchar(20)
,away_division_id int
,away_file_code varchar(3)
,away_w int
,away_l int
,away_league varchar(2)
,away_type varchar(4)
,away_league_id int
,away_code varchar(3)

,double_header_sw varchar(5)
,game_nbr int
,status_ varchar(15)
,away_games_back float
,home_games_back float
,away_games_back_wildcard float
,home_games_back_wildcard float

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP

,index(game_pk)
,index(game_date)
,index(home_id)
,index(away_id)

);

alter table pfx_game add column double_header_sw varchar(5) after away_code;


DROP TABLE IF EXISTS pfx_prob;

CREATE TABLE pfx_prob (
gid varchar(31) primary key
,game_pk int
,game_date date
,game_status varchar(10)
,away_score int
,home_score int

,away_player_id int
,away_usename varchar(40)
,away_lastname varchar(30)
,away_rosterdisplayname varchar(30)
,away_number int
,away_throwinghand varchar(1)
,away_wins int
,away_losses int
,away_era varchar(6)
,away_so int
,away_std_wins int
,away_std_losses int
,away_std_era varchar(6)
,away_std_so int

,home_player_id int
,home_usename varchar(40)
,home_lastname varchar(30)
,home_rosterdisplayname varchar(30)
,home_number int
,home_throwinghand varchar(1)
,home_wins int
,home_losses int
,home_era varchar(6)
,home_so int
,home_std_wins int
,home_std_losses int
,home_std_era varchar(6)
,home_std_so int

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP

,index(game_pk)
,index(game_date)
,index(home_player_id)
,index(away_player_id)
);


DROP TABLE IF EXISTS pfx_action;

CREATE TABLE pfx_action (
gid varchar(31)
,game_pk int
,game_date date                                       
,inn_num int
,inn_half varchar(6)
,home_bat_fl int
,b int
,s int
,o int
,des varchar(150)
,des_es varchar(150)
,event_ varchar(50)
,event_es varchar(50)
,tfs int
,tfs_zulu varchar(25)
,player int
,pitch int
,event_num int
,home_team_runs int
,away_team_runs int
,play_guid varchar(36)

,x_action_id int auto_increment primary key

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP

,unique key (game_pk,event_num)
,index (player)
,index (event_num)
,index (pitch)
,index (play_guid)

)
;

DROP TABLE IF EXISTS pfx_atbat;

CREATE TABLE pfx_atbat (

gid varchar(31)
,game_pk int
,game_date date                                       
,inn_num int
,inn_half varchar(6)
,home_bat_fl int
,ab_num int
,b int
,s int
,o int
,start_tfs int
,start_tfs_zulu varchar(25)
,batter int
,stand varchar(4)
,b_height varchar(6)
,pitcher int
,p_throws varchar (4)
,des varchar(150)
,des_es varchar(150)
,event_num int
,event_ varchar(50)
,event_es varchar(50)
,event2 varchar(50)
,event2_es varchar(50)
,score varchar(4)
,home_team_runs int
,away_team_runs int
,play_guid varchar(36)

,x_atbat_id int auto_increment primary key

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
, unique key (gid,game_pk,ab_num)
, index(game_pk,ab_num)
, index(batter)
, index(pitcher)
, index(event_num)
)
;


DROP TABLE IF EXISTS pfx_pitch;

CREATE TABLE pfx_pitch (
gid varchar(31)
,game_pk int
,game_date date                                       
,inn_num int
,inn_half varchar(6)
,home_bat_fl int
,ab_num int
,ab_pitch_num int                        
,des varchar(150)
,des_es varchar(150)
,id_ int
,type_ varchar(4)
,tfs int
,tfs_zulu varchar(25)
,x float
,y float
,event_num int
,on_1b int
,on_2b int
,on_3b int
,sv_id varchar(13)
,start_speed float
,end_speed float
,sz_top float
,sz_bot float
,pfx_x float
,pfx_z float
,px float
,pz float
,x0 float
,z0 float
,y0 float
,vx0 float
,vy0 float
,vz0 float
,ax float
,ay float
,az float
,break_y float
,break_angle float
,break_length float
,pitch_type varchar(4)
,type_confidence float
,zone int
,nasty int
,spin_dir float
,spin_rate float
,cc varchar(20)
,mt varchar(20)
,play_guid varchar(36)

,x_pitch_id int auto_increment primary key

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP

,unique key (gid, id_)
,index(sv_id)
,index(game_pk)
,index(ab_num)
,index(event_num)
);


DROP TABLE IF EXISTS pfx_runner;

CREATE TABLE pfx_runner (

gid varchar(31)
,game_pk int
,game_date date                                       
,inn_num int
,inn_half varchar(6)
,home_bat_fl int
,ab_num int
,id_ int
,start_base varchar(4)
,end_base varchar(4)
,event_ varchar(50)
,event_num int
,score varchar(4)
,rbi varchar(4)
,earned varchar(4)

,x_runner_id int auto_increment primary key

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
,unique key (gid, id_, start_base, end_base, event_num, game_pk)
,index(game_pk)
,index(event_num)
)
;


DROP TABLE IF EXISTS pfx_pickoff;

CREATE TABLE pfx_pickoff (

gid varchar(31)
,game_pk int
,game_date date                                       
,inn_num int
,inn_half varchar(6)
,home_bat_fl int
,ab_num int
,des varchar(150)
,des_es varchar(150)
,event_num int
,catcher varchar(8)
,play_guid varchar(36)

,x_pickoff_id int auto_increment primary key

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP

,unique key (gid,event_num,ab_num)
,index(game_pk)
,index(play_guid)
,index(ab_num)
,index(event_num)
);


DROP TABLE IF EXISTS pfx_player;

CREATE TABLE pfx_player (

gid varchar(31)
,game_date date
,side varchar(10)
,id_ int
,first_ varchar(30)
,last_ varchar(30)
,num int
,boxname varchar(50)
,rl varchar(5)
,bats varchar(5)
,position varchar(5)
,current_position varchar(5)
,status_ varchar(5)
,team_abbrev varchar(5)
,team_id int
,parent_team_abbrev varchar(5)
,parent_team_id int
,bat_order varchar(5)
,game_position varchar(5)
,avg_ float
,hr int
,rbi int
,wins int
,losses int
,era varchar(10)                         

,player_record_id int auto_increment primary key

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP

,unique key (gid,id_)
,index(id_)
,index(team_id)
,index(parent_team_id)
);


DROP TABLE IF EXISTS pfx_miniscore;

CREATE TABLE pfx_miniscore (
gid varchar(31) primary key
,game_date date

,home_name_abbrev varchar(5)
,away_name_abbrev varchar(5)
,home_code varchar(5)
,away_code varchar(5)
,home_file_code varchar(5)
,away_file_code varchar(5)
,home_team_id int
,away_team_id int
,home_games_back varchar(6)
,away_games_back varchar(6)
,home_games_back_wildcard varchar(6)
,away_games_back_wildcard varchar(6)
,venue_w_chan_loc varchar(8)
,gameday_sw varchar(3)
,double_header_sw varchar(3)
,gameday varchar(35)
,home_win int
,home_loss int
,away_win int
,away_loss int
,id_ varchar(30)
,status_ varchar(20)
,status_ind varchar(3)
,delay_reason varchar(50)
,inning int
,top_inning varchar(5)
,b int
,s int
,o int
,inning_state varchar(10)
,note varchar(60)
,is_perfect_game varchar(5)
,is_no_hitter varchar(5)

,away_score int
,home_score int
,diff_score int

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP

,index(gameday)
,index(game_date)
,index(home_team_id)
,index(away_team_id)
,index(id_)
);

DROP TABLE IF EXISTS pfx_hitfx;

CREATE TABLE pfx_hitfx (
game_pk int
,play_guid varchar(36)
,guid_type varchar(30)
,guid_num int
,exit_vel float
,distance float
,launch_ang float

,index(game_pk)
,index(play_guid)
,index(guid_num)
);


DROP TABLE IF EXISTS pfx_basesav;

CREATE TABLE pfx_basesav (

game_pk int
,pitch_id int
,sv_id varchar(13)
,batter int
,stand varchar(3)
,pitcher int
,p_throws varchar(3)
,catcher int
,umpire int
,effective_speed float DEFAULT NULL
,release_spin_rate float DEFAULT NULL
,release_extension float DEFAULT NULL
,hc_x float DEFAULT NULL
,hc_y float DEFAULT NULL
,hit_distance_sc float DEFAULT NULL
,hit_speed float DEFAULT NULL
,hit_angle float DEFAULT NULL

,lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
,createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP


,unique key (game_pk,pitch_id)

,index(sv_id)
,index(batter)
,index(pitcher)
,index(catcher)
,index(umpire)

);



