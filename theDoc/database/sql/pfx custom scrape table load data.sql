/*
load data local infile 
"/Users/dangoldberg/pfx_game_table___2008.csv" 
into table pfx_game
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

load data local infile 
"/Users/dangoldberg/pfx_probables_table___2008.csv" 
into table pfx_prob
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

load data local infile 
"/Users/dangoldberg/pfx_action_table___2008.csv" 
into table pfx_action
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

load data local infile 
"/Users/dangoldberg/pfx_atbat_table___2008.csv" 
into table pfx_atbat
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

load data local infile 
"/Users/dangoldberg/pfx_pitch_table___2008.csv" 
into table pfx_pitch
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

load data local infile 
"/Users/dangoldberg/pfx_runner_table___2008.csv" 
into table pfx_runner
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

load data local infile 
"/Users/dangoldberg/pfx_pickoff_table___2008.csv" 
into table pfx_pickoff
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

load data local infile 
"/Users/dangoldberg/pfx_miniscore_table___2015.csv" 
into table pfx_miniscore
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;



load data local infile 
"/Users/dangoldberg/pfx_players_table___2008.csv" 
into table pfx_player
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

load data local infile 
"/Users/dangoldberg/pfx_hfx_table___2015.csv" 
into table pfx_hitfx
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;


select createddate, count(1) from pfx_game where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_prob where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_action where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_atbat where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_pitch where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_runner where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_pickoff where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_miniscore where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_player where date(createddate) = date(now()) group by 1;
select createddate, count(1) from pfx_hitfx where date(createddate) = date(now()) group by 1;
*/