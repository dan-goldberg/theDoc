use mlb;

select * 
from 
pfx_atbats as t1
where
ab_number = (select max(ab_number) from pfx_atbats as t2 where t2.game_id = t1.game_id);

select
game_id,
date(concat(t1.year,'-',t1.month,'-',t1.day)) as game_date,
local_game_time,
away_team_id,
home_team_id,
sum(case
	when bat_home_id = 0 and ab_des LIKE '%scores%scores%scores%scores%' 
		or ab_des LIKE '%homers%scores%scores%scores%'
        or ab_des LIKE '%grand slam%scores%scores%scores%' 
        or ab_des LIKE '%hits an inside-the-park home run%scores%scores%scores%' then 4
    when bat_home_id = 0 and ab_des LIKE '%scores%scores%scores%' 
		or ab_des LIKE '%homers%scores%scores%' 
        or ab_des LIKE '%hits an inside-the-park home run%scores%scores%' then 3
    when bat_home_id = 0 and ab_des LIKE '%scores%scores%' 
		or ab_des LIKE '%homers%scores%' 
        or ab_des LIKE '%hits an inside-the-park home run%scores%' then 2
    when bat_home_id = 0 and ab_des LIKE '%scores%' 
		or ab_des LIKE '%homers%' 
        or ab_des LIKE '%hits an inside-the-park home run%' then 1
    else 0
    end) as away_runs,
sum(case 
	when bat_home_id = 1 and ab_des LIKE '%scores%scores%scores%scores%' 
		or ab_des LIKE '%homers%scores%scores%scores%'
        or ab_des LIKE '%grand slam%scores%scores%scores%' 
        or ab_des LIKE '%hits an inside-the-park home run%scores%scores%scores%' then 4
    when bat_home_id = 1 and ab_des LIKE '%scores%scores%scores%' 
		or ab_des LIKE '%homers%scores%scores%' 
        or ab_des LIKE '%hits an inside-the-park home run%scores%scores%' then 3
    when bat_home_id = 1 and ab_des LIKE '%scores%scores%' 
		or ab_des LIKE '%homers%scores%' 
        or ab_des LIKE '%hits an inside-the-park home run%scores%' then 2
    when bat_home_id = 1 and ab_des LIKE '%scores%' 
		or ab_des LIKE '%homers%' 
        or ab_des LIKE '%hits an inside-the-park home run%' then 1
    else 0
    end) as home_runs
from pfx_atbats as t1
group by game_id,
away_team_id,
home_team_id,
game_date
;


select * from pfx_pitches where pitch_des LIKE '%ball in dirt%' and pa_terminal_fl = 'F' limit 1000;

select * 
from 
pfx_pitches as t1
where
pitch_id = (select max(pitch_id) from pfx_pitches as t2 where t2.game_id = t1.game_id)


select * from pfx_atbats where ab_des LIKE '%inside-the-park%'