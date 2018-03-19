use mlb;


drop table if exists betting_scrape;

create table betting_scrape
(
team_away varchar(40),
team_home varchar(40),
game_date date,
game_time_et varchar(40),
book_id int,
spread_away_points float default NULL,
spread_away_odds float default NULL,
spread_home_points float default NULL,
spread_home_odds float default NULL,
money_away_odds float default NULL,
money_home_odds float default NULL,
total_points float default NULL,
total_over float default NULL,
total_under float default NULL,
raw_spread_away_points varchar(10) default NULL,
raw_spread_away_odds varchar(10) default NULL,
raw_spread_home_points varchar(10) default NULL,
raw_spread_home_odds varchar(10) default NULL,
raw_money_away_odds varchar(10) default NULL,
raw_money_home_odds varchar(10) default NULL,
raw_total_points varchar(10) default NULL,
raw_total_over varchar(10) default NULL,
raw_total_under varchar(10) default NULL,
betting_scrape_id int primary key auto_increment,
lastmodifieddate TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
createddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
unique key (team_away,team_home,game_date,game_time_et,book_id),
index(team_home),
index(game_date),
index(game_time_et),
index(book_id)
);


load data local infile 
"/Users/dangoldberg/betting_scrape_backup_20170328" 
into table betting_scrape
columns terminated by ',' enclosed by '"' IGNORE 1 LINES;

UPDATE 
	betting_scrape
SET
	spread_away_points = CASE WHEN spread_away_points = 0 THEN NULL ELSE spread_away_points END,
	spread_away_odds = CASE WHEN spread_away_odds = 0 THEN NULL ELSE spread_away_odds END,
	spread_home_points = CASE WHEN spread_home_points = 0 THEN NULL ELSE spread_home_points END,
	spread_home_odds = CASE WHEN spread_home_odds = 0 THEN NULL ELSE spread_home_odds END,
	money_away_odds = CASE WHEN money_away_odds = 0 THEN NULL ELSE money_away_odds END,
	money_home_odds = CASE WHEN money_home_odds = 0 THEN NULL ELSE money_home_odds END,
	total_points = CASE WHEN total_points = 0 THEN NULL ELSE total_points END,
	total_over = CASE WHEN total_over = 0 THEN NULL ELSE total_over END,
	total_under = CASE WHEN total_under = 0 THEN NULL ELSE total_under END
    ;

select createddate, count(1) from betting_scrape where date(createddate) = date(now()) group by 1;

select * from betting_scrape
