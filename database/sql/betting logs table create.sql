use mlb;

DROP TABLE IF EXISTS theDoc_logs_predsspread;
CREATE table theDoc_logs_predsspread (
game_date DATE,
gid varchar(40),
points1 float,
points0 float,
payoutodds1 float,
payoutodds0 float,
probabilities1 float,
probabilities0 float,
prediction int,
area1 float,
area0 float,
bet int,
createddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
lastmodifieddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS theDoc_logs_predstotal;
CREATE table theDoc_logs_predstotal (
game_date DATE,
gid varchar(40),
points float,
payoutodds1 float,
payoutodds0 float,
probabilities1 float,
probabilities0 float,
prediction int,
area1 float,
area0 float,
bet int,
createddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
lastmodifieddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS theDoc_logs_predsmoney;
CREATE table theDoc_logs_predsmoney (
game_date DATE,
gid varchar(40),
payoutodds1 float,
payoutodds0 float,
probabilities1 float,
probabilities0 float,
prediction int,
area1 float,
area0 float,
bet int,
createddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
lastmodifieddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS theDoc_logs_bets;
CREATE table theDoc_logs_bets (
game_date DATE,
gid varchar(40),
bettype varchar(20),
side int,
payoutodds float,
amount float,
createddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
lastmodifieddate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

