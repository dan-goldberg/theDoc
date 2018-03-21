DROP TABLE theDoc_investments;

create table theDoc_investments 
(
id int auto_increment primary key,
firstname varchar(40),
lastname varchar(40),
deposit_date date,
deposit_amount float,
bank_before_deposit float,
bank_after_deposit float,
createddate timestamp DEFAULT current_timestamp,
lastmodified timestamp DEFAULT current_timestamp ON UPDATE current_timestamp
)
;


INSERT INTO theDoc_investments
VALUE
(
NULL,
'Dan',
'Goldberg',
'2017-01-01',
1000.00,
0,
1000.00,
NULL,
NULL
);


SELECT * FROM theDoc_investments;