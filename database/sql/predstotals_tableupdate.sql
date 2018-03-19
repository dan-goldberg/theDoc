ALTER TABLE theDoc_logs_predstotal ADD COLUMN 
partit float;
UPDATE theDoc_logs_predstotal SET
partit = probabilities0+probabilities1
;

update theDoc_logs_predstotal SET 
probabilities1 = CASE
	WHEN points % 1 <> 0 THEN probabilities1
    ELSE probabilities1/partit
END,
probabilities0 = CASE
	WHEN points % 1 <> 0 THEN probabilities0
    ELSE probabilities0/partit
END;

SELECT * FROM theDoc_logs_predstotal;

ALTER TABLE theDoc_logs_predstotal DROP COLUMN partit;

UPDATE theDoc_logs_predstotal SET 
prediction = CASE
	WHEN probabilities1 > probabilities0 THEN 1 ELSE 0
END,
area1 = probabilities1*payoutodds1,
area0 = probabilities0*payoutodds0
;


SELECT * FROM theDoc_logs_predstotal;