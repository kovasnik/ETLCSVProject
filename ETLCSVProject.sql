--CREATE DATABASE ETLSQLProject;

USE ETLSQLProject;
--DROP TABLE CabInfo;

--Did not add id column here because SqlBulkCopy tried to put the time column in id
--CREATE TABLE CabInfo(
--	tpep_pickup_datetime DATETIME NOT NULL,
--	tpep_dropoff_datetime DATETIME NOT NULL,
--	passenger_count INT,
--	trip_distance FLOAT,
--	store_and_fwd_flag VARCHAR(3),
--	PULocationID INT,
--	DOLocationID INT,
--	fare_amount DECIMAL(10, 2),
--	tip_amount DECIMAL(10, 2),
--);

--SELECT PULocationID, AVG(tip_amount) AS average_tip_amount 
--FROM CabInfo 
--GROUP BY PULocationID 
--ORDER BY average_tip_amount DESC;

--SELECT TOP 100 * FROM CabInfo ORDER BY trip_distance DESC;

--SELECT TOP 100 *, DATEDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) AS diff_time 
--FROM CabInfo 
--ORDER BY diff_time DESC;

--Did not quite understand what execlty needs tobe shown here
--SELECT * FROM CabInfo WHERE PULocationID > 240;