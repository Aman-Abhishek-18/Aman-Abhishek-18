--Steps to Achieve This:
--
--Create the Master Table
--Create Staging Tables for 5 branches
--Create a Stored Procedure to Merge Data
--Create a Scheduler Job to Run the Merge Process

-- Create the master table to store the merged data
CREATE TABLE starbucks_master (
    store_id       NUMBER PRIMARY KEY,
    store_name     VARCHAR2(100),
    store_address  VARCHAR2(200),
    store_zipcode  NUMBER,
    coffee_type    VARCHAR2(50),
    price          NUMBER,
    last_updated   TIMESTAMP
);


-- Create staging table for branch 1
CREATE TABLE starbucks_stage_branch_1 (
    store_id       NUMBER,
    store_name     VARCHAR2(100),
    store_address  VARCHAR2(200),
    store_zipcode  NUMBER,
    coffee_type    VARCHAR2(50),
    price          NUMBER,
    upload_dt      TIMESTAMP
);

-- Create staging table for branch 2
CREATE TABLE starbucks_stage_branch_2 (
    store_id       NUMBER,
    store_name     VARCHAR2(100),
    store_address  VARCHAR2(200),
    store_zipcode  NUMBER,
    coffee_type    VARCHAR2(50),
    price          NUMBER,
    upload_dt      TIMESTAMP
);

-- Create staging table for branch 3
CREATE TABLE starbucks_stage_branch_3 (
    store_id       NUMBER,
    store_name     VARCHAR2(100),
    store_address  VARCHAR2(200),
    store_zipcode  NUMBER,
    coffee_type    VARCHAR2(50),
    price          NUMBER,
    upload_dt      TIMESTAMP
);

-- Create staging table for branch 4
CREATE TABLE starbucks_stage_branch_4 (
    store_id       NUMBER,
    store_name     VARCHAR2(100),
    store_address  VARCHAR2(200),
    store_zipcode  NUMBER,
    coffee_type    VARCHAR2(50),
    price          NUMBER,
    upload_dt      TIMESTAMP
);

-- Create staging table for branch 5
CREATE TABLE starbucks_stage_branch_5 (
    store_id       NUMBER,
    store_name     VARCHAR2(100),
    store_address  VARCHAR2(200),
    store_zipcode  NUMBER,
    coffee_type    VARCHAR2(50),
    price          NUMBER,
    upload_dt      TIMESTAMP
);



CREATE OR REPLACE PROCEDURE merge_starbucks_data AS
BEGIN
    -- Merge data from all branches into the master table using UNION ALL
    MERGE INTO starbucks_master tgt
    USING (
        SELECT store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt
        FROM starbucks_stage_branch_1
        UNION ALL
        SELECT store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt
        FROM starbucks_stage_branch_2
        UNION ALL
        SELECT store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt
        FROM starbucks_stage_branch_3
        UNION ALL
        SELECT store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt
        FROM starbucks_stage_branch_4
        UNION ALL
        SELECT store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt
        FROM starbucks_stage_branch_5
    ) src
    ON (tgt.store_id = src.store_id)  -- Matching by store_id
    WHEN MATCHED THEN
        UPDATE SET 
            tgt.store_name = src.store_name,
            tgt.store_address = src.store_address,
            tgt.store_zipcode = src.store_zipcode,
            tgt.coffee_type = src.coffee_type,
            tgt.price = src.price,
            tgt.last_updated = src.upload_dt
    WHEN NOT MATCHED THEN
        INSERT (store_id, store_name, store_address, store_zipcode, coffee_type, price, last_updated)
        VALUES (src.store_id, src.store_name, src.store_address, src.store_zipcode, src.coffee_type, src.price, src.upload_dt);
    
    COMMIT;
END merge_starbucks_data;



--create job to automatically run job at specified time
BEGIN
    DBMS_SCHEDULER.create_job (
        job_name        => 'merge_starbucks_data_job',
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'merge_starbucks_data',  -- Call the stored procedure
        start_date      => TO_TIMESTAMP('2025-04-25 23:00:00', 'YYYY-MM-DD HH24:MI:SS'),
        repeat_interval => 'FREQ=DAILY; BYHOUR=18; BYMINUTE=25; BYSECOND=0',  -- Runs every day at 6:25 PM
        enabled         => TRUE
    );
END;
/


--drop above job to change start_date as systimestamp
BEGIN
    DBMS_SCHEDULER.drop_job('merge_starbucks_data_job');
END;
/
--you may also alter it, using attribute option


BEGIN
    DBMS_SCHEDULER.create_job (
        job_name        => 'MERGE_STARBUCKS_JOB',
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'MERGE_STARBUCKS_DATA',  -- your stored procedure name
        start_date      => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=18; BYMINUTE=26',
        enabled         => TRUE
    );
END;
/

--check job status
SELECT job_name, state
FROM all_scheduler_jobs
WHERE job_name = 'MERGE_STARBUCKS_JOB';



--now, insert few data into branches so that it gets merged into master at 6.25pm automatically

-- Insert sample data into Branch 1
INSERT INTO starbucks_stage_branch_1 (store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt)
VALUES (1, 'Starbucks Downtown', '123 Main St, Downtown', 12345, 'Cappuccino', 4.50, SYSDATE);

-- Insert sample data into Branch 2
INSERT INTO starbucks_stage_branch_2 (store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt)
VALUES (2, 'Starbucks Uptown', '456 Uptown Ave', 23456, 'Latte', 4.00, SYSDATE);

-- Insert sample data into Branch 3
INSERT INTO starbucks_stage_branch_3 (store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt)
VALUES (3, 'Starbucks Suburbia', '789 Suburbia Rd', 34567, 'Espresso', 3.00, SYSDATE);

-- Insert sample data into Branch 4
INSERT INTO starbucks_stage_branch_4 (store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt)
VALUES (4, 'Starbucks Mall', '101 Mall St', 45678, 'Americano', 3.50, SYSDATE);

-- Insert sample data into Branch 5
INSERT INTO starbucks_stage_branch_5 (store_id, store_name, store_address, store_zipcode, coffee_type, price, upload_dt)
VALUES (5, 'Starbucks Airport', '202 Airport Blvd', 56789, 'Mocha', 4.75, SYSDATE);


SELECT * FROM starbucks_stage_branch_1;
SELECT * FROM starbucks_stage_branch_2;
SELECT * FROM starbucks_stage_branch_3;
SELECT * FROM starbucks_stage_branch_4;
SELECT * FROM starbucks_stage_branch_5;


--run this after or at 6.25pm to see the data 
SELECT * FROM starbucks_master;


--DISABLE job
BEGIN
    DBMS_SCHEDULER.disable('MERGE_STARBUCKS_JOB');
END;
/
