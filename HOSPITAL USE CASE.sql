create database hackathon_db;


CREATE SCHEMA RAW;
CREATE SCHEMA VALIDATED;
CREATE SCHEMA CURATED;
CREATE SCHEMA ANALYTICS;
CREATE SCHEMA CONTROL;
CREATE SCHEMA UTIL;
CREATE SCHEMA TASKS;

-------------------------------------------------------


CREATE SCHEMA IF NOT EXISTS CONTROL;

CREATE OR REPLACE TABLE CONTROL.ERROR_LOG (
    table_name STRING,
    error_type STRING,
    record_data VARIANT,
    error_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

USE SCHEMA CONTROL;
CREATE OR REPLACE TABLE CONTROL.ANOMALY_LOG (
    anomaly_type   STRING,        -- e.g., 'HIGH_BILLING', 'SCHEDULE_CLASH'
    record_id      STRING,        -- ID of the patient, doctor, or bill causing the issue
    description    STRING,        -- Human-readable detail about the anomaly
    detected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP() -- When it was caught
);
---------------------------------------------------------------




CREATE OR REPLACE FILE FORMAT UTIL.CSV_FORMAT
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL','null','');

CREATE OR REPLACE STORAGE INTEGRATION S3_INT
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::110695445731:role/hacakthon'
STORAGE_ALLOWED_LOCATIONS = ('s3://hackathonb12003/pipes/');

desc STORAGE INTEGRATION S3_INT;
CREATE OR REPLACE STAGE UTIL.HEALTH_STAGE
URL='s3://hackathonb12003/pipes/'
STORAGE_INTEGRATION = S3_INT
FILE_FORMAT = UTIL.CSV_FORMAT;

list @UTIL.HEALTH_STAGE;


USE SCHEMA RAW;

CREATE TABLE PATIENTS_INC (
    patient_id STRING,
    name STRING,
    gender STRING,
    age STRING,
    city STRING,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE DOCTORS_INC (
    doctor_id STRING,
    doctor_name STRING,
    specialization STRING,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE APPOINTMENTS_INC (
    appointment_id STRING,
    patient_id STRING,
    doctor_id STRING,
    appointment_date STRING,
    status STRING,
    visit_type STRING,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE BILLING_INC (
    billing_id STRING,
    patient_id STRING,
    billing_date STRING,
    amount STRING,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE MEDICAL_INC (
    record_id STRING,
    patient_id STRING,
    disease STRING,
    severity STRING,
    chronic_flag STRING,
    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PIPE patients_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RAW.PATIENTS_INC (
    patient_id,
    name,
    gender,
    age,
    city
)
FROM @UTIL.HEALTH_STAGE/patients/
FILE_FORMAT = (FORMAT_NAME = UTIL.CSV_FORMAT)
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE doctors_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RAW.DOCTORS_INC (
    doctor_id,
    doctor_name,
    specialization
)
FROM @UTIL.HEALTH_STAGE/doctors/
FILE_FORMAT = (FORMAT_NAME = UTIL.CSV_FORMAT)
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE appointments_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RAW.APPOINTMENTS_INC (
    appointment_id,
    patient_id,
    doctor_id,
    appointment_date,
    status,
    visit_type
)
FROM @UTIL.HEALTH_STAGE/appointments/
FILE_FORMAT = (FORMAT_NAME = UTIL.CSV_FORMAT)
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE PIPE billing_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RAW.BILLING_INC (
    billing_id,
    patient_id,
    billing_date,
    amount
)
FROM @UTIL.HEALTH_STAGE/billing/
FILE_FORMAT = (FORMAT_NAME = UTIL.CSV_FORMAT)
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE medical_pipe
AUTO_INGEST = TRUE
AS
COPY INTO RAW.MEDICAL_INC (
    record_id,
    patient_id,
    disease,
    severity,
    chronic_flag
)
FROM @UTIL.HEALTH_STAGE/medical/
FILE_FORMAT = (FORMAT_NAME = UTIL.CSV_FORMAT)
ON_ERROR = 'CONTINUE';


show pipes;

select system$pipe_status('HACKATHON_DB.RAW.APPOINTMENTS_PIPE');
select system$pipe_status('HACKATHON_DB.RAW.BILLING_PIPE');
select system$pipe_status('HACKATHON_DB.RAW.DOCTORS_PIPE');
select system$pipe_status('HACKATHON_DB.RAW.MEDICAL_PIPE');
select system$pipe_status('HACKATHON_DB.RAW.PATIENTS_PIPE');



CREATE OR REPLACE STREAM RAW.PATIENTS_STREAM ON TABLE RAW.PATIENTS_INC;
CREATE OR REPLACE STREAM RAW.DOCTORS_STREAM ON TABLE RAW.DOCTORS_INC;
CREATE OR REPLACE STREAM RAW.APPOINTMENTS_STREAM ON TABLE RAW.APPOINTMENTS_INC;
CREATE OR REPLACE STREAM RAW.BILLING_STREAM ON TABLE RAW.BILLING_INC;
CREATE OR REPLACE STREAM RAW.MEDICAL_STREAM ON TABLE RAW.MEDICAL_INC;

select * from  RAW.BILLING_INC;
select * from RAW.PATIENTS_INC;
select * from RAW.DOCTORS_INC;
select * from RAW.APPOINTMENTS_INC;
select * from RAW.MEDICAL_INC;


select * from RAW.MEDICAL_STREAM;
select * from RAW.PATIENTS_STREAM;
select * from RAW.BILLING_STREAM;
select * from RAW.DOCTORS_STREAM;
select * from RAW.APPOINTMENTS_STREAM;




-- ======================================================================================
-- 2. VALIDATED LAYER: TABLES & STRICT CLEANING SP
-- ======================================================================================
USE SCHEMA VALIDATED;

 
-- ======================================================================================
-- 4. VALIDATED LAYER: TABLES & STRICT CLEANING PROCEDURE
-- ======================================================================================
USE SCHEMA VALIDATED;
CREATE OR REPLACE TABLE PATIENTS (
    patient_id STRING, name STRING, gender STRING, age NUMBER, city STRING, load_ts TIMESTAMP
);
CREATE OR REPLACE TABLE DOCTORS (
    doctor_id STRING, doctor_name STRING, specialization STRING, load_ts TIMESTAMP
);
CREATE OR REPLACE TABLE APPOINTMENTS (
    appointment_id STRING, patient_id STRING, doctor_id STRING, 
    appointment_date DATE, status STRING, visit_type STRING, load_ts TIMESTAMP
);
CREATE OR REPLACE TABLE BILLING (
    billing_id STRING, patient_id STRING, billing_date DATE, amount FLOAT, load_ts TIMESTAMP
);
CREATE OR REPLACE TABLE MEDICAL_RECORDS (
    record_id STRING, patient_id STRING, disease STRING, severity STRING, chronic_flag STRING, load_ts TIMESTAMP
);
-- CLEANING STORED PROCEDURE

CREATE OR REPLACE PROCEDURE VALIDATED.SP_CLEAN_WAREHOUSE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

--------------------------------------------------
-- TEMP TABLES (prevent stream loss)
--------------------------------------------------
CREATE OR REPLACE TEMP TABLE tmp_patients AS SELECT * FROM RAW.PATIENTS_STREAM;
CREATE OR REPLACE TEMP TABLE tmp_doctors AS SELECT * FROM RAW.DOCTORS_STREAM;
CREATE OR REPLACE TEMP TABLE tmp_appointments AS SELECT * FROM RAW.APPOINTMENTS_STREAM;
CREATE OR REPLACE TEMP TABLE tmp_billing AS SELECT * FROM RAW.BILLING_STREAM;
CREATE OR REPLACE TEMP TABLE tmp_medical AS SELECT * FROM RAW.MEDICAL_STREAM;

--------------------------------------------------
-- PATIENTS
--------------------------------------------------
INSERT INTO CONTROL.ERROR_LOG
SELECT 'PATIENTS','INVALID_DATA',
OBJECT_CONSTRUCT('patient_id',patient_id,'age',age),
CURRENT_TIMESTAMP
FROM tmp_patients
WHERE patient_id IS NULL OR TRY_TO_NUMBER(age) IS NULL;

MERGE INTO VALIDATED.PATIENTS tgt
USING (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY patient_id ORDER BY load_ts DESC) rn
    FROM tmp_patients
    WHERE patient_id IS NOT NULL
) src
ON tgt.patient_id = src.patient_id

WHEN MATCHED THEN UPDATE SET
    tgt.name = COALESCE(src.name,'UNKNOWN'),
    tgt.age = COALESCE(TRY_TO_NUMBER(src.age),0),
    tgt.city = COALESCE(src.city,'UNKNOWN'),
    tgt.load_ts = src.load_ts

WHEN NOT MATCHED THEN INSERT VALUES (
    src.patient_id,
    COALESCE(src.name,'UNKNOWN'),
    src.gender,
    COALESCE(TRY_TO_NUMBER(src.age),0),
    COALESCE(src.city,'UNKNOWN'),
    src.load_ts
);

--------------------------------------------------
-- DOCTORS
--------------------------------------------------
MERGE INTO VALIDATED.DOCTORS tgt
USING (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY doctor_id ORDER BY load_ts DESC) rn
    FROM tmp_doctors
    WHERE doctor_id IS NOT NULL
) src
ON tgt.doctor_id = src.doctor_id

WHEN NOT MATCHED THEN INSERT VALUES (
    src.doctor_id,
    COALESCE(src.doctor_name,'UNKNOWN'),
    COALESCE(src.specialization,'GENERAL'),
    src.load_ts
);

--------------------------------------------------
-- APPOINTMENTS (FIXED DATE LOGIC)
--------------------------------------------------
INSERT INTO CONTROL.ERROR_LOG
SELECT 'APPOINTMENTS','INVALID_DATE',
OBJECT_CONSTRUCT('appointment_id',appointment_id,'date',appointment_date),
CURRENT_TIMESTAMP
FROM tmp_appointments
WHERE TRY_TO_DATE(appointment_date) IS NULL;

MERGE INTO VALIDATED.APPOINTMENTS tgt
USING (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY appointment_id ORDER BY load_ts DESC) rn
    FROM tmp_appointments
    WHERE appointment_id IS NOT NULL
) src
ON tgt.appointment_id = src.appointment_id

WHEN NOT MATCHED THEN INSERT VALUES (
    src.appointment_id,
    src.patient_id,
    src.doctor_id,
    COALESCE(TRY_TO_DATE(src.appointment_date), CURRENT_DATE),
    COALESCE(src.status,'UNKNOWN'),
    COALESCE(src.visit_type,'OUTPATIENT'),
    src.load_ts
);

--------------------------------------------------
-- BILLING (FIXED)
--------------------------------------------------
INSERT INTO CONTROL.ERROR_LOG
SELECT 'BILLING','INVALID_AMOUNT',
OBJECT_CONSTRUCT('billing_id',billing_id,'amount',amount),
CURRENT_TIMESTAMP
FROM tmp_billing
WHERE TRY_TO_NUMBER(amount) IS NULL;

MERGE INTO VALIDATED.BILLING tgt
USING (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY billing_id ORDER BY load_ts DESC) rn
    FROM tmp_billing
    WHERE billing_id IS NOT NULL
) src
ON tgt.billing_id = src.billing_id

WHEN NOT MATCHED THEN INSERT VALUES (
    src.billing_id,
    src.patient_id,
    COALESCE(TRY_TO_DATE(src.billing_date), CURRENT_DATE),
    COALESCE(TRY_TO_NUMBER(src.amount),0),
    src.load_ts
);

--------------------------------------------------
-- MEDICAL (FIXED)
--------------------------------------------------
MERGE INTO VALIDATED.MEDICAL_RECORDS tgt
USING (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY record_id ORDER BY load_ts DESC) rn
    FROM tmp_medical
    WHERE patient_id IS NOT NULL
) src
ON tgt.record_id = src.record_id

WHEN NOT MATCHED THEN INSERT VALUES (
    src.record_id,
    src.patient_id,
    COALESCE(src.disease,'UNKNOWN'),
    COALESCE(src.severity,'LOW'),
    COALESCE(src.chronic_flag,'NO'),
    src.load_ts
);

RETURN 'CLEANING SUCCESSFUL';

END;
$$;

call VALIDATED.SP_CLEAN_WAREHOUSE();

-- ======================================================================================
-- 5. CURATED LAYER (SCD 2 & FACT_HEALTHCARE)
-- ======================================================================================
USE SCHEMA CURATED;
CREATE OR REPLACE TABLE DIM_PATIENT (patient_sk INT AUTOINCREMENT PRIMARY KEY, patient_id STRING, name STRING, gender STRING, age INT, city STRING, start_date DATE, end_date DATE, is_current BOOLEAN);
CREATE OR REPLACE TABLE DIM_DOCTOR (doctor_sk INT AUTOINCREMENT PRIMARY KEY, doctor_id STRING, doctor_name STRING, specialization STRING, start_date DATE, end_date DATE, is_current BOOLEAN);
CREATE OR REPLACE TABLE DIM_DIAGNOSIS (diagnosis_sk INT AUTOINCREMENT PRIMARY KEY, disease_name STRING, chronic_flag STRING);
-- FACT TABLE AS PER IMAGE
CREATE OR REPLACE TABLE FACT_HEALTHCARE (
    fact_sk INT AUTOINCREMENT PRIMARY KEY,
    patient_sk INT, doctor_sk INT, date_sk INT, diagnosis_sk INT,
    appointment_count INT, no_show_count INT, billing_amount NUMBER(12,2)
);
CREATE OR REPLACE TABLE DIM_DATE AS SELECT REPLACE(d, '-', '')::INT as date_sk, d as full_date, DAY(d) as day, MONTH(d) as month, YEAR(d) as year, DAYNAME(d) as weekday FROM (SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '2024-01-01'::DATE) AS d FROM TABLE(GENERATOR(ROWCOUNT => 1000)));
CREATE OR REPLACE PROCEDURE CURATED.SP_LOAD_CURATED()
RETURNS STRING LANGUAGE SQL AS
$$
BEGIN
    MERGE INTO CURATED.DIM_PATIENT tgt USING VALIDATED.PATIENTS src ON tgt.patient_id = src.patient_id AND tgt.is_current = TRUE
    WHEN MATCHED AND (tgt.city <> src.city OR tgt.age <> src.age) THEN UPDATE SET tgt.is_current = FALSE, tgt.end_date = CURRENT_DATE()
    WHEN NOT MATCHED THEN INSERT (patient_id, name, gender, age, city, start_date, is_current) VALUES (src.patient_id, src.name, src.gender, src.age, src.city, CURRENT_DATE(), TRUE);

    INSERT INTO CURATED.DIM_PATIENT (patient_id, name, gender, age, city, start_date, is_current)
    SELECT src.patient_id, src.name, src.gender, src.age, src.city, CURRENT_DATE(), TRUE
    FROM VALIDATED.PATIENTS src
    JOIN CURATED.DIM_PATIENT tgt ON tgt.patient_id = src.patient_id AND tgt.is_current = FALSE
    WHERE NOT EXISTS (SELECT 1 FROM CURATED.DIM_PATIENT x WHERE x.patient_id = src.patient_id AND x.is_current = TRUE)
      AND (tgt.city <> src.city OR tgt.age <> src.age);

    MERGE INTO CURATED.DIM_DOCTOR tgt USING VALIDATED.DOCTORS src ON tgt.doctor_id = src.doctor_id AND tgt.is_current = TRUE
    WHEN MATCHED AND tgt.specialization <> src.specialization THEN UPDATE SET tgt.is_current = FALSE, tgt.end_date = CURRENT_DATE()
    WHEN NOT MATCHED THEN INSERT (doctor_id, doctor_name, specialization, start_date, is_current) VALUES (src.doctor_id, src.doctor_name, src.specialization, CURRENT_DATE(), TRUE);

    INSERT INTO CURATED.DIM_DOCTOR (doctor_id, doctor_name, specialization, start_date, is_current)
    SELECT src.doctor_id, src.doctor_name, src.specialization, CURRENT_DATE(), TRUE
    FROM VALIDATED.DOCTORS src
    JOIN CURATED.DIM_DOCTOR tgt ON tgt.doctor_id = src.doctor_id AND tgt.is_current = FALSE
    WHERE NOT EXISTS (SELECT 1 FROM CURATED.DIM_DOCTOR x WHERE x.doctor_id = src.doctor_id AND x.is_current = TRUE)
      AND tgt.specialization <> src.specialization;

    MERGE INTO CURATED.DIM_DIAGNOSIS tgt USING (SELECT DISTINCT disease, chronic_flag FROM VALIDATED.MEDICAL_RECORDS) src ON tgt.disease_name = src.disease
    WHEN NOT MATCHED THEN INSERT (disease_name, chronic_flag) VALUES (src.disease, src.chronic_flag);

    TRUNCATE TABLE CURATED.FACT_HEALTHCARE;

    INSERT INTO CURATED.FACT_HEALTHCARE (patient_sk, doctor_sk, date_sk, diagnosis_sk, appointment_count, no_show_count, billing_amount)
    SELECT
        dp.patient_sk,
        dd.doctor_sk,
        d.date_sk,
        di.diagnosis_sk,
        COUNT(DISTINCT a.appointment_id),
        SUM(CASE WHEN a.status = 'NO_SHOW' THEN 1 ELSE 0 END),
        COALESCE(b.total_amount, 0)
    FROM VALIDATED.APPOINTMENTS a
    JOIN CURATED.DIM_PATIENT dp ON a.patient_id = dp.patient_id AND dp.is_current = TRUE
    JOIN CURATED.DIM_DOCTOR dd ON a.doctor_id = dd.doctor_id AND dd.is_current = TRUE
    JOIN CURATED.DIM_DATE d ON a.appointment_date = d.full_date
    LEFT JOIN (SELECT DISTINCT patient_id, disease FROM VALIDATED.MEDICAL_RECORDS) m ON a.patient_id = m.patient_id
    LEFT JOIN CURATED.DIM_DIAGNOSIS di ON m.disease = di.disease_name
    LEFT JOIN (SELECT patient_id, billing_date, SUM(amount) AS total_amount FROM VALIDATED.BILLING GROUP BY patient_id, billing_date) b
        ON a.patient_id = b.patient_id AND a.appointment_date = b.billing_date
    GROUP BY dp.patient_sk, dd.doctor_sk, d.date_sk, di.diagnosis_sk, b.total_amount;

    RETURN 'SUCCESS';
END;
$$;

call CURATED.SP_LOAD_CURATED();

-- 3 ANOMALIES
CREATE OR REPLACE PROCEDURE CONTROL.SP_RUN_ANOMALIES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

DELETE FROM CONTROL.ANOMALY_LOG;

-- HIGH BILLING
INSERT INTO CONTROL.ANOMALY_LOG
SELECT 'HIGH_BILLING', billing_id, 'Amount > 10000', CURRENT_TIMESTAMP
FROM VALIDATED.BILLING
WHERE amount > 10000;

-- DOCTOR CLASH
INSERT INTO CONTROL.ANOMALY_LOG
SELECT 'DOCTOR_OVERLOAD', doctor_id, 'More than 5 appointments/day', CURRENT_TIMESTAMP
FROM (
    SELECT doctor_id, appointment_date, COUNT(*) c
    FROM VALIDATED.APPOINTMENTS
    GROUP BY doctor_id, appointment_date
)
WHERE c > 5;

-- HIGH RISK PATIENT
INSERT INTO CONTROL.ANOMALY_LOG
SELECT 'HIGH_RISK', patient_id, 'Chronic + high visits', CURRENT_TIMESTAMP
FROM (
    SELECT patient_id, COUNT(*) c
    FROM VALIDATED.APPOINTMENTS
    GROUP BY patient_id
)
WHERE c > 10;

RETURN 'ANOMALY DONE';

END;
$$;

call CONTROL.SP_RUN_ANOMALIES();


-- 4 KPIs
CREATE OR REPLACE VIEW ANALYTICS.KPI_PATIENT_360 AS SELECT p.name, f.appointment_count, f.billing_amount FROM CURATED.FACT_HEALTHCARE f JOIN CURATED.DIM_PATIENT p ON f.patient_sk = p.patient_sk;

CREATE OR REPLACE VIEW ANALYTICS.KPI_RISK_INDEX AS SELECT p.name, CASE WHEN f.billing_amount > 5000 THEN 'HIGH' ELSE 'LOW' END as risk FROM CURATED.FACT_HEALTHCARE f JOIN CURATED.DIM_PATIENT p ON f.patient_sk = p.patient_sk;
CREATE OR REPLACE VIEW ANALYTICS.KPI_REVENUE AS SELECT d.month, SUM(f.billing_amount) as rev FROM CURATED.FACT_HEALTHCARE f JOIN CURATED.DIM_DATE d ON f.date_sk = d.date_sk GROUP BY 1;
CREATE OR REPLACE VIEW ANALYTICS.KPI_NOSHOWS AS SELECT dd.doctor_name, SUM(f.no_show_count) as missed FROM CURATED.FACT_HEALTHCARE f JOIN CURATED.DIM_DOCTOR dd ON f.doctor_sk = dd.doctor_sk GROUP BY 1;









-- TASKS
CREATE OR REPLACE TASK TASKS.TSK_START WAREHOUSE='COMPUTE_WH' SCHEDULE='1 MINUTE' WHEN SYSTEM$STREAM_HAS_DATA('RAW.PATIENTS_STREAM') AS CALL VALIDATED.SP_CLEAN_WAREHOUSE();
CREATE OR REPLACE TASK TASKS.TSK_CURATED WAREHOUSE='COMPUTE_WH' AFTER TASKS.TSK_START AS CALL CURATED.SP_LOAD_CURATED();
CREATE OR REPLACE TASK TASKS.TSK_ANOMALY WAREHOUSE='COMPUTE_WH' AFTER TASKS.TSK_CURATED AS CALL CONTROL.SP_RUN_ANOMALIES();
ALTER TASK TASKS.TSK_ANOMALY RESUME;
ALTER TASK TASKS.TSK_CURATED RESUME;
ALTER TASK TASKS.TSK_START RESUME;
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY QUERY_START_TIME DESC;





CREATE OR REPLACE VIEW ANALYTICS.KPI_PATIENT_360 AS
SELECT
    p.patient_id,
    p.name,

    SUM(f.appointment_count) AS total_visits,
    SUM(f.billing_amount) AS total_revenue,
    SUM(f.no_show_count) AS total_no_shows,

    ROUND(
        SUM(f.no_show_count) * 100.0 / NULLIF(SUM(f.appointment_count),0),
        2
    ) AS no_show_rate

FROM CURATED.FACT_HEALTHCARE f
JOIN CURATED.DIM_PATIENT p 
ON f.patient_sk = p.patient_sk

GROUP BY p.patient_id, p.name;




CREATE OR REPLACE VIEW ANALYTICS.KPI_RISK_INDEX AS
SELECT
    p.patient_id,
    p.name,

    CASE 
        WHEN SUM(f.appointment_count) > 8
             OR SUM(f.billing_amount) > 10000
        THEN 'HIGH'

        WHEN SUM(f.appointment_count) BETWEEN 4 AND 8
        THEN 'MEDIUM'

        ELSE 'LOW'
    END AS risk_level

FROM CURATED.FACT_HEALTHCARE f
JOIN CURATED.DIM_PATIENT p 
ON f.patient_sk = p.patient_sk

GROUP BY p.patient_id, p.name;



CREATE OR REPLACE VIEW ANALYTICS.KPI_REVENUE AS
SELECT
    d.year,
    d.month,

    SUM(f.billing_amount) AS total_revenue

FROM CURATED.FACT_HEALTHCARE f
JOIN CURATED.DIM_DATE d 
ON f.date_sk = d.date_sk

GROUP BY d.year, d.month
ORDER BY d.year, d.month;

CREATE OR REPLACE VIEW ANALYTICS.KPI_NOSHOWS AS
SELECT
    dd.doctor_name,

    SUM(f.appointment_count) AS total_appointments,
    SUM(f.no_show_count) AS missed,

    ROUND(
        SUM(f.no_show_count) * 100.0 / NULLIF(SUM(f.appointment_count),0),
        2
    ) AS no_show_rate

FROM CURATED.FACT_HEALTHCARE f
JOIN CURATED.DIM_DOCTOR dd 
ON f.doctor_sk = dd.doctor_sk

GROUP BY dd.doctor_name
ORDER BY no_show_rate DESC;

select * from ANALYTICS.KPI_PATIENT_360;

select * from ANALYTICS.KPI_RISK_INDEX;

select * from ANALYTICS.KPI_NOSHOWS;

select * from ANALYTICS.KPI_REVENUE;