# Code for testing Oracle database failover timings

[Full blog post about the results is here](https://ilmarkerm.eu/blog/2023/02/testing-client-failover-with-data-guard-fast-start-failover-1-setup/)

Requires oracledb python module and Oracle instantclient (it runs in thick mode... for now). I only tested it with instantclient 21.9.

It runs multiple simple test queries on the target database, in multiple threads. It logs all errors received, and when the database driver reports that the connection is no longer valid, it tries to reconnect and resume testing as fast as possible. All timestamps are logged.

## Analysing the results

Results are written in CSV format to /tmp/failover.csv

Easiest is to map this file as Oracle external table and just query it as you like.

```sql
create table failtest_ext (
    testcode varchar2(100),
    logtime varchar2(100),
    stmt_start_time varchar2(100),
    testname varchar2(100),
    testphase varchar2(100),
    testresult varchar2(100),
    instance_name varchar2(100),
    errcode varchar2(100),
    errmsg varchar2(200)
) organization external (
    default directory DATAPUMP_DIR
    access parameters (
        RECORDS DELIMITED BY 0x'0D0A'
        FIELDS TERMINATED BY ',' optionally enclosed by '"'
    )
    location ('failover.csv')
);

-- This is to change timestamp strings into database timestamp
create or replace view failtest as
SELECT testcode, 
    to_timestamp_tz(logtime,'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm') logtime,
    to_timestamp_tz(stmt_start_time,'yyyy-mm-dd"T"hh24:mi:ss.fftzh:tzm') stmt_start_time,
    testname, testphase, testresult, instance_name, errcode, errmsg
FROM failtest_ext;
```

One example query

```sql
WITH tr AS (
    SELECT *
    FROM failtest
    WHERE testcode='rerun_local_switchover' AND testname in ('read','write')
),
minmaxtime AS (
    SELECT cast(min(logtime)-numtodsinterval(5, 'second') as timestamp(0) with time zone) mintime, 
    max(logtime)+numtodsinterval(5, 'second') maxtime
    FROM tr WHERE testresult='False'
),
rowgenerator(r) AS (
    SELECT 1 r FROM dual
    UNION ALL
    SELECT r+1 FROM rowgenerator WHERE r < 600
),
timegrid AS (
    SELECT cast(minmaxtime.mintime+numtodsinterval(rowgenerator.r, 'second') as timestamp(0) with time zone) ts
    FROM rowgenerator CROSS JOIN minmaxtime
    WHERE minmaxtime.mintime+numtodsinterval(rowgenerator.r, 'second') < minmaxtime.maxtime
)
SELECT p.*, ts-minmaxtime.mintime-numtodsinterval(5, 'second') delta FROM (
    SELECT * FROM (
            SELECT timegrid.ts, tr.testname, tr.testresult, tr.instance_name, tr.errcode
            FROM timegrid LEFT OUTER JOIN tr ON timegrid.ts = cast(tr.logtime as timestamp(0) with time zone)
    ) PIVOT (
        MAX(testresult) AS res, max(instance_name) as inst, max(errcode) as err
        FOR testname in ('read' AS read,'write' AS write)
    )
) p CROSS JOIN minmaxtime
ORDER BY ts
;
```
