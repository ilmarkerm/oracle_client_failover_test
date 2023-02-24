"""
This program tests how quick database failover is
2023 Ilmar Kerm
"""

import oracledb, logging, random, sys, csv, signal, string
from datetime import datetime, timedelta, timezone
from time import sleep
from threading  import Thread
from queue import Queue
from pathlib import Path

# Initialize logging
logger = logging.getLogger("failover_main")
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.getLogger().setLevel(logging.DEBUG)
# Config
results_file = Path('/tmp/failover.csv') # Test results will be written to this file in CSV format
time_limit = timedelta(seconds=600) # How long the test will run
sleep_time = timedelta(seconds=1) # How much sleep time after each query execution
# testcode is just a code to identify each test
if len(sys.argv) > 1:
    # Test code was supplied from command line
    testcode = sys.argv[1]
else:
    # Generate test code randomly
    letters = string.ascii_lowercase
    testcode = ''.join(random.choice(letters) for i in range(10))
# how to connect to the database
oracle_connect = {
    'user': "soe",
    "password": "soe",
    "dsn": """(description=(failover=on)(connect_timeout=2)(transport_connect_timeout=1 sec)
            (address_list=(load_balance=on)
                (address=(protocol=tcp)(host=failtest-1)(port=1521))
                (address=(protocol=tcp)(host=failtest-2)(port=1521))
            )(connect_data=(service_name=soe.prod1.dbs)))"""
}

def connect_db():
    global oracle_connect
    db = oracledb.connect(
        user=oracle_connect['user'], password=oracle_connect['password'],
        dsn=oracle_connect['dsn']
    )
    db.call_timeout = 5000
    return db

def queue_result(q, testname, starttime, ok=True, phase='execute', instancename='-', errObj=None):
    global testcode
    d = {
        'testcode': testcode,
        'time': datetime.now(tz=timezone.utc),
        'starttime': starttime,
        'test': testname,
        'phase': phase,
        'result': ok,
        'instancename': instancename
    }
    if errObj is not None:
        d['errcode'] = errObj.full_code
        d['errmsg'] = errObj.message
    q.put(d, timeout=1)

def test_wrapper(db_conn, q, testname, test_executer, autocommit=True):
    # Executes a test, takes measurements and in case of failure reconnects to the database
    global stop_threads, sleep_time
    # A little bit of added randomness in wait times cannot hurt
    random_sleep = random.uniform(0, 0.1)
    # Database connection does not exist for this test, do the initial connection
    if db_conn is None:
        db_conn = connect_db()
    # Next line makes all Oracle scene people scared and screaming in horror. Halloween nightmare story for Oracle scene.
    db_conn.autocommit = autocommit
    # Nightmare over
    while True:
        # Set module name for the database connection
        db_conn.module = testname
        #
        with db_conn.cursor() as c:
            # Get instance name
            instance_name = None
            while True:
                starttime = datetime.now(tz=timezone.utc)
                try:
                    # In case of TAF this may change without Python driver realising it
                    for row in c.execute("SELECT sys_context('userenv','instance_name') FROM dual"):
                        instance_name = row[0]
                    # This is the part that actually executes the test
                    test_executer(c)
                    if not db_conn.autocommit:
                        db_conn.commit()
                    # If test was successful, log success message
                    queue_result(q, testname, starttime, ok=True, instancename=instance_name)
                except Exception as e:
                    # We got an exception
                    errorObj, = e.args
                    queue_result(q, testname, starttime, ok=False, errObj=errorObj, instancename=instance_name)
                    if not db_conn.is_healthy():
                        # If database connection should not be used anymore, reconnect
                        db_conn = None
                        break
                if stop_threads:
                    break
                # Lets sleep a little before running the test again
                sleep(sleep_time.total_seconds()+random_sleep)
        if stop_threads:
            break
        if db_conn is None:
            # Database connection is not valid anymore, reconnect
            while True:
                starttime = datetime.now(tz=timezone.utc)
                try:
                    db_conn = connect_db()
                    queue_result(q, testname, starttime, ok=True, phase='connect')
                    break
                except Exception as e:
                    # Reconnection failed, log the result and try again
                    errorObj, = e.args
                    db_conn = None
                    queue_result(q, testname, starttime, ok=False, phase='connect', errObj=errorObj)
                    sleep(0.5)
                if stop_threads:
                    break
        if stop_threads:
            break

def read_test_executor(c):
    # Takes a cursor object and executes a read query
    for row in c.execute("SELECT d FROM conntest_write FETCH FIRST 1 ROWS ONLY"):
        pass

def write_test_executor(c):
    # Takes a cursor object and executes a write query
    # Since autocommit is on, no need to send and extra commit command, it is all handled in the same network packet
    c.execute("insert into CONNTEST_WRITE (d) values (sys_extract_utc(systimestamp))")

def long_write_test_executor(c):
    # Make a really long running transaction
    for i in range(10):
        c.execute("insert into CONNTEST_WRITE (d) values (sys_extract_utc(systimestamp))")
        sleep(0.1)

def read_test(db_conn, q):
    test_wrapper(db_conn, q, 'read', read_test_executor)

def write_test(db_conn, q):
    test_wrapper(db_conn, q, 'write', write_test_executor)

def long_write_test(db_conn, q):
    test_wrapper(db_conn, q, 'long_write', long_write_test_executor, autocommit=False)

def write_results(q):
    # This processes the results coming in from the testing threads
    global stop_threads
    with results_file.open("a", encoding='utf8', newline='') as f:
        c = csv.writer(f)
        while True:
            if q.empty():
                sleep(1)
            else:
                # Write results in CSV format to file
                # So it would be easy to analyse them using an external table
                item = q.get(timeout=1)
                csvdata = [
                    item['testcode'],
                    item['time'].isoformat(),
                    item['starttime'].isoformat(),
                    item['test'],
                    item['phase'],
                    item['result'],
                    item['instancename'],
                    item.get('errcode', '0'),
                    item.get('errmsg', '-').replace("\n", ", ")
                ]
                c.writerow(csvdata)
                #f.write(f"{item['testcode']}\n")
                print(item)
            if stop_threads and q.empty():
                break

def signal_handler(signum, frame):
    # If OS signal was received, exit threads nicely
    global stop_threads
    stop_threads = True
    logger.info("Termination signal received")


#
# MAIN PROGRAM
#
if __name__ == '__main__':
    # Initialize thick mode
    oracledb.init_oracle_client()

    logger.info(f"Test code: {testcode}")
    logger.info("Creating database connections...")
    db_write = connect_db()
    # Create a table for testing purposes
    with db_write.cursor() as c:
        table_exists = False
        for rows in c.execute("SELECT count(*) FROM user_tables where table_name='CONNTEST_WRITE' AND dropped='NO'"):
            if rows[0] == 1:
                table_exists = True
        if table_exists:
            logger.info("Truncating existing table")
            c.execute("truncate table CONNTEST_WRITE")
        else:
            logger.info("Creating new table")
            c.execute("create table CONNTEST_WRITE (d timestamp) pctfree 0")

    # Register signal handlers
    ign = signal.signal(signal.SIGTERM, signal_handler)
    ign = signal.signal(signal.SIGINT, signal_handler)
    # Starting threads
    logger.info("Starting threads")
    stop_threads = False
    result_queue = Queue()
    test_threads = [
        Thread(target=write_test, args=(db_write, result_queue)),
        Thread(target=read_test, args=(None, result_queue)),
        Thread(target=long_write_test, args=(None, result_queue))
    ]
    result_thread = Thread(target=write_results, args=(result_queue,))
    for t in test_threads:
        t.start()
    result_thread.start()

    # Doing it this way to handle the OS signal interrupts properly in case test is cancelled early
    for i in range(1, int(time_limit.total_seconds())):
        sleep(1)
        if stop_threads:
            break
    if not stop_threads:
        stop_threads = True
    # Wait for threads to finish
    for t in test_threads:
        t.join()
    result_thread.join()
