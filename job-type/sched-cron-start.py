import psycopg2, sys, json, socket, datetime
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

#job_id should be the first and only argument
job_id = sys.argv.pop()

conn = psycopg2.connect(database="scheduler", user = "postgres", password = "postgrespw", host = "host.docker.internal", port = "32768")
cur = conn.cursor()

producer_conf = {
    'bootstrap.servers': "localhost:9092",
    'client.id': socket.gethostname()
}

producer = Producer(producer_conf)

# Update job_status table that job is starting
sql = 'insert into job_status (timestamp, job_id, status, message) VALUES (\'%s\',%s,\'Start\',\'\');' % (datetime.datetime.now(), sys.argv[1])
cur.execute(sql)
print(sql)
conn.commit()

# Do work
print('Signaling completion of job_id=' + sys.argv[1])

# Signal complete
producer.produce('sched-job-complete', value='{"timestamp":"%s", "job_id":"%s", "status":"Success", "message":""}' % (datetime.datetime.now(), sys.argv[1]))
producer.flush()