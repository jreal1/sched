import psycopg2, sys, json, socket, datetime
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

conn = psycopg2.connect(database="scheduler", user = "postgres", password = "postgrespw", host = "host.docker.internal", port = "32768")
cur = conn.cursor()

producer_conf = {
    'bootstrap.servers': "localhost:9092",
    'client.id': socket.gethostname()
}

producer = Producer(producer_conf)

# Update job_status table that job is starting
sql = 'insert into job_status (timestamp, job_id, status, message) VALUES (\'%s\',%s,\'Start\',\'\');' % (datetime.datetime.now(), vals["job_id"])
cur.execute(sql)
print(sql)
conn.commit()

# Do work
print(vals["job_id"])

# Signal complete
producer.produce('sched-job-complete', value='{"timestamp":"%s", "job_id":"%s", "status":"Success", "message":""}' % (datetime.datetime.now(), vals["job_id"]))
producer.flush()