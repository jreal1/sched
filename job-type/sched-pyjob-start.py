import psycopg2, sys, json, socket, datetime
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

conn = psycopg2.connect(database="scheduler", user = "postgres", password = "postgrespw", host = "host.docker.internal", port = "32768")
cur = conn.cursor()

consumer_conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': 'sched-pyjob-start',
    'auto.offset.reset': 'smallest',
    'client.id': socket.gethostname(),
    'enable.auto.commit': 'false'
}

producer_conf = {
    'bootstrap.servers': "localhost:9092",
    'client.id': socket.gethostname()
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
running = True

def msg_process(msg):
    # Get message contents
    vals = json.loads(msg.value())

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

    return True

try:
    consumer.subscribe(['sched-pyjob-start'])

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            if msg_process(msg):
                consumer.commit(asynchronous=False)
finally:
    # Close down consumer to commit final offsets.
    cur.close()
    consumer.close()
    print('Exiting')
