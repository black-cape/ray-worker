import click
from kafka import KafkaProducer
import time
import json

@click.command()
@click.option('--boot-strap-server', default='localhost:9093', help='boot strap server')
@click.option('--kafka-topic', default='castiron_text_payload', help='kafka topic with which to listen')
@click.option('--worker-run-method', default='etl.example.example_text_stream_processor.process', help='kafka topic with which to listen')
def main(kafka_topic: str, boot_strap_server: str, worker_run_method: str):
    k = KafkaProducer(bootstrap_servers=[boot_strap_server])

    data: str = json.dumps({"field1": 1, "field2": 2})

    while True:
        #see corresponding processor in etl/example/example_text_stream_processor.py
        msg_dict = {'worker_run_method': worker_run_method, 'data': data, 'arg1': 'test', 'arg2': 'test2'}
        print(f'sending to topic: {kafka_topic} {json.dumps(msg_dict)}')

        k.send(kafka_topic, json.dumps(msg_dict).encode())
        k.flush()
        time.sleep(2)



if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
