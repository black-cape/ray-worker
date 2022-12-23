import click
from kafka import KafkaConsumer
from typing import Dict
import json

@click.command()
@click.option('--boot-strap-server', default='localhost:9093', help='boot strap server')
@click.option('--kafka-topic', default='castiron_text_payload', help='kafka topic with which to listen')
def main(kafka_topic: str, boot_strap_server: str):
    """
    Just consume from Kafka topic
    """
    k = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[boot_strap_server],
        auto_offset_reset='latest',
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode('utf-8') if k is not None else k,
        value_deserializer=lambda v: json.loads(v) if v is not None else v
    )


    print(k.topics())

    print(f'Okay waiting for new data on topic {kafka_topic}')

    while True:
        records_dict = k.poll(timeout_ms=1000, max_records=1)

        non_arg_keys = ['worker_run_method', 'data']
        for topic_partition, consumer_records in records_dict.items():
            for record in consumer_records:
                print(f"worker run method is {record.value['worker_run_method']}")
                for _ in record.value['data'].split('\r\n'):
                    print(_)

                print('args list')
                for _ in record.value.keys():
                    if _ not in non_arg_keys:
                        print(f'{_}:{record.value[_]}')


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
