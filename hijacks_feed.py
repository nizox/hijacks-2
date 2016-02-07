# -*- coding: utf-8 -*-

import logging

from kafka.consumer import KafkaConsumer


PARTITIONS = {
    "rrc18": 0,
    "rrc19": 1,
    "rrc20": 2,
    "rrc21": 3,
}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="get a feed of abnormal BGP conflicts")
    parser.add_argument("--offset", type=int)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    consumer = KafkaConsumer("hijacks",
                             bootstrap_servers=["comet-17-08.sdsc.edu:9092"],
                             group_id="client")
    if args.offset is not None:
        topics = [("hijacks", i, args.offset) for i in PARTITIONS.values()]
        consumer.set_topic_partitions(*topics)
    for item in consumer:
        print(item)
