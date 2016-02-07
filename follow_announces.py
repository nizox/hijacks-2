# -*- coding: utf-8 -*-

import json
import logging

from kafka.consumer import KafkaConsumer
from tabi.rib import Radix

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="get a feed ofannounces from a BGP collector")
    parser.add_argument("collector")
    parser.add_argument("--offset", type=int)
    parser.add_argument("--prefixes-file")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    consumer = KafkaConsumer("rib-{}".format(args.collector),
                             bootstrap_servers=["comet-17-08.sdsc.edu:9092"],
                             group_id="follower")

    if args.offset is not None:
        topics = [("rib-{}".format(args.collector), 0, args.offset)]
        consumer.set_topic_partitions(*topics)

    # setup filters
    filters = []

    if args.prefixes_file is not None:
        filter_prefixes = Radix()
        with open(args.prefixes_file, "r") as f:
            for prefix in f:
                filter_prefixes.add(prefix.strip())

        def func(data):
            return data["prefix"] in filter_prefixes
        logger.info("filtering on prefixes from the file %s", args.prefixes_file)
        filters.append(func)

    for item in consumer:
        data = json.loads(item.value)
        if len(filters) == 0 or any(f(data) for f in filters):
            print(data)
