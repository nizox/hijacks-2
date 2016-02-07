# -*- coding: utf-8 -*-

import json
import logging

from kafka.consumer import KafkaConsumer

from tabi.rib import Radix
from tabi.helpers import get_as_origin

logger = logging.getLogger(__name__)


COLLECTORS = ["rrc18", "rrc19", "rrc20", "rrc21", "caida-bmp"]


def find_more_specific(consumer):
    rib = Radix()
    for item in consumer:
        data = json.loads(item.value)
        for node in rib.search_covering(data["prefix"]):
            for peer, as_path in node.data.iteritems():
                if peer != data["peer_as"]:
                    yield (node.prefix, as_path, data["prefix"], data["as_path"])
        node = rib.add(data["prefix"])
        node.data[data["peer_as"]] = data["as_path"]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="get a feed ofannounces from a BGP collector")
    parser.add_argument("collector", nargs="*")
    parser.add_argument("--offset", type=int)
    parser.add_argument("--anycast-file")
    parser.add_argument("--our-servers", default=",".join(["comet-17-22.sdsc.edu:9092"]))

    args = parser.parse_args()

    collectors = COLLECTORS if len(args.collector) == 0 else args.collector

    logging.basicConfig(level=logging.INFO)

    topics = ["rib-{}".format(c) for c in collectors]
    logger.info("using topics %s", topics)

    consumer = KafkaConsumer(*topics,
                             bootstrap_servers=args.our_servers.split(","),
                             group_id="follower")

    if args.offset is not None:
        consumer.set_topic_partitions({(t, 0): args.offset for t in topics})

    # setup filters
    filters = []

    if args.anycast_file is not None:
        filter_prefixes = Radix()
        count = 0
        with open(args.anycast_file, "r") as f:
            for prefix in f:
                if not prefix.startswith("#"):
                    filter_prefixes.add(prefix.strip())
                    count += 1
        logger.info("loaded %s prefixes in the anycast list", count)

        def func(event):
            nodes = list(filter_prefixes.search_covering(event[3]))
            return len(nodes) > 0

        logger.info("filtering on prefixes from the file %s", args.anycast_file)
        filters.append(func)
    else:
        raise ValueError("please provide a anycast prefix list file")

    for event in find_more_specific(consumer):
        filtered = any(f(event) for f in filters)
        print(event, "anycast" if filtered else "unknown")
