# -*- coding: utf-8 -*-

import os
import sys
import json
import cPickle
import logging

from tabi.core import InternalMessage

from kafka.consumer import KafkaConsumer
from kafka import KafkaClient
from kafka.common import ProduceRequest
from kafka.protocol import create_message

logger = logging.getLogger(__name__)

RIPE_SERVERS = ["node{}.kafka.ris.ripe.net".format(i) for i in range(1, 6)]


def group_by_n(it, n):
    acc = []
    for elem in it:
        acc.append(elem)
        if len(acc) == n:
            yield acc
            del acc[:]
    yield acc


def exabgp_as_path(as_path):
    res = []
    for entry in as_path:
        if isinstance(entry, list):
            res.append("\{{}\}".format(",".join([str(i) for i in entry])))
        else:
            res.append(str(entry))
    return " ".join(res)


def exabgp_format(collector, message):
    neighbor = message["neighbor"]
    update = neighbor["message"].get("update")
    if update is not None:
        announce = update.get("announce")
        withdraw = update.get("withdraw")
        attribute = update.get("attribute")
        if announce is not None and attribute is not None:
            as_path = attribute.get("as-path", [])
            if len(as_path) > 0:
                origin = as_path[-1]
                for item in announce.get("ipv4 unicast", {}).values():
                    for prefix in item.keys():
                        yield InternalMessage("U", float(message["time"]), collector, int(neighbor["asn"]["peer"]), neighbor["ip"], prefix, origin, exabgp_as_path(as_path))
                for item in announce.get("ipv6 unicast", {}).values():
                    for prefix in item.keys():
                        yield InternalMessage("U", float(message["time"]), collector, int(neighbor["asn"]["peer"]), neighbor["ip"], prefix, origin, exabgp_as_path(as_path))
        if withdraw is not None:
            for item in withdraw.get("ipv4 unicast", {}).values():
                for prefix in item.keys():
                    yield InternalMessage("U", float(message["time"]), collector, int(neighbor["asn"]["peer"]), neighbor["ip"], prefix, None, None)
            for item in withdraw.get("ipv6 unicast", {}).values():
                for prefix in item.keys():
                    yield InternalMessage("U", float(message["time"]), collector, int(neighbor["asn"]["peer"]), neighbor["ip"], prefix, None, None)
    else:
        logger.error("got %s", message)


def messages_from_internal(it):
    for msg in it:
        ts = msg.timestamp
        key = "{}-{}".format(msg.prefix, msg.peer_as)
        if msg.as_path is None:
            yield create_message(json.dumps({"timestamp": ts,
                                             "prefix": msg.prefix,
                                             "peer_ip": msg.peer_ip,
                                             "peer_as": msg.peer_as}),
                                 key)
            yield create_message(None, key)
        else:
            yield create_message(json.dumps({"timestamp": ts,
                                             "prefix": msg.prefix,
                                             "as_path": msg.as_path,
                                             "peer_ip": msg.peer_ip,
                                             "peer_as": msg.peer_as}),
                                 key)


def iterate_messages(consumer, collector):
    for msg in consumer:
        bgp_data = json.loads(msg.value)
        if bgp_data["type"] == "update":
            for item in exabgp_format(collector, bgp_data):
                yield item


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("collector")
    parser.add_argument("--from-beginning")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    collector = sys.argv[1]

    consumer = KafkaConsumer("raw-{}".format(args.collector),
                             group_id='test_hackathon10',
                             bootstrap_servers=RIPE_SERVERS)

    save_file = "offsets-{}".format(args.collector)
    if args.from_beginning:
        logger.info("starting from scratch")
        offsets = {("raw-{}".format(args.collector), i): 0 for i in range(0, 10)}
        consumer.set_topic_partitions(offsets)
    elif os.path.exists(save_file):
        with open(save_file, "r") as f:
            offsets = cPickle.load(f)
        logger.info("loading offsets from file: %s", offsets)
        consumer.set_topic_partitions(offsets)
    else:
        logger.info("starting from last messages")

    client = KafkaClient("localhost:9092")
    count = 0
    for batch in group_by_n(messages_from_internal(iterate_messages(consumer, collector)), 1000):
        req = ProduceRequest("rib-{}".format(collector), 0, batch)
        count += len(batch)
        logger.info("sending %i", count)
        res = client.send_produce_request([req])
        offsets = consumer.offsets("fetch")
        try:
            with open(save_file, "w") as f:
                f.write(cPickle.dumps(offsets))
        except:
            logger.warning("could not write offsets to %s", save_file)
            pass
