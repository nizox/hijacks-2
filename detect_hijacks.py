# -*- coding: utf-8 -*-
import os
import json
import logging

from datetime import datetime

from tabi.core import InternalMessage
from tabi.helpers import get_as_origin
from tabi.rib import EmulatedRIB
from tabi.emulator import detect_hijacks

from kafka import KafkaClient
from kafka.consumer import KafkaConsumer
from kafka.protocol import create_message
from kafka.common import ProduceRequest

logger = logging.getLogger()


PARTITIONS = {
    "rrc18": 0,
    "rrc19": 1,
    "rrc20": 2,
    "rrc21": 3,
}


def kafka_format(collector, message):
    data = json.loads(message)
    as_path = data.get("as_path", None)
    if as_path is not None:
        origins = frozenset(get_as_origin(as_path))
        yield InternalMessage(data.get("type", "U"),
                              data["timestamp"],
                              collector,
                              int(data["peer_as"]),
                              data["peer_ip"],
                              data["prefix"],
                              origins,
                              as_path)
    else:
        yield InternalMessage("W",
                              data["timestamp"],
                              collector,
                              int(data["peer_as"]),
                              data["peer_ip"],
                              data["prefix"],
                              None,
                              None)


class KafkaInputBview(object):
    """
    Emulates a bview from messages stored in a kafka topic.
    """

    def __init__(self, consumer, collector):
        self.consumer = consumer
        self.collector = collector

    def open(self):
        return self

    def close(self):
        pass

    def __iter__(self):
        tmp_rib = EmulatedRIB()

        # go to the tail of the log, we should get all prefixes from there
        self.consumer.set_topic_partitions(("rib-{}".format(self.collector), 0, 0))

        logger.info("consumer from the start to construct bview")
        for data in kafka_iter(self.consumer):
            # kafka internal message used to represent that a key was deleted
            if data is None:
                continue
            message = json.loads(data)
            node = tmp_rib.radix.add(message["prefix"])
            key = message["peer_as"]
            c = node.data.get(key, 0)
            # end of the bview if we already have this prefix in our RIB
            if c != 0:
                logger.info("end of bview")
                break
            node.data[key] = c + 1
            # fake bview
            message["type"] = "F"
            yield json.dumps(message)
        del tmp_rib

    def __enter__(self):
        return iter(self)

    def __exit__(self, exc_type, exc, traceback):
        pass


def kafka_iter(consumer):
    """
    Iterate over messages from the kafka topic.
    """
    for data in consumer:
        if data.value is not None:
            yield data.value


def kafka_input(collector, **options):
    group_id = options.pop("group_id", "hackathon")
    broker = options.pop("broker", os.getenv("KAFKA_BROKER", "").split(","))

    consumer = KafkaConsumer(collector, metadata_broker_list=broker,
                             group_id=group_id, auto_commit_enable=False)
    return {
        "collector": collector,
        "files": [KafkaInputBview(consumer, collector), kafka_iter(consumer)],
        "format": kafka_format
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("collector")
    parser.add_argument("--from-timestamp", type=float)
    parser.add_argument("--our-servers", default="localhost:9092")
    parser.add_argument("--irr-ro-file",
                        help="CSV file containing IRR route objects")
    parser.add_argument("--irr-mnt-file",
                        help="CSV file containing IRR maintainer objects")
    parser.add_argument("--irr-org-file",
                        help="CSV file containing IRR organisation objects")
    parser.add_argument("--rpki-roa-file",
                        help="CSV file containing ROA")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    kwargs = kafka_input(args.collector, broker=args.our_servers.split(","))

    if args.irr_ro_file is not None:
        kwargs["irr_ro_file"] = args.irr_ro_file

    if args.rpki_roa_file is not None:
        kwargs["rpki_roa_file"] = args.rpki_roa_file

    if args.irr_org_file is not None:
        kwargs["irr_org_file"] = args.irr_org_file

    if args.irr_mnt_file is not None:
        kwargs["irr_mnt_file"] = args.irr_mnt_file

    if args.from_timestamp is None:
        consumer = KafkaConsumer("conflicts",
                                 metadata_broker_list=args.our_servers.split(","),
                                 group_id="detector",
                                 auto_commit_enable=False)
        offset, = consumer.get_partition_offsets("conflicts", PARTITIONS[args.collector], -1, 1)
        consumer.set_topic_partitions({("conflicts", PARTITIONS[args.collector]): offset - 1})
        last_message = next(iter(consumer))
        last_data = json.loads(last_message.value)
        last_ts = last_data["timestamp"]
        logger.info("last detected event was at offset %s timestamp %s", offset, last_ts)
    else:
        last_ts = args.from_timestamp

    logger.info("detecting conflicts newer than %s", datetime.utcfromtimestamp(last_ts))

    client = KafkaClient(args.our_servers.split(","))
    for msg in detect_hijacks(**kwargs):
        ts = msg.get("timestamp", 0)
        if last_ts is None or ts > last_ts:
            if msg.get("type", "none") == "ABNORMAL":
                client.send_produce_request([ProduceRequest("conflicts", PARTITIONS[args.collector], [create_message(json.dumps(msg))])])
