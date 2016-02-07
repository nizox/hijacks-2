# -*- coding: utf-8 -*-

import logging
import json

from kafka.consumer import KafkaConsumer



PARTITIONS = {
    "rrc18": 0,
    "rrc19": 1,
    "rrc20": 2,
    "rrc21": 3,
}



def caida_filter_annaunce(relation_name,cone_name):

    relations = {}
    childs = {}
    parents = {}

    fin = open(relation_name,"r");
    
    
    valid=0
    total=0
    print("start reading relationship")
    
    for line in fin:
        ls = line.split("|")
        try:
            val0 = int(ls[0])
            val1 = int(ls[1])
            if(val0 not in relations): relations[val0]={}
            if(val1 not in relations[val0]): relations[val0][val1]=int(ls[2])
            if(val1 not in relations): relations[val1]={}
            if(val0 not in relations[val1]): relations[val1][val0]=int(ls[2])
        except ValueError:
            notn=1;

    print("start reading cone")    

    fin = open(cone_name,"r");
    
    for line in fin:
        ls = line.split()
        try:
            parent = int(ls[0])
            if(parent not in childs):
                childs[parent]=set()
            for i in range(0,len(ls)):
                tmp_child=int(ls[i])
                childs[parent].add(int(tmp_child))
                if(tmp_child not in parents):
                    parents[tmp_child]=set()
                parents[tmp_child].add(parent)
        except ValueError:
            notn=1;
                    
            
    return relations,childs,parents

def is_legittimate(relations,childs,parents, data):

    p1=int(data["announce"]["asn"])
    p2=int(data["conflict_with"]["asn"])
    legittimate=0
    
    if(p1 in relations and p2 in relations[p1]): legittimate=1
    if(p2 in relations and p1 in relations[p2]): legittimate=1
    
    if(p1 in childs): #if p1 has a parent means that it is a child
        if(p2 in childs[p1]): legittimate=1
    
    if(p2 in childs):
        if(p1 in childs[p2]): legittimate=1
        
    data["caida_relation"] = bool(legittimate)
    return legittimate

if __name__ == "__main__":
    import argparse

    relations,childs,parents=caida_filter_annaunce("20160101.as-rel.txt","20160101.ppdc-ases.txt")
 
    print(len(relations),len(childs),len(parents))
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

    hijacks=0
    total=0
    for item in consumer:
        total+=1     
        if(is_legittimate(relations,childs,parents, json.loads(item.value))==0): 
            hijacks+=1 
            #print(item.value)
            
        if(total==10000): print(total,hijacks)
