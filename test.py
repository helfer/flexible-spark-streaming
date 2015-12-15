#!/usr/bin/env python2

import argparse
import json
import os
import signal
import traceback

import scheduler
import tweetdownloader
import queryparser

NORMAL_EXIT = "java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext"

def main():
    global stop_list

    queries = []
    queries.append(queryparser.SimpleQuery("HAPPY-1", {'agg': 'count', 'field': '*'}, {'text': {'_contains': 'happy'}}))
    queries.append(queryparser.SimpleQuery("HAPPY-2", {'agg': 'count', 'field': '*'}, {'text': {'_contains': 'happy'}}))
    queries.append(queryparser.SimpleQuery("SAD-1", {'agg': 'count', 'field': '*'}, {'text': {'_contains': 'sad'}}))

    fss = scheduler.FlexibleStreamingScheduler("test/", test_queries=queries)
    stop_list.append(fss)
    fss.start()
    shutdown()

stop_list = []
def shutdown():
    global stop_list
    for entry in stop_list:
        try:
            entry.stop()
        except:
            traceback.print_exc()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("KeyboardInterrupt!")
        shutdown()
        exit(0)
    except Exception, e:
        if NORMAL_EXIT in str(e):
            print("Exiting gracefully...")
            shutdown()
            exit(0)
        else:
            print("Exception in demo.py")
            traceback.print_exc()
            shutdown()
            exit(1)
