#!/usr/bin/env python2

import argparse
import json
import os
import signal
import traceback

import scheduler
import tweetdownloader

NORMAL_EXIT = "java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext"

def main():
    global stop_list

    # CONFIGURATION
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--watch_dir",
        help="the directory where new input files will be added",
        default="data/")
    parser.add_argument(
        "-w", "--window",
        type=int,
        help="the window size (in ms) for reading input data, default=10000",
        default=10000)
    parser.add_argument('--no-firehose', dest='firehose', action='store_false')
    parser.set_defaults(firehose=True)

    args = parser.parse_args()

    # Normalize watch_dir and ensure it exists
    if args.watch_dir[-1] != "/":
        args.watch_dir += "/"
    if not os.path.exists(args.watch_dir):
        os.makedirs(args.watch_dir)

    # Read Twitter API credentials from a file
    with open(".twitter.json") as f:
        creds = json.load(f)

    # STARTUP
    print("Initializing demo...")

    if args.firehose:
        print("* Starting tweet downloader")
        td = tweetdownloader.TweetDownloader(
            args.watch_dir,
            creds['consumer_key'],
            creds['consumer_secret'],
            creds['access_key'],
            creds['access_secret'],
            args.window)
        stop_list.append(td)
        td.start()
    else:
        print("* Running in static mode, not downloading tweets")

    print("* Starting scheduler")
    fss = scheduler.FlexibleStreamingScheduler(args.watch_dir)
    stop_list.append(fss)
    fss.start()

    # fss.start() will return after 10 iterations...
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
