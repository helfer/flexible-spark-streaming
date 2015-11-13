from __future__ import print_function

import os
import re
import signal
import sys
import time
import argparse

from collections import deque
from operator import add

from pyspark import SparkContext

from queryparser.Query import Query
from dirwatcher.dirwatcher import DirWatcher
from twitterstream.twitterstream import TwitterStream

# The scheduler runs synchronosuly for now. It should become its own process which receives signals from the
# dirwatcher and the queryparser. The queryparser will be responsible for registering and unregistering
# queries while the dirwatcher will be responsible for notifying the scheduler of new files.


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
              "watch_dir",
              help="the directory where new tweet files will be added")
    parser.add_argument(
              "consumer_key",
              help="Twitter API key")
    parser.add_argument(
              "consumer_secret",
              help="Twitter API secret")
    parser.add_argument(
              "access_key",
              help="Twitter API access token")
    parser.add_argument(
              "access_secret",
              help="Twitter API access token secret")
    parser.add_argument(
              "-p", "--prefix",
              help="prefix for tweet file names, default=tweets",
              default="tweets")
    parser.add_argument(
              "-s", "--suffix",
              help="suffix for tweet files, default=txt",
              default="txt")
    parser.add_argument(
              "-w", "--window",
              type=int,
              help="the window size (in ms) for reading tweets, default=10000",
              default=10000)
    parser.add_argument(
              "-v", "--verbose",
              help="display more messages for debugging",
              action="store_true")
    args = parser.parse_args()

    # normalize the path syntax
    if args.watch_dir[-1] != "/":
        args.watch_dir += "/"

    # if the watch directory does not exist, create it and clean it at the end.
    if not os.path.exists(args.watch_dir):
        print("creating " + args.watch_dir)
        os.makedirs(args.watch_dir)

    keep_going = True

    # exit gracefully on stop
    # XXX doesn't actually work. signal handler never gets called... oh noes!
    print("register signal-handler")
    def signal_handler(signal, frame):
        global keep_going
        print("Stopping")
        keep_going = False

    signal.signal(signal.SIGINT, signal_handler)

    # Initialize the spark context.
    sc = SparkContext(appName="FlexibleStreaming")

    print("start twitterstream")
    ts = TwitterStream(
             args.watch_dir,
             args.consumer_key,
             args.consumer_secret,
             args.access_key,
             args.access_secret,
             args.prefix,
             args.suffix,
             args.window,
             args.verbose)
    ts.start()

    # deque is thread-safe
    new_tweet_files = deque()
    def register_new_tweet_files(changes):
        if changes['added']:
            new_tweet_files.extend(changes['added'])

    print("start dirwatcher")
    dw = DirWatcher(args.watch_dir, register_new_tweet_files)
    dw.start()

    # dummy parse tweet method:
    def parse_tweet(line):
        return {'text': line.strip()}

    # dummy query for testing:
    tag_list = ['#test']
    q = Query(tag_list)
    q2 = Query(['#spark'])

    n = 0
    # just run for about 5 seconds so I don't have to manually kill the dirwatcher
    while keep_going and n < 10:
        # let's assume this gets called often enough to empty the queue...

        if new_tweet_files:
            filename = os.path.abspath(os.path.join(sys.argv[1], new_tweet_files.popleft()))
            print("new file: %s" % (filename))
            lines = sc.textFile(filename, 3) # 5 is the number of blocks


            # Loads all URLs from input file and initialize their neighbors.
            tweets = lines.map(parse_tweet)
            count = tweets.filter(q.filter).count()
            count2 = tweets.filter(q2.filter).count()

            print("%s tweets match the filter: %s." % (count, tag_list))
            print("%s tweets match the filter: %s." % (count2, ['#spark']))

        n += 1
        time.sleep(0.5)

    dw.stop()
    ts.stop()
    sc.stop()
    print("stopped")

