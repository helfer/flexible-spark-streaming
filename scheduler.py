from __future__ import print_function

import os
import re
import signal
import sys
import time

from collections import deque
from operator import add

from pyspark import SparkContext

from queryparser.Query import Query
from dirwatcher.dirwatcher import DirWatcher

# The scheduler runs synchronosuly for now. It should become its own process which receives signals from the
# dirwatcher and the queryparser. The queryparser will be responsible for registering and unregistering
# queries while the dirwatcher will be responsible for notifying the scheduler of new files.


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: scheduler <dir to watch>", file=sys.stderr)
        exit(-1)

    keep_going = True

    # exit gracefully on stop
    # XXX doesn't actually work. signal handler never gets called... oh noes!
    print('register signal-handler')
    def signal_handler(signal, frame):
        global keep_going
        print('Stopping')
        keep_going = False

    signal.signal(signal.SIGINT, signal_handler)

    # Initialize the spark context.
    sc = SparkContext(appName="FlexibleStreaming")

    # deque is thread-safe
    new_tweet_files = deque()
    def register_new_tweet_files(changes):
        if changes['added']:
            new_tweet_files.extend(changes['added'])

    print('start dirwatcher')
    dw = DirWatcher(sys.argv[1], register_new_tweet_files)
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
    sc.stop()
    print('Stopped')
