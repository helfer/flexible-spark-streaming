from __future__ import print_function

import re
import sys
from operator import add

from pyspark import SparkContext

from queryparser.Query import Query

# The scheduler runs synchronosuly for now. It should become its own process which receives signals from the
# dirwatcher and the queryparser. The queryparser will be responsible for registering and unregistering
# queries while the dirwatcher will be responsible for notifying the scheduler of new files.

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: scheduler <file>", file=sys.stderr)
        exit(-1)

    # Initialize the spark context.
    sc = SparkContext(appName="FlexibleStreaming")

    # Loads in input files. They should be in format of:
    #     tweet
    #     tweet
    #     tweet
    #     ...
    tweets = sc.textFile(sys.argv[1], 1)

    tag_list = ['#test']
    q = Query(tag_list)

    # Loads all URLs from input file and initialize their neighbors.
    count = tweets.filter(q.filter).count()

    print("%s tweets match the filter: %s." % (count, tag_list))

    sc.stop()
