import argparse
import collections
import os
import time

import pyspark

import dirwatcher
from queryparser import SimpleQuery
import wrapper



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w", "--wrapper",
        help="which wrapper to use",
        default=None)
    parser.add_argument(
        "-d", "--dir",
        help="which data directory to use",
        default=None)
    parser.add_argument(
        "-r", "--repetitions",
        type=int,
        help="number of repetitions to run",
        default=5)
    parser.add_argument(
        "-n", "--num_queries",
        type=int,
        help="number of queries to run",
        default=1)
    parser.add_argument(
        "-s", "--start",
        type=int,
        help="number of queries to start with",
        default=1)
    parser.add_argument('--sample', dest='sample', action='store_true')
    parser.set_defaults(sample=False)
    args = parser.parse_args()

    if args.wrapper == 'subquery':
        tw = wrapper.CommonSubqueryWrapper
    elif args.wrapper == 'scan':
        tw = wrapper.ScanSharingWrapper
    elif args.wrapper == 'aggregate':
        tw = wrapper.AggregateWrapper
    elif args.wrapper == 'plain':
        tw = wrapper.Wrapper
    else:
        tw = None
    test_wrapper = tw
    print('wrapper:',tw)

    fts = FlexibleTestScheduler()

    query_terms = ['happy','sad','and','the','I','One Direction','1D','Trump','Hillary','Sanders']

    queries = []

    for qt in query_terms:
        queries.append( SimpleQuery( qt, { 'agg': 'count', 'field': '*' }, { 'text': { '_contains': qt } } ) )


    source = './largedata/'
    if args.dir:
        source = args.dir
    if args.sample:
        source += 'tweets-1449729650086.txt'

    alltimers = []
    for i in range( args.start ,args.num_queries+1):
        timers = fts.run(queries[:i], test_wrapper, data_source = source, repetitions = args.repetitions)
        alltimers.append(timers)
        print( str(i) + '>>>>>>>' )
        print( timers )


    print( 'RESULTS >>>>>>>', args.wrapper )
    for i, timers in enumerate(alltimers):
        print(args.start + i, args.wrapper, len(timers), sum(timers)/len(timers), timers )

    print( 'DONE >>>>>>>' )


class FlexibleTestScheduler():

    def __init__(self):
        self.sc = pyspark.SparkContext(appName="FlexibleStreaming")

    def run(self, queries, test_wrapper, data_source = './largedata', repetitions = 5):

        # INITIALIZE
        timers = []

        def parse_input(i):
            return json.loads(i) if len(i) > 0 else {}

        # RUN LOOP
        for i in range(0, repetitions):
            queries = queries

            # START TIMER
            start = time.time()

            # reading the entire watch-dir
            if test_wrapper is None:
                lines = self.sc.textFile( data_source )
            else:
                lines = test_wrapper(self.sc.textFile( data_source ))
            #     no minimum line param in case of empty file

            total = lines.count()

            results = [q.apply(lines) for q in queries]

            if test_wrapper is None:
                counts = results
            else:
                total = total.__eval__()
                counts = [rdd.__eval__() for rdd in results]

            # STOP TIMER
            end = time.time()
            timers.append(end - start)

            for i,c in enumerate(counts):
                print(">>> %s of %s tweets match the filter: %s." % (c, total, queries[i].where))

            time.sleep(0.1)

        return timers



if __name__ == "__main__":
    print('running')
    main()
