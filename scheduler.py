import collections
import os
import time

import pyspark

import dirwatcher
import queryparser
import wrapper

class FlexibleStreamingScheduler():

    def __init__(self, watch_dir, test_queries=None):
        self.watch_dir = watch_dir
        self.inputs = collections.deque()  # is thread-safe
        self.test_queries = test_queries

        self.dw = dirwatcher.DirWatcher(
            self.watch_dir, self.register_new_input_files)
        self.sc = pyspark.SparkContext(appName="FlexibleStreaming")

    def register_new_input_files(self, changes):
        if changes['added']:
            self.inputs.extend(changes['added'])

    def start(self):
        # START DIRECTORY WATCHER
        self.dw.start()

        #queryparser.init()

        # INITIALIZE
        timers = []

        def parse_input(i):
            return json.loads(i) if len(i) > 0 else {}

        # RUN LOOP
        for i in range(0,5):
            queries = queryparser.get_active_queries()
            print 'queries:', queries

            # START TIMER
            start = time.time()

            # reading the entire watch-dir
            #lines = wrapper.AggregateWrapper(self.sc.textFile(os.path.join(self.watch_dir, self.inputs[0] )))
            lines = wrapper.CommonSubqueryWrapper(self.sc.textFile(os.path.join(self.watch_dir )))
            #     no minimum line param in case of empty file
            total = lines.count()

            results = [q.apply(lines) for q in queries]

            total = total.__eval__()
            counts = [rdd.__eval__() for rdd in results]
            # Loads all URLs from input file and initialize their neighbors.

            # STOP TIMER
            end = time.time()
            timers.append(end - start)

            for i,c in enumerate(counts):
                print(">>> %s of %s tweets match the filter: %s." % (c, total, queries[i].where))


            queryparser.write_results_to_mongodb( queries, counts )
            time.sleep(0.1)

        print( '>>>>>>>' )
        print( timers )
        print( sum(timers), len(timers), sum(timers)/len(timers) )

    def stop(self):
        self.dw.stop()
        self.sc.stop()
