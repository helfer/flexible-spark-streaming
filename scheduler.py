import collections
import json
import os
import time

import pyspark

import dirwatcher
import queryparser
import wrapper

class FlexibleStreamingScheduler():

    def __init__(self, watch_dir):
        self.watch_dir = watch_dir
        self.inputs = collections.deque()  # is thread-safe

        self.dw = dirwatcher.DirWatcher(
            self.watch_dir, self.register_new_input_files)
        self.sc = pyspark.SparkContext(appName="FlexibleStreaming")

    def register_new_input_files(self, changes):
        if changes['added']:
            self.inputs.extend(changes['added'])

    def start(self):
        # START DIRECTORY WATCHER
        self.dw.start()

        # RUN LOOP
        while True:
            if self.inputs:
                filename = os.path.abspath(
                    os.path.join(self.watch_dir, self.inputs.popleft()))
                print("Detected new file: %s" % filename)
                queries = queryparser.get_active_queries()
                print 'queries:', queries

                lines = wrapper.AggregateWrapper(self.sc.textFile(filename))
                #     no minimum line param in case of empty file
                total = lines.count()

                # Loads all URLs from input file and initialize their neighbors.
                results = [q.apply(lines) for q in queries]

                total = total.__eval__()
                counts = [rdd.__eval__() for rdd in results]

                for i,c in enumerate(counts):
                    print(">>> %s of %s tweets match the filter: %s." % (c, total, queries[i].where))

                queryparser.write_results_to_mongodb( queries, counts )
            time.sleep(0.1)

    def stop(self):
        self.dw.stop()
        self.sc.stop()
