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

        # INITIALIZE
        q1 = queryparser.Query(['and'])
        q2 = queryparser.Query(['but'])

        def parse_input(i):
            return json.loads(i) if len(i) > 0 else {}

        # RUN LOOP
        while True:
            if self.inputs:
                filename = os.path.abspath(
                    os.path.join(self.watch_dir, self.inputs.popleft()))
                print("Detected new file: %s" % filename)

                lines = wrapper.Wrapper(self.sc.textFile(filename))
                #     no minimum line param in case of empty file
                total = lines.count()

                # Loads all URLs from input file and initialize their neighbors.
                tweets = lines.map(parse_input)
                count1 = tweets.filter(q1.filter).count()
                count2 = tweets.filter(q2.filter).count()

                total = total.__eval__()
                count1 = count1.__eval__()
                count2 = count2.__eval__()

                print("%s of %s tweets match the filter: %s." % (count1, total, ['and']))
                print("%s of %s tweets match the filter: %s." % (count2, total, ['but']))

            time.sleep(2)

    def stop(self):
        self.dw.stop()
        self.sc.stop()
