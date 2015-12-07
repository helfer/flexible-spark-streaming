import collections
import json
import os
import thread
import threading
import time

from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener

import safethread

class TweetException(Exception):
    pass

class TweetListener(StreamListener):

    def __init__(self, writer):
        super(TweetListener, self).__init__()
        self.writer = writer

    def on_data(self, data):
        vals = json.loads(data)
        self.writer.write(vals)
        return True

    def on_error(self, status):
        if status == 420:
            print("WARNING: Tweepy received status code 420")
            return True  # continue processing

        print("")
        print("Tweepy received status code %i, exiting..." % status)
        print("")
        thread.interrupt_main()
        return False

class TweetDownloader(safethread.SafeThread, StreamListener):

    def __init__(self,
                 destpath,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_secret,
                 window = 10000,
                 verbose = False):
        super(TweetDownloader, self).__init__(name="TweetDownloader")
        self.destpath = destpath
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret
        self.prefix = 'tweets'
        self.suffix = 'txt'
        self.window = window
        self.buf = collections.deque()
        self.stopped = True

    # Write the tweet text to the current file. May throw an error if the file
    # is currently being switched out (i.e. writing at the end of a window).
    def write(self, vals):
        self.buf.appendleft(json.dumps(vals))

    def action(self):
        if len(self.buf) > 0:
            self.f.write(self.buf.pop() + '\n')

        if ((time.time() * 1000) - self.begin > self.window):
            self.f.close()
            fname = self.destpath + self.prefix + '-' + str(self.begin) + \
                    '.' + self.suffix
            os.rename(self.destpath + 'tmp', fname)

            self.begin = int(time.time() * 1000)
            self.f = open(self.destpath + 'tmp', 'w')

    def start(self):
        # Setup the stream
        auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_secret)
        self.stream = Stream(auth, TweetListener(self))

        # Create the first file
        self.begin = int(time.time() * 1000)
        self.f = open(self.destpath + 'tmp', 'w')

        # Start the threads
        self.stream.sample(async=True)
        super(TweetDownloader, self).start()

    def stop(self):
        self.stream.disconnect()
        super(TweetDownloader, self).stop()
