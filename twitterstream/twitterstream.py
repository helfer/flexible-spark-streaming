import os, time, json

from collections import deque

from threading import Thread
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener

class TweetListener(StreamListener):

    def __init__(self, writer):
        self.writer = writer

        # Python 2.x.x style
        super(StreamListener, self).__init__()


    def on_data(self, data):
        vals = json.loads(data)
        '''
        if 'text' in vals:
            self.writer.write(vals['text'])
        elif self.writer.verbose:
            print 'No text key in data'
        '''
        self.writer.write(vals)
        return True 


    def on_error(self, status):
        print('error: ' + status)


class TwitterStream(StreamListener):

    def __init__(self,
                 destpath,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_secret,
                 prefix = 'tweets',
                 suffix = 'txt',
                 window = 10000,
                 verbose = False):
        self.destpath = destpath
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret
        self.prefix = prefix
        self.suffix = suffix
        self.window = window
        self.verbose = verbose
        self.buf = deque()
        self.stopped = True


    # Write the tweet text to the current file. May throw an error if the file
    # is currently being switched out (i.e. writing at the end of a window).
    def write(self, vals):
        self.buf.appendleft(json.dumps(vals))


    def run(self):
        while not self.stopped:
            if len(self.buf) > 0:
                try:
                    self.f.write(self.buf.pop() + '\n')
                except UnicodeEncodeError:
                    if self.verbose:
                        print 'Cannot encode non-unicode characters'

            if ((time.time() * 1000) - self.begin > self.window):
                self.f.close()
                self.begin = int(time.time() * 1000)
                self.f = open(
                             self.destpath +
                             self.prefix + '-' + str(self.begin) +
                             '.' + self.suffix, 'w')

    def start(self):
        self.stopped = False

        # Setup the stream
        auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_secret)
        self.stream = Stream(auth, TweetListener(self))

        # Create the first file
        self.begin = int(time.time() * 1000)
        self.f = open(
                     self.destpath +
                     self.prefix + '-' + str(self.begin) +
                     '.' + self.suffix, 'w')

        # Start the threads
        self.stream.sample(async=True)
        thread = Thread(target=self.run, args=())
        thread.start()


    def stop(self):
        self.stream.disconnect()
        self.stopped = True

