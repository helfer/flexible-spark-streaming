import os, time, json
from threading import Thread
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener

class TweetListener(StreamListener):

    def __init__(self, writer, api=None):
        self.writer = writer
        
        # Python 2.x.x style
        super(StreamListener, self).__init__(api)


    def on_data(self, data):
        self.writer.write(data)
        return True


    def on_error(self, status):
        print status

class TwitterStream(StreamListener):

    def __init__(self,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_secret,
                 destpath,
                 prefix = 'tweets',
                 suffix = 'txt',
                 window = 10):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret
        self.destpath = destpath
        self.prefix = prefix
        self.window = window
        self.stopped = True


    def write(data):
        try:
            text = json.loads(data)['text']
            self.file.write(text + '\n')
        except ValueError:
            print 'Error writing file.'


    def run(self):
        while not self.stopped:
            if ((time.time() * 1000) - self.begin > self.window):
                self.file.close()
                self.begin = int(time.time() * 1000)
                self.file = open(self.destpath + '/' +
                                     self.prefix + '-' + self.begin +
                                     '.' + self.suffix, 'w')

    def start(self):
        self.stopped = False

        # Setup the stream
        auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_secret)
        stream = Stream(auth, TweetListener(self))

        # Create the first file
        self.begin = int(time.time() * 1000)
        self.file = open(self.destpath + '/' +
                             self.prefix + '-' + self.begin +
                             '.' + self.suffix, 'w')

        # Start the threads
        stream.firehose(async=True)
        thread = Thread(target=self.run, args=())
        thread.start()


    def stop(self):
        self.stopped = True

