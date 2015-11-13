from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

class TestListener(StreamListener):
    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print "error: " + str(status)


ckey = "Xzv7k9I5skT4i7CyozsUtzCW3"
csec = "Uiju1uzYI0zpxhAtqi72HqiQQmgGk7Cc7y0B2edlKh8UFeuQ79"
atok = "4158432017-6DbZPRWVeQSMAyZEChPeNtHYh0chO0efq270hIf"
asec = "Xnpyyx6658Y98LZz2uKtt3qaxSZNBpIlSaBpFpTIsOZJ7"

auth = OAuthHandler(ckey, csec)
auth.set_access_token(atok, asec)

stream = Stream(auth, TestListener())
stream.filter(track=["car"])

