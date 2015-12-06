class Query():
    # Super-simple query class, to be expanded

    def __init__(self, tags):
        self.tags = tags

    # right now matches tweet if ANY of the tags is in the tweet, not all
    def filter(self, tweet):
      for t in self.tags:
        if 'text' in tweet and t in tweet['text']:
          return True
      return False

# Here's what a query could look like:
#
# SELECT count(id), user.name
# FROM tweets[<starttime>:<endtime>]
# WHERE "#test" IN hashtags AND location LIKE "%USA%"
# GROUP BY user.name
