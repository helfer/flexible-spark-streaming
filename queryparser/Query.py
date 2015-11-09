class Query():
    # Super-simple query class, to be expanded

    def __init__(self, tags):
        self.tags = tags

    def filter(self, tweet):
      for t in self.tags:
        if t in tweet:
          return True
      return False
