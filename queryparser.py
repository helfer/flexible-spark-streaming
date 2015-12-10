from pymongo import MongoClient
import time

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

class SimpleQuery():

    def __init__(self, _id, select, where):
      self._id = _id
      self.select = select
      self.where = where


    # this is very ugly, there's a much nicer way. fix it
    def filter(self, tweet):
        if "_and" in self.where:
            pass #for now. we'll make this work recursively later
        elif "_or" in self.where:
            pass #for now, we'll make this work recursively later
        else:
            field = self.where.keys()[0]

            field_filter = self.where[field]
            modifier = field_filter.keys()[0]
            value = field_filter[modifier]

            if not field in tweet:
              return False
            if modifier == '_contains':
              return value in tweet[field]
            elif modifier == '_eq':
              return value == tweet[field]
            elif modifier == '_neq':
              return value != tweet[field]
            else:
              raise Exception("Unsupported modifier in filter: {}".format( modifier ))

    def aggregate(self):
        field = self.select['field']
        agg = self.select['agg']
        if agg == 'count':
          pass # don't do anything. call 'count'
        elif agg == 'max':
          pass # do a reduce a,b -> max(a,b)
        elif agg == 'min':
          pass # do a reduce
        elif agg == 'sum':
          pass # do a reduce. if we ever do group by, we'll have to do reduce by key
        elif agg == 'avg':
          pass # combineByKey, then calculate average from that.
        else:
            raise Exception("Unsupported aggregator in select: {}".format( agg ))

# Here's what a query could look like:
#
# SELECT count(id), user.name
# FROM tweets[<starttime>:<endtime>]
# WHERE "#test" IN hashtags AND location LIKE "%USA%"
# GROUP BY user.name


def get_active_queries():
    # not very efficient to connect every time, but eh...
    mc = MongoClient('localhost',3001)
    db = mc.meteor

    return [ SimpleQuery( q['_id'], q['select'], q['where'] ) for q in db.queries.find({}) ]


def write_results_to_mongodb( queries, values ):
  mc = MongoClient('localhost',3001)
  db = mc.meteor

  t = time.time()

  for i,q in enumerate(queries):
    db.results.insert( { 'query_id': q._id, 'time': t, 'values': [ values[i] ] } )
    print('>>> value inserted into mongodb for {}: {}'.format(q._id, values[i]))




