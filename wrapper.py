class Wrapper(object):

    # http://stackoverflow.com/a/2704528
    def __init__(self, wrapped, deferred=None):
        """
        Creates a wrapper around a given object that intercepts all
        attribute accesses and function calls to return more wrapper
        objects.

        Arguments:
        wrapped -- the object being wrapped
        deferred -- for internal use. A method call to apply to the
            object at eval-time: (name, args, kwargs). May only be set
            when the wrapped object is another Wrapper.
        """
        self._wrapped = wrapped
        self._deferred = deferred

    def __getattr__(self, name):
        attr = getattr(self._wrapped, name)
        if hasattr(attr, "__call__"):
            def fn(*args, **kwargs):
                deferred = (name, args, kwargs)
                return self.__class__(self, deferred)
            return fn
        else:
            print("WARNING: raw attribute access")
            return attr

    def __eval__(self):
        """
        Evaluate the wrapped object, converting it to a real object.
        """
        if not self._deferred:
            # no deferred action, just pass through the object
            return self._wrapped
        else:
            # evaluate all ancestors of this object
            parent = self._wrapped.__eval__()
            # then apply the deferred action
            name, args, kwargs = self._deferred
            return getattr(parent, name)(*args, **kwargs)

class CachingWrapper(Wrapper):

    def __init__(self, *args, **kwargs):
        super(CachingWrapper, self).__init__(*args, **kwargs)
        self._cached = None
        self._cache_present = False

    def __eval__(self):
        if not self._cache_present:
            self._cached = super(CachingWrapper, self).__eval__()
            self._cache_present = True
        return self._cached

class CommonSubqueryWrapper(CachingWrapper):

    def __init__(self, *args, **kwargs):
        super(CommonSubqueryWrapper, self).__init__(*args, **kwargs)
        self._call_cache = {}

    def __getattr__(self, name):
        attr = getattr(self._wrapped, name)
        if hasattr(attr, "__call__"):
            def fn(*args, **kwargs):
                hashkey = (name, args, frozenset(kwargs.items()))
                if hashkey not in self._call_cache:
                    deferred = (name, args, kwargs)
                    self._call_cache[hashkey] = self.__class__(self, deferred)
                return self._call_cache[hashkey]
            return fn
        else:
            print("WARNING: raw attribute access")
            return attr
