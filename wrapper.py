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
        self.__wrapped = wrapped
        self.__deferred = deferred

    def __getattr__(self, name):
        attr = getattr(self.__wrapped, name)
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
        if not self.__deferred:
            # no deferred action, just pass through the object
            return self.__wrapped
        else:
            # evaluate all ancestors of this object
            parent = self.__wrapped.__eval__()
            # then apply the deferred action
            name, args, kwargs = self.__deferred
            return getattr(parent, name)(*args, **kwargs)

class CachingWrapper(Wrapper):

    def __init__(self, *args, **kwargs):
        super(CachingWrapper, self).__init__(*args, **kwargs)
        self.__cached = None
        self.__cache_present = False

    def __eval__(self):
        if not self.__cache_present:
            self.__cached = super(CachingWrapper, self).__eval__()
            self.__cache_present = True
        return self.__cached
