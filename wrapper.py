class Wrapper(object):

    # http://stackoverflow.com/a/2704528
    def __init__(self, wrapped, deferred=None):
        """
        Wraps an object for the purpose of lazy evaluation and optimization. To
        create a wrapper:

            x = Wrapper(real_x)
            y = x.map(foo, bar)

        Here, y is another Wrapper object that causes evaluation of real_x.map
        to be deferred. To force evaluation:

            real_y = y.__eval__()

        Which results in real_y = real_x.map(foo, bar).

        It's a little unclear what to do about attribute accesses like x.baz,
        since they're not really used in Spark. If this ever comes up, Wrapper
        will print a warning so we can figure out what the right thing to do is.

        ---

        The deferred argument (internal use only!) represents the deferred
        method call used to transform one wrapper into the next. For instance,
        the above y would be created as follows:

            y = Wrapper(x, deferred=('map', [foo, bar], {}))

        deferred is a tuple of (name, args, kwargs). Only Wrapper objects that
        were created by calling a function on another Wrapper object have
        deferred set. User-created Wrappers do not.

        Here, x is the parent Wrapper and y is its child. By recursively
        following ._wrapper, we can trace the lineage of y to x to real_x (its
        ancestors).
        """
        self._wrapped = wrapped
        self._deferred = deferred

    def __getattr__(self, name):
        """
        Intercepts accesses of nonexistent attributes.

        Accessing real attributes skips this method:

            self._wrapped
            self._deferred
            self.__eval__()

        But we can use this to synthesize "fake" attributes:

            self.map()
            ...

        """
        # This logic is probably buggy
        attr = getattr(self._wrapped, name)
        if hasattr(attr, "__call__"):
            return self.__getcall__(name)
        else:
            print("WARNING: raw attribute access")
            return attr

    def __getcall__(self, name):
        """
        Helper function for constructing deferred functions for __getattr__.
        When __getattr__('map') is called, it must return a *function* that
        returns the child Wrapper.

        Note: we use self.__class__ to ensure that this still works when Wrapper
        is subclassed. Children will have the same class as their parent, so
        CachingWrappers will produce more CachingWrappers, etc.
        """
        def fn(*args, **kwargs):
            deferred = (name, args, kwargs)
            return self.__class__(self, deferred)
        return fn

    def __eval__(self):
        """
        Recursively force evaluation of this wrapper and its ancestors,
        returning the real object with all method calls applied to it.
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
        """
        Wraps an object, and caches the result of __eval__() so that the
        computation isn't duplicated. It's possible that this optimization is
        something that Spark already gives us, though.
        """
        super(CachingWrapper, self).__init__(*args, **kwargs)

        # The cached result of __eval__()
        self._cached = None

        # False until the result of __eval__() has been computed and cached,
        # then True.
        self._cache_present = False

    def __eval__(self):
        if not self._cache_present:
            self._cached = super(CachingWrapper, self).__eval__()
            self._cache_present = True
        return self._cached

class CommonSubqueryWrapper(CachingWrapper):

    def __init__(self, *args, **kwargs):
        """
        Wraps an object and collapses the lineage of duplicate computations by
        making wrappers singletons with respect to the deferred actions.

            x = CommonSubqueryWrapper(real_x)
            y = x.filter(f)
            z = x.filter(f)

        Here, y and z will refer to the same Wrapper object. In conjunction with
        __eval__() caching, above, this will eliminate unnecessary recomputation
        of the filter.

        To determine whether or not two actions are identical, we use Python's
        built-in hashing. Arguments must be hashable types. Lambdas and
        functions are hashable, but note that changing variable names can change
        the hash.
        """
        super(CommonSubqueryWrapper, self).__init__(*args, **kwargs)

        # A dictionary of hashkey => child, where hashkey is a hashable version
        # of deferred (see below) and child is the resulting
        # CommonSubqueryWrapper.
        self._call_cache = {}

    def __getcall__(self, name):
        def fn(*args, **kwargs):
            # Like deferred, hashkey represents a method call performed on the
            # parent object. Unlike deferred, hashkey is hashable.
            deferred = (name, args, kwargs)
            hashkey = (name, args, frozenset(kwargs.items()))
            if hashkey not in self._call_cache:
                self._call_cache[hashkey] = self.__class__(self, deferred)
            return self._call_cache[hashkey]
        return fn
