import inspect

def make_hashkey(name, args, kwargs):
    hn = name
    ha = []
    for arg in args:
        if hasattr(arg, "__call__"):
            c = [x.cell_contents for x in arg.__closure__] if arg.__closure__ else ()
            key = (arg.__code__.co_code, tuple(c))
            ha.append(key)
        else:
            ha.append(arg)
    ha = tuple(ha)
    hk = frozenset(kwargs.items())
    return (hn, ha, hk)


class Wrapper(object):

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
        # http://stackoverflow.com/a/2704528
        if name == "__getnewargs__":
            # http://stackoverflow.com/a/6416080
            raise ValueError("You're trying to pickle a Wrapper. Don't do that!")

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

class ScanSharingWrapper(Wrapper):

    def __init__(self, *args, **kwargs):
        """
        Wraps an object and performs scan sharing on a limited set of queries,
        e.g. map. The wrapper records all deferred map actions called on it. At
        evaluation time, the first child performs a mega-map consisting of the
        union of all these map actions. Then each child runs their individual
        map on this result, which should be an efficiency gain.
        """
        super(ScanSharingWrapper, self).__init__(*args, **kwargs)

        # For each optimized action, a list of arguments to be computed, e.g.
        # self._tasks["map"] is a list of map actions to run.
        self._tasks = {
            "map": [],
            "filter": [],
            "aggregate": [],
        }

        # For each optimized action, the "megaresult" produced by running the
        # megaquery with the actions in the list above.
        self._results = {}

    def __getcall__(self, name):
        fn = super(ScanSharingWrapper, self).__getcall__(name)
        def ffn(*args, **kwargs):
            if name in self._tasks:
                if name == "aggregate":
                    if len(args) != 3:
                        raise ValueError("%s only takes three arguments" % name)
                    v = args
                else:
                    if len(args) != 1:
                        raise ValueError("%s only takes one argument" % name)
                    v = args[0]
                if len(kwargs) != 0:
                    raise ValueError("%s does not take keyword arguments" % name)
                self._tasks[name].append(v)
            return fn(*args, **kwargs)
        return ffn

    def __eval__(self):
        if not self._deferred:
            return self._wrapped

        name, args, kwargs = self._deferred
        parent = self._wrapped.__eval__()

        # Bind to a local variable to prevent Spark from trying to pickle self.
        tasks = self._wrapped._tasks.get(name)

        if len(self._wrapped._tasks.get(name, "")) <= 1:
            # disable optimizations
            pass
        elif name == "filter":
            megaresult = self.__getmegaresult__(
                name, parent, lambda item: any(task(item) for task in tasks))
            print "RUN ON MEGARESULT:", name, args, kwargs
            return megaresult.filter(*args, **kwargs)
        elif name == "map":
            megaresult = self.__getmegaresult__(
                name, parent, lambda item: [task(item) for task in tasks])
            index = self._wrapped._tasks[name].index(args[0])
            print "RUN ON MEGARESULT:", name, index
            return megaresult.map(lambda item: item[index])
        elif name == "aggregate":
            if name not in self._wrapped._results:
                zeroValues = [v[0] for v in tasks]
                def seqOp(a, b):
                    result = [None] * len(tasks)
                    for i in xrange(len(tasks)):
                        result[i] = tasks[i][1](a[i], b)
                    return result
                def combOp(a, b):
                    result = [None] * len(tasks)
                    for i in xrange(len(tasks)):
                        result[i] = tasks[i][2](a[i], b[i])
                    return result
                self._wrapped._results[name] = parent.aggregate(
                    zeroValues, seqOp, combOp)
            megaresult = self._wrapped._results[name]
            index = self._wrapped._tasks[name].index(v)
            print "RUN ON MEGARESULT (BYPASSES SPARK):", name, index
            return megaresult[index]

        print "RUN:", name, args, kwargs
        return getattr(parent, name)(*args, **kwargs)

    def __getmegaresult__(self, name, parent, megaquery):
        """
        Gets the cached megaresult from the parent. If it hasn't been computed
        yet, compute, cache and return it.
        """
        if name not in self._wrapped._results:
            print "CALCULATING MEGARESULT:", name, megaquery
            self._wrapped._results[name] = getattr(parent, name)(megaquery)
            self._wrapped._results[name].cache()
        return self._wrapped._results[name]

class CachingWrapper(ScanSharingWrapper):

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
            hashkey = make_hashkey(name, args, kwargs)
            if hashkey not in self._call_cache:
                self._call_cache[hashkey] = super(CommonSubqueryWrapper, self).__getcall__(name)(*args, **kwargs)
            return self._call_cache[hashkey]
        return fn

class AggregateWrapper(CommonSubqueryWrapper):

    def __getcall__(self, name):
        if name == "reduce":
            fn = super(AggregateWrapper, self).__getcall__("aggregate")
            def ffn(*args, **kwargs):
                if len(args) != 1:
                    raise ValueError("%s takes one argument" % name)
                if len(kwargs) != 0:
                    raise ValueError("%s does not take keyword arguments" % name)
                f = args[0]
                def combOp(a, b):
                    if a is None and b is None:
                        return None
                    if a is None:
                        return b
                    if b is None:
                        return a
                    return f(a, b)
                def seqOp(acc, val):
                    if acc is None:
                        return val
                    return f(acc, val)
                return fn(None, seqOp, combOp)
            return ffn
        elif name == "fold":
            fn = super(AggregateWrapper, self).__getcall__("aggregate")
            def ffn(*args, **kwargs):
                if len(args) != 2:
                    raise ValueError("%s takes two arguments" % name)
                if len(kwargs) != 0:
                    raise ValueError("%s does not take keyword arguments" % name)
                zeroValue, op = args
                return fn(zeroValue, op, op)
            return ffn
        else:
            return super(AggregateWrapper, self).__getcall__(name)
