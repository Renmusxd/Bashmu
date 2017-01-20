from threading import RLock, Condition


class Sensitive:
    def __init__(self,fname,args,kwargs,deferred):
        self.hashstring = "{}:{}:{}:{}".format(id(deferred),fname,args,kwargs)
        self.deferred = deferred

    def __hash__(self):
        return hash(self.hashstring)

    def __eq__(self, other):
        return id(self.deferred)==id(other.deferred)

class Deferred:
    def __init__(self,fname,args,kwargs):
        self._lock = RLock()
        self._cond = Condition(self._lock)
        self._value = None
        self._error = None
        self._torun = (fname,args,kwargs)
        self._notifyobservers = {self._cond}
        self._waitingon = set()
        self._providingfor = set()
        self._sensitive = Sensitive(fname,args,kwargs,self)

        # TODO test!
        def new__setattr__(self, key, value):
            return self.__waitforvalue__().__setattr__(key, value)
        self.__setattr__ = new__setattr__

    def __hasvalue__(self):
        with self._lock:
            return self._value is not None

    def __setvalue__(self, val):
        with self._lock:
            if self._value is None:
                self._value = val
                for cond in self._notifyobservers:
                    cond.notifyAll()
            else:
                raise Exception("Value already set")

    def __addnotifyobserver__(self, cond):
        with self._lock:
            if self._value is not None:
                cond.notifyAll()
            self._notifyobservers.add(cond)

    def __addprovidingfor__(self,deferobj):
        with self._lock:
            self._providingfor.add(deferobj._sensitive)

    def __getprovidingfor__(self):
        with self._lock:
            return [s.deferred for s in self._providingfor]

    def __addwaitingon__(self,deferobj):
        with self._lock:
            if not deferobj.__hasvalue__():
                self._waitingon.add(deferobj._sensitive)
            return len(self._waitingon)

    def __removewaitingon__(self,deferobj):
        with self._lock:
            self._waitingon.remove(deferobj._sensitive)
            return len(self._waitingon)

    def __waitforvalue__(self):
        with self._lock:
            while self._value is None and self._error is None:
                self._cond.wait()
            if self._error:
                raise DeferredException(*self._torun,self._error)
            else:
                return self._value

    def __seterrorvalue__(self, err):
        with self._lock:
            self._error = err
            self._cond.notify()

    def __repr__(self):
        with self._lock:
            if self._value is None:
                return "(Deferred: [{},{},{}])".format(self._torun[0],self._torun[1],self._torun[2])
            else:
                return "(Deferred: {})".format(self._value)

    def __getattr__(self, item):
        return self.__waitforvalue__().__getattribute__(item)

    # Now forward through self.__waitforvalue__()

    # 3.3.1 Basic customization
    def __str__(self):
        return str(self.__waitforvalue__())
    def __bytes__(self):
        return bytes(self.__waitforvalue__())
    def __format__(self, format_spec):
        return self.__waitforvalue__().__format__(format_spec)
    def __lt__(self, other):
        return self.__waitforvalue__() < other
    def __le__(self, other):
        return self.__waitforvalue__() <= other
    def __eq__(self, other):
        return self.__waitforvalue__() == other
    def __ne__(self, other):
        return self.__waitforvalue__() != other
    def __gt__(self, other):
        return self.__waitforvalue__() > other
    def __ge__(self, other):
        return self.__waitforvalue__() >= other
    def __hash__(self):
        return hash(self.__waitforvalue__())
    def __bool__(self):
        return bool(self.__waitforvalue__())

    # Bonus?
    def __unicode__(self):
        return self.__waitforvalue__().__unicode__()
    def __cmp__(self, other):
        return self.__waitforvalue__().__cmp__(other)

    # 3.3.2 Customizing attribute access
    # def __getattr__
    # def __getattribute__
    # def __setattr__
    # def __delattr__
    def __dir__(self):
        return dir(self.__waitforvalue__())
    # 3.3.2.1 Implementing Descriptors
    # 3.3.2.2 Invoking Descriptors
    # 3.3.2.3. __slots__
    # 3.3.3. Customizing class creation
    # 3.3.3.1. Metaclasses ...
    # 3.3.4 Customizing instance and subclass checks
    def __instancecheck__(self, instance):
        return self.__waitforvalue__().__instancecheck__(instance)
    def __subclasscheck__(self,subclass):
        return self.__waitforvalue__().__subclasscheck__(subclass)

    # 3.3.5. Emulating callable objects
    def __call__(self, *args, **kwargs):
        return (self.__waitforvalue__())()

    # 3.3.6 Emulating container types
    def __len__(self):
        return len(self.__waitforvalue__())
    def __length_hint__(self):
        return self.__waitforvalue__().__length_hint__()
    def __getitem__(self, item):
        return self.__waitforvalue__()[item]
    def __missing__(self, key):
        return self.__waitforvalue__().__missing__(key)
    def __setitem__(self, key, value):
        return self.__waitforvalue__().__setitem__(key,value)
    def __delitem__(self, key):
        del self.__waitforvalue__()[key]
    def __iter__(self):
        return iter(self.__waitforvalue__())
    def __reversed__(self):
        return reversed(self.__waitforvalue__())
    def __contains__(self, item):
        return item in self.__waitforvalue__()

    # 3.3.7. Emulating numeric types
    def __add__(self, other):
        return self.__waitforvalue__() + other
    def __radd__(self, other):
        return other + self.__waitforvalue__()
    def __sub__(self, other):
        return self.__waitforvalue__() - other
    def __rsub__(self, other):
        return other - self.__waitforvalue__()
    def __mul__(self, other):
        return self.__waitforvalue__() * other
    def __rmul__(self, other):
        return other * self.__waitforvalue__()
    def __truediv__(self, other):
        return self.__waitforvalue__() / other
    def __rtruediv__(self, other):
        return other / self.__waitforvalue__()
    def __floordiv__(self, other):
        return self.__waitforvalue__() / other
    def __rfloordiv__(self, other):
        return other / self.__waitforvalue__()
    def __mod__(self, other):
        return self.__waitforvalue__() % other
    def __rmod__(self, other):
        return other % self.__waitforvalue__()
    def __divmod__(self, other):
        return divmod(self.__waitforvalue__(),other)
    def __rdivmod__(self, other):
        return divmod(other, self.__waitforvalue__())
    def __pow__(self, power, modulo=None):
        return pow(self.__waitforvalue__(),power,modulo)
    def __rpow__(self, other):
        return pow(other,self.__waitforvalue__())
    def __rshift__(self, other):
        return self.__waitforvalue__() << other
    def __rrshift__(self, other):
        return other << self.__waitforvalue__()
    def __lshift__(self, other):
        return self.__waitforvalue__() >> other
    def __rlshift__(self, other):
        return other >> self.__waitforvalue__()
    def __and__(self, other):
        return self.__waitforvalue__() and other
    def __rand__(self, other):
        return other and self.__waitforvalue__()
    def __or__(self, other):
        return self.__waitforvalue__() or other
    def __ror__(self, other):
        return other or self.__waitforvalue__()
    def __xor__(self, other):
        return self.__waitforvalue__() ^ other
    def __rxor__(self, other):
        return other ^ self.__waitforvalue__()

    def __iadd__(self, other):
        return self.__waitforvalue__().__iadd__(other)
    def __isub__(self, other):
        return self.__waitforvalue__().__isub__(other)
    def __imul__(self, other):
        return self.__waitforvalue__().__imul__(other)
    def __itruediv__(self, other):
        return self.__waitforvalue__().__itruediv__(other)
    def __ifloordiv__(self, other):
        return self.__waitforvalue__().__ifloordiv__(other)
    def __imod__(self, other):
        return self.__waitforvalue__().__imod__(other)
    def __ipow__(self, other):
        return self.__waitforvalue__().__ipow__(other)
    def __ilshift__(self, other):
        return self.__waitforvalue__().__ilshift__(other)
    def __irshift__(self, other):
        return self.__waitforvalue__().__irshift__(other)
    def __iand__(self, other):
        return self.__waitforvalue__().__iand__(other)
    def __ixor__(self, other):
        return self.__waitforvalue__().__ixor__(other)
    def __ior__(self, other):
        return self.__waitforvalue__().__ior__(other)

    def __neg__(self):
        return -self.__waitforvalue__()
    def __pos__(self):
        return +self.__waitforvalue__()
    def __abs__(self):
        return abs(self.__waitforvalue__())
    def __invert__(self):
        return ~self.__waitforvalue__()
    def __complex__(self):
        return complex(self.__waitforvalue__())
    def __int__(self):
        return int(self.__waitforvalue__())
    def __float__(self):
        return float(self.__waitforvalue__())
    def __round__(self, n=None):
        return round(self.__waitforvalue__(),n)
    def __index__(self):
        return self.__waitforvalue__().__index__()

    # 3.3.8 With Statement Context Managers
    def __enter__(self):
        return self.__waitforvalue__().__enter__()
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.__waitforvalue__().__exit__(exc_type,exc_val,exc_tb)

    # 3.4.1. Awaitable Objects
    def __await__(self):
        return self.__waitforvalue__().__await__()
    # 3.4.2. Coroutine Objects
    # 3.4.3. Asynchronous Iterators
    def __aiter__(self):
        return self.__waitforvalue__().__aiter__()
    def __anext__(self):
        return self.__waitforvalue__().__anext__()

    # 3.4.4. Asynchronous Context Managers
    def __aenter__(self):
        return self.__waitforvalue__().__aenter__()
    def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__waitforvalue__().__aexit__(exc_type,exc_val,exc_tb)

    # Misc

    def __trunc__(self):
        return self.__waitforvalue__().__trunc__()

    def __ceil__(self):
        return self.__waitforvalue__().__ceil__()

    def __coerce__(self, other):
        return self.__waitforvalue__().__coerce__(other)

    def __get__(self, instance, owner):
        return self.__waitforvalue__().__get__(instance,owner)
    def __set__(self, instance, value):
        return self.__waitforvalue__().__set__(instance,value)

    def __set_name__(self, owner, name):
        return self.__waitforvalue__().__set_name__(owner,name)
    def __setslice__(self, i, j, sequence):
        return self.__waitforvalue__().__setslice__(i,j,sequence)
    def __setstate__(self, state):
        return self.__waitforvalue__().__setstate__(state)
    def __sizeof__(self):
        return self.__waitforvalue__().__sizeof__()

class DeferredException(Exception):
    def __init__(self, f, args, kwargs, error):
        super(DeferredException, self).__init__(DeferredException.make_error_message(f,args,kwargs,error))

    @staticmethod
    def make_error_message(f,args,kwargs,error):
        errortype = str(type(error).__name__)
        fname = str(f)
        argsstring = 'args='+args +', kwargs='+ kwargs
        return "[{errortype}] {fname}({argstring}): {errormessage}".format(errortype=errortype,fname=fname,argstring=argsstring,errormessage=str(error))

def value(deferred):
    if type(deferred)==Deferred:
        return deferred.__waitforvalue__()
    elif hasattr(deferred,'__iter__'):
        for defitem in deferred:
            if type(defitem)==Deferred:
                defitem.__waitforvalue__()
        return deferred