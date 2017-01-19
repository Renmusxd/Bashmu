from scylla.Deferred import Deferred
from threading import RLock, Condition
from threading import Thread
from functools import wraps

class DistBase:
    def __init__(self, start=True):
        self.lock = RLock()
        self.cond = Condition(self.lock)
        self.frontier = []
        self.jobqueue = set()
        self.running = False
        self.runcondition = {}
        if start:
            self.start()

    def queueLoop(self):
        with self.lock:
            shouldrun = self.frontier or self.running
        while shouldrun:
            f, args, kwargs, deferobj, callback = None, None, None, None, None
            with self.lock:
                while len(self.frontier) == 0 and self.running:
                    self.cond.wait()
                if len(self.frontier) > 0:
                    f, args, kwargs, deferobj, callback = self.frontier.pop()
            if f is not None:
                newargs = [item.__waitforvalue__() if type(item)==Deferred else item for item in args]
                newkwargs = {}
                for k in kwargs.keys():
                    v = kwargs[k]
                    if type(k)==Deferred:
                        k = k.__waitforvalue__()
                    if type(v)==Deferred:
                        v = v.__waitforvalue__()
                    newkwargs[k] = v
                def makecallback(foo):
                    if foo is None:
                        return self.jobDone
                    else:
                        def newcallback(dobj):
                            self.jobDone(dobj)
                            if foo is not None:
                                foo(dobj.__waitforvalue__())
                        return newcallback
                self.dispatch(f,newargs,newkwargs,deferobj,makecallback(callback),(f,args,kwargs,deferobj,callback))
            with self.lock:
                shouldrun = self.frontier or self.running

    def jobDone(self,deferobj):
        with self.lock:
            for nextitem in deferobj.__getprovidingfor__():
                waittot = nextitem.__removewaitingon__(deferobj)
                if waittot == 0:
                    self.frontier.append(self.runcondition.pop(nextitem._sensitive))
            self.cond.notifyAll()
            self.jobqueue.remove(deferobj._sensitive)

    def dispatch(self,f,args,kwargs,deferobj,callback,addjobargs):
        '''
        Dispatch a job to be run
        :param f: function to call
        :param args: args for function
        :param kwargs: kwargs for function
        :param deferobj: Deferred object for job
        :param callback: function to be called on completion, takes deferobj
        '''
        deferobj.__setvalue__(f(*args,**kwargs))
        callback(deferobj)

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        '''
        Starts the Distribution, returns true if started, false if
        already started
        :return: now started
        '''
        with self.lock:
            if not self.running:
                self.running = True
                t = Thread(target=self.queueLoop)
                t.start()
                return True
            return False

    def wait(self):
        with self.lock:
            while len(self.jobqueue)>0:
                self.cond.wait()

    def stop(self,wait=True):
        if wait:
            self.wait()
        with self.lock:
            self.running = False
            self.cond.notifyAll()

    def addJob(self, f, args, kwargs, deferobj, callback=None):
        with self.lock:
            totalwaiting = 0
            if type(f)==Deferred:
                f.__addprovidingfor__(deferobj)
                totalwaiting += deferobj.__addwaitingon__(f)
            for item in args:
                if type(item)==Deferred:
                    item.__addprovidingfor__(deferobj)
                    totalwaiting += deferobj.__addwaitingon__(item)
            for key in kwargs.keys():
                if type(key)==Deferred:
                    key.__addprovidingfor__(deferobj)
                    totalwaiting += deferobj.__addwaitingon__(key)
                if type(kwargs[key])==Deferred:
                    kwargs[key].__addprovidingfor__(deferobj)
                    totalwaiting += deferobj.__addwaitingon__(kwargs[key])
            if totalwaiting == 0:
                self.frontier.append((f, args, kwargs, deferobj, callback))
                self.cond.notifyAll()
            else:
                self.runcondition[deferobj._sensitive] = (f,args,kwargs,deferobj,callback)

            self.jobqueue.add(deferobj._sensitive)

    def defer(self, f):
        '''
        Returns a deferred object which will operate like the original return value
        '''
        @wraps(f)
        def wrapper(*args, **kwargs):
            deferobj = Deferred(repr(f),repr(args),repr(kwargs))
            self.addJob(f,args,kwargs,deferobj)
            return deferobj
        return wrapper

    def deferargs(self, callback=None):
        def defer(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                deferobj = Deferred(repr(f), repr(args), repr(kwargs))
                self.addJob(f, args, kwargs, deferobj, callback)
                return deferobj
            return wrapper
        return defer