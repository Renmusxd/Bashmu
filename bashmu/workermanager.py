from bashmu.FormatSock import FormatSocket
from bashmu.serverconstants import *
from threading import Lock, Condition
import dill
import queue


class WorkerManager:

    def __init__(self):
        self.mutex = Lock()
        self.workers = []
        self.workerwait = Condition(self.mutex)

    def addWorker(self,w):
        with self.mutex:
            for i in range(w.threads):
                self.workers.append(w)
            self.workerwait.notify()

    def removeWorker(self,w):
        with self.mutex:
            while w in self.workers:
                self.workers.remove(w)

    def recruitWorker(self,w):
        """
        Use a thread from the worker
        :param w: worker to recruit
        """
        with self.mutex:
            if w in self.workers:
                self.workers.remove(w)

    def freeWorker(self,w):
        """
        Return a thread under worker control
        :param w: worker to free
        """
        with self.mutex:
            self.workers.append(w)
            self.workerwait.notify()

    def getBestWorker(self,reqfuncs=list(),reqresources=list()):
        """
        :param reqfuncs: functions required for execution [hash-int]
        :param reqresources: resources required for execution [str]
        """
        with self.mutex:
            while len(self.workers)==0:
                try:
                    self.workerwait.wait()
                except:
                    pass
            return self.workers[0]


class Worker:
    def __init__(self,sock,distserver):
        self.fsock = FormatSocket(sock)
        dillstr = self.fsock.recv()
        dillobj = dill.loads(dillstr)
        self.lock = Lock()
        self.cond = Condition(self.lock)
        self.threads = dillobj[THREADS_JSON]
        self.functioncache = set()
        self.deferobjs = {}
        self.jobid = 0
        self.distserver = distserver

    def dispatchJob(self,f,args,kwargs,deferobj,callback,ecallback,addjobargs):
        with self.lock:
            # If all threads actually taken up to client to deal with it
            bytecode = dill.dumps(f)
            keyid = hash(bytecode)
            if keyid not in self.functioncache:
                dilldict = {FUNCTION_JSON: bytecode,
                            FID_JSON: keyid,
                            ARGS_JSON: args,
                            KWARGS_JSON: kwargs,
                            JOBID_JSON: self.jobid}
                self.functioncache.add(keyid)
            else:
                dilldict = {FID_JSON: keyid,
                            ARGS_JSON: args,
                            KWARGS_JSON: kwargs,
                            JOBID_JSON: self.jobid}
            dillbytes = dill.dumps(dilldict)
            self.fsock.send(dillbytes)
            self.deferobjs[self.jobid] = (deferobj,callback,ecallback,addjobargs)
            self.jobid += 1
            self.threads -= 1
            return self.threads

    def recvandwork(self):
        msg = self.fsock.recv()
        dillobj = dill.loads(msg)

        jobid = dillobj[JOBID_JSON]
        if RESULT_JSON in dillobj:
            result = dillobj[RESULT_JSON]

            with self.lock:
                deferobj, callback, ecallback, addjobargs = self.deferobjs.pop(jobid)
                callback(result)
                deferobj.__setvalue__(result)
                self.threads += 1
                return self.threads
        elif ERROR_JSON in dillobj:
            error = dillobj[ERROR_JSON]
            with self.lock:
                deferobj, callback, ecallback, addjobargs = self.deferobjs.pop(jobid)
                self.threads += 1
                ecallback(error)
                return self.threads

    def hasdata(self):
        return self.fsock.hasdata()

    def getunfinishedjobs(self):
        jobstoadd = []
        with self.lock:
            for jobid in self.deferobjs.keys():
                deferobj, callback, ecallback, addjobargs = self.deferobjs[jobid]
                jobstoadd.append(addjobargs)
        return jobstoadd

    def close(self):
        self.fsock.close()

    def fileno(self):
        return self.fsock.fileno()


class CheckableQueue(queue.Queue):
    def _init(self, maxsize):
        self.queue = []

    def _put(self, item):
        self.queue.append(item)

    def _get(self):
        return self.queue.pop(0)

    def __len__(self):
        with self.mutex:
            return len(self.queue)

    def __contains__(self, item):
        with self.mutex:
            return item in self.queue

    def remove(self, item):
        with self.mutex:
            self.queue.remove(item)