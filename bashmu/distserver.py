from bashmu.dist import DistBase, Deferred
from bashmu.FormatSock import FormatSocket
import dill
import socket
from threading import Lock, Condition, Thread
import queue
import os
import select


class DistServer(DistBase):
    WORKER_TAG = b'\x00'
    SERVER_TAG = b'\x01'
    THREADS_JSON = 'threads'
    JOBID_JSON = 'jobid'
    RESULT_JSON = 'result'
    ERROR_JSON = 'error'
    FUNCTION_JSON = 'f'
    FID_JSON = 'fid'
    ARGS_JSON = 'args'
    KWARGS_JSON = 'kwargs'


    def __init__(self,addr=None,port=1708,start=True,):
        '''
        Connect to a Server or Host one
        :param addr: if addr is None then start a new server, else connect to an existing one
        :param port: port for server/connection
        '''
        super().__init__(start=False)
        self.allsockets = []
        self.hasidle = CheckableQueue()
        self.ss = None
        self.selectpipes = os.pipe()
        self.local = addr is None
        self.addr = '' if self.local else addr
        self.port = port
        self.numWorkerThreads = 0
        if start:
            self.start()

    def start(self):
        if super(DistServer, self).start():
            with self.lock:
                if self.local:
                    t = Thread(target=self.acceptloop)
                    t.start()
                else:
                    self.ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.ss.connect((self.addr,self.port))
                    self.ss.send(DistServer.SERVER_TAG)
                    sock_obj = Worker(self.ss,self)
                    self.allsockets.append(sock_obj)
                    self.hasidle.put(sock_obj)
                t = Thread(target=self.serveloop)
                t.start()

    def stop(self,wait=True):
        super(DistServer, self).stop(wait=wait)
        with self.lock:
            for worker in self.allsockets:
                worker.close()
            self.ss.close()

    def wait(self):
        with self.lock:
            if self.local:
                while self.running:
                    self.cond.wait()
        super(DistServer, self).wait()

    def acceptloop(self):
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ss.bind((self.addr, self.port))
        self.ss.listen(5)
        while self.running:
            try:
                sock, _ = self.ss.accept()
                switch = sock.recv(1)
                sock_obj = None

                if switch == DistServer.WORKER_TAG:
                    # If we should send jobs to the sock_obj
                    sock_obj = Worker(sock,self)
                    with self.lock:
                        self.numWorkerThreads += sock_obj.threads
                        self.hasidle.put(sock_obj)
                elif switch == DistServer.SERVER_TAG:
                    # If we will only receive jobs from sock_obj
                    sock_obj = Client(sock,self)

                # In any case listen to the socket
                with self.lock:
                    self.allsockets.append(sock_obj)

                os.write(self.selectpipes[1],b'\x01')
            except IOError as e:
                pass

    def serveloop(self):
        while self.running:
            with self.lock:
                waitingon = self.allsockets[:]
            try:
                rs,ws,xs = select.select([self.selectpipes[0]]+waitingon,[],[])
                for r in rs:
                    # If it's the pipe it just means we should wake up
                    if r != self.selectpipes[0]:
                        try:
                            threads = r.recvandwork()
                            while r.hasdata():
                                threads = r.recvandwork()
                            if threads > 0 and r not in self.hasidle:
                                self.hasidle.put(r)
                        except IOError:
                            with self.lock:
                                print("[IOError] Removing:",r)
                                try:
                                    print("\tAttempting to close... ",end='')
                                    r.close()
                                    print("done")
                                except IOError as e:
                                    print("\n\tError on close: ",e)
                                unfinishedjobs = r.getunfinishedjobs()
                                self.allsockets.remove(r)
                                if r in self.hasidle:
                                    self.hasidle.remove(r)
                            for jobargs in unfinishedjobs:
                                print("Re-adding ",jobargs)
                                self.addJob(*jobargs)
                                print("Done")
                    else:
                        os.read(self.selectpipes[0],1)

                for x in xs:
                    with self.lock:
                        print("[Exception] Removing:", x)
                        try:
                            print("\tAttempting to close... ",end='')
                            x.close()
                            print("\tdone")
                        except IOError as e:
                            print("\n\tError on close: ", e)
                        unfinishedjobs = x.getunfinishedjobs()
                        self.allsockets.remove(x)
                        if x in self.hasidle:
                            self.hasidle.remove(x)
                    for jobargs in unfinishedjobs:
                        print("Re-adding ", jobargs)
                        self.addJob(*jobargs)
                        print("Done")
            except OSError:
                pass

    def getNumWorkerThreads(self):
        with self.lock:
            print(self.numWorkerThreads)
            return self.numWorkerThreads

    def dispatch(self,f,args,kwargs,deferobj,callback,ecallback,addjobargs):
        # Send to worker
        nextworker = self.hasidle.get()
        threads = nextworker.dispatchJob(f,args,kwargs,deferobj,
                                         callback,ecallback,addjobargs)
        if threads > 0:
            self.hasidle.put(nextworker)


class Client:
    def __init__(self,sock,distserver):
        self.fsock = FormatSocket(sock)
        self.funccache = {}
        self.lock = Lock()
        self.distserver = distserver

        dillbytes = dill.dumps({DistServer.THREADS_JSON: distserver.getNumWorkerThreads()})
        self.fsock.send(dillbytes)

    def recvandwork(self):
        msg = self.fsock.recv()
        dillobj = dill.loads(msg)

        fid = dillobj[DistServer.FID_JSON]
        jobid = dillobj[DistServer.JOBID_JSON]

        args = dillobj[DistServer.ARGS_JSON]
        kwargs = dillobj[DistServer.KWARGS_JSON]
        with self.lock:
            if DistServer.FUNCTION_JSON in dillobj:
                self.funccache[fid] = dill.loads(dillobj[DistServer.FUNCTION_JSON])
            f = self.funccache[fid]

        def makecallbacks(jid):
            def customcallback(result):
                cdillbytes = dill.dumps({DistServer.JOBID_JSON: jid,
                                         DistServer.RESULT_JSON: result})
                self.fsock.send(cdillbytes)
            def errorcallback(error):
                cdillbytes = dill.dumps({DistServer.JOBID_JSON: jid,
                                         DistServer.ERROR_JSON: error})
                self.fsock.send(cdillbytes)
            return customcallback, errorcallback
        self.distserver.addJob(f,args,kwargs,Deferred(f.__name__,args,kwargs),*makecallbacks(jobid))
        # Return 0 to indicate cannot do work
        return 0

    def hasdata(self):
        return self.fsock.hasdata()

    def getunfinishedjobs(self):
        return []

    def close(self):
        self.fsock.close()

    def fileno(self):
        return self.fsock.fileno()


class Worker:
    def __init__(self,sock,distserver):
        self.fsock = FormatSocket(sock)
        dillstr = self.fsock.recv()
        dillobj = dill.loads(dillstr)
        self.lock = Lock()
        self.cond = Condition(self.lock)
        self.threads = dillobj[DistServer.THREADS_JSON]
        self.functioncache = set()
        self.deferobjs = {}
        self.jobid = 0
        self.distserver = distserver

    def dispatchJob(self,f,args,kwargs,deferobj,callback,ecallback,addjobargs):
        with self.lock:
            # If all threads actually taken up to client to deal with it
            if id(f) not in self.functioncache:
                dilldict = {DistServer.FUNCTION_JSON: dill.dumps(f),
                            DistServer.FID_JSON: id(f),
                            DistServer.ARGS_JSON: args,
                            DistServer.KWARGS_JSON: kwargs,
                            DistServer.JOBID_JSON: self.jobid}
                self.functioncache.add(id(f))
            else:
                dilldict = {DistServer.FID_JSON: id(f),
                            DistServer.ARGS_JSON: args,
                            DistServer.KWARGS_JSON: kwargs,
                            DistServer.JOBID_JSON: self.jobid}
            dillbytes = dill.dumps(dilldict)
            self.fsock.send(dillbytes)
            self.deferobjs[self.jobid] = (deferobj,callback,ecallback,addjobargs)
            self.jobid += 1
            self.threads -= 1
            return self.threads

    def recvandwork(self):
        msg = self.fsock.recv()
        dillobj = dill.loads(msg)

        jobid = dillobj[DistServer.JOBID_JSON]
        if DistServer.RESULT_JSON in dillobj:
            result = dillobj[DistServer.RESULT_JSON]

            with self.lock:
                deferobj, callback, ecallback, addjobargs = self.deferobjs.pop(jobid)
                callback(result)
                deferobj.__setvalue__(result)
                self.threads += 1
                return self.threads
        elif DistServer.ERROR_JSON in dillobj:
            error = dillobj[DistServer.ERROR_JSON]
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
                deferobj, callback, addjobargs = self.deferobjs[jobid]
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