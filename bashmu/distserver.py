try:
    from bashmu.dist import DistBase, Deferred
    from bashmu.FormatSock import FormatSocket
    from bashmu.workermanager import WorkerManager, Worker
    from bashmu.serverconstants import *
except ImportError:
    from .dist import DistBase, Deferred
    from .FormatSock import FormatSocket
    from .workermanager import WorkerManager, Worker
    from .serverconstants import *


import dill
import socket
from threading import Lock, Thread


import os
import select


class DistServer(DistBase):
    WORKER_TAG = b'\x00'
    SERVER_TAG = b'\x01'

    def __init__(self,addr=None,port=1708,start=True,):
        '''
        Connect to a Server or Host one
        :param addr: if addr is None then start a new server, else connect to an existing one
        :param port: port for server/connection
        '''
        super().__init__(start=False)
        self.allsockets = []
        self.workers = WorkerManager()
        self.ss = None
        self.selectpipes = os.pipe()
        self.local = addr is None
        self.addr = '' if self.local else addr
        self.port = port
        self.numWorkerThreads = 0
        if start:
            self.start()

    def start(self,daemonic=True):
        if super(DistServer, self).start(daemonic=daemonic):
            with self.lock:
                if self.local:
                    t = Thread(target=self.acceptloop)
                    t.setDaemon(daemonic)
                    t.start()
                else:
                    self.ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.ss.connect((self.addr,self.port))
                    self.ss.send(DistServer.SERVER_TAG)
                    sock_obj = Worker(self.ss,self)
                    self.allsockets.append(sock_obj)
                    self.workers.addWorker(sock_obj)
                t = Thread(target=self.serveloop)
                t.setDaemon(daemonic)
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
                    self.workers.addWorker(sock_obj)
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
                            r.recvandwork()
                            if type(r)==Worker:
                                self.workers.freeWorker(r)
                            while r.hasdata():
                                r.recvandwork()
                                if type(r) == Worker:
                                    self.workers.freeWorker(r)
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
                                self.workers.removeWorker(r)
                            for jobargs in unfinishedjobs:
                                print("Re-adding {}... ".format(jobargs),end='')
                                self.addJob(*jobargs)
                                print("done")
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
                        self.workers.removeWorker(x)
                    for jobargs in unfinishedjobs:
                        print("Re-adding ", jobargs)
                        self.addJob(*jobargs)
                        print("Done")
            except OSError:
                pass

    def getNumWorkerThreads(self):
        with self.lock:
            return self.numWorkerThreads

    def dispatch(self,f,args,kwargs,deferobj,callback,ecallback,addjobargs):
        # Send to worker
        nextworker = self.workers.getBestWorker()
        nextworker.dispatchJob(f,args,kwargs,deferobj,
                               callback,ecallback,addjobargs)
        self.workers.recruitWorker(nextworker)


class Client:
    def __init__(self,sock,distserver):
        self.fsock = FormatSocket(sock)
        self.funccache = {}
        self.lock = Lock()
        self.distserver = distserver

        dillbytes = dill.dumps({THREADS_JSON: distserver.getNumWorkerThreads()})
        self.fsock.send(dillbytes)

    def recvandwork(self):
        msg = self.fsock.recv()
        dillobj = dill.loads(msg)

        fid = dillobj[FID_JSON]
        jobid = dillobj[JOBID_JSON]

        args = dillobj[ARGS_JSON]
        kwargs = dillobj[KWARGS_JSON]
        with self.lock:
            if FUNCTION_JSON in dillobj:
                self.funccache[fid] = dill.loads(dillobj[FUNCTION_JSON])
            f = self.funccache[fid]

        def makecallbacks(jid):
            def customcallback(result):
                cdillbytes = dill.dumps({JOBID_JSON: jid,
                                         RESULT_JSON: result})
                self.fsock.send(cdillbytes)
            def errorcallback(error):
                cdillbytes = dill.dumps({JOBID_JSON: jid,
                                         ERROR_JSON: error})
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
