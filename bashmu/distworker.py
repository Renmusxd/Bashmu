from bashmu.FormatSock import FormatSocket
from bashmu.distserver import DistServer
import dill
import socket
import multiprocessing
import sys
import traceback


class Worker:
    def __init__(self,addr,port,threads=8):
        self.addr = addr
        self.port = port
        self.fsock = None
        self.threads = threads
        self.pool = multiprocessing.Pool(self.threads)
        self.funccache = {}

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.addr,self.port))
        sock.send(DistServer.WORKER_TAG)
        self.fsock = FormatSocket(sock)

        dillbytes = dill.dumps({DistServer.THREADS_JSON:self.threads})
        self.fsock.send(dillbytes)
        while True:
            msg = self.fsock.recv()
            dillobj = dill.loads(msg)
            fid = dillobj[DistServer.FID_JSON]
            jobid = dillobj[DistServer.JOBID_JSON]
            args = dillobj[DistServer.ARGS_JSON]
            kwargs = dillobj[DistServer.KWARGS_JSON]
            if DistServer.FUNCTION_JSON in dillobj:
                self.funccache[fid] = dillobj[DistServer.FUNCTION_JSON]
            fcode = self.funccache[fid]

            def makecustomcallback(jid):
                def customcallback(result):
                    cdillbytes = dill.dumps({DistServer.JOBID_JSON: jid,
                                             DistServer.RESULT_JSON: result})
                    self.fsock.send(cdillbytes)
                def errorcallback(error):
                    cdillbytes = dill.dumps({DistServer.JOBID_JSON: jid,
                                             DistServer.ERROR_JSON: error})
                    self.fsock.send(cdillbytes)
                return customcallback, errorcallback

            customcallback, errorcallback = makecustomcallback(jobid)
            self.pool.apply_async(undillfunc, [fcode]+list(args), kwargs,
                                  customcallback, errorcallback)


class DeferredException(Exception):
    def __init__(self, message):
        super(DeferredException, self).__init__(message)

    @staticmethod
    def make_error_message(f,args,kwargs,error):
        errortype = str(type(error).__name__)
        fname = str(f.__name__)
        total_args = list(str(arg) for arg in args) + list(str(key)+"="+str(kwargs[key]) for key in kwargs.keys())
        argsstring = ", ".join(total_args)
        return "[{errortype}] {fname}({argstring}): {errormessage}".format(errortype=errortype,fname=fname,
                                                                           argstring=argsstring,
                                                                           errormessage=str(error))


def undillfunc(fcode, *args, **kwargs):
    newf = dill.loads(fcode)
    try:
        return newf(*args, **kwargs)
    except Exception as error:
        message = DeferredException.make_error_message(newf,args,kwargs,error)
        raise DeferredException(message)

