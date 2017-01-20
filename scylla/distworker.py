from scylla.FormatSock import FormatSocket
from scylla.distserver import DistServer
import dill
import socket
import multiprocessing


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
            self.pool.apply_async(undillfunc, [fcode,jobid]+list(args), kwargs,
                                  customcallback, errorcallback)


def undillfunc(fcode,jobid, *args, **kwargs):
    newf = dill.loads(fcode)
    return newf(*args, **kwargs)
