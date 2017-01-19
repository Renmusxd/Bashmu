from scylla.dist import DistBase
from multiprocessing import Pool, RLock
import dill

class DistThreadPool(DistBase):

    def __init__(self, start=True, threads=8):
        super().__init__(start=start)
        self.pool = Pool(threads)
        self.poollock = RLock()

    def dispatch(self,f,args,kwargs,deferobj,callback,addjobargs):
        with self.poollock:
            fcode = dill.dumps(f)
            def customcallback(result):
                ndo = deferobj
                ndo.__setvalue__(result)
                callback(ndo)
            def errorcallback(result):
                ndo = deferobj
                ndo.__setvalue__(None)
                callback(ndo)
                raise result
            self.pool.apply_async(undillfunc, [fcode] + list(args), kwargs, customcallback, errorcallback)


def undillfunc(fcode, *args, **kwargs):
    newf = dill.loads(fcode)
    return newf(*args, **kwargs)
