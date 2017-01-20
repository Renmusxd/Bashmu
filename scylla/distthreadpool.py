from scylla.dist import DistBase
from multiprocessing import Pool, RLock
import dill


class DistThreadPool(DistBase):
    def __init__(self, start=True, threads=8):
        super().__init__(start=start)
        self.pool = Pool(threads)
        self.poollock = RLock()

    def dispatch(self,f,args,kwargs,deferobj,callback,ecallback,addjobargs):
        with self.poollock:
            fcode = dill.dumps(f)
            def makecallback(dobj):
                def customcallback(result):
                    callback(result)
                    dobj.__setvalue__(result)
                return customcallback
            self.pool.apply_async(undillfunc, [fcode] + list(args), kwargs,
                                  makecallback(deferobj), ecallback)


def undillfunc(fcode, *args, **kwargs):
    newf = dill.loads(fcode)
    return newf(*args, **kwargs)
