try:
    from bashmu.dist import DistBase
except ImportError:
    from .dist import DistBase

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

