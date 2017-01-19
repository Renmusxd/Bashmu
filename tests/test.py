from scylla.distthreadpool import DistThreadPool
import time

ds = DistThreadPool()


@ds.defer
def foo(n, s):
    print("Running",n,s)
    time.sleep(n)
    print("Done",n,s)
    return ['hi']

if __name__ == "__main__":
    a = foo(1,['0'])
    b = foo(2,a)
    c = foo(3,a)
    d = foo(4,a)

    print("Here")

    ds.wait()

    print("Done waiting")

    print(a + b + c + d)

    ds.stop()