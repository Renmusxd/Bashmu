from bashmu.distserver import DistServer

ds = DistServer('localhost',1708)


@ds.defer
def foo(n, s):
    import time
    print("Running", n, s)
    time.sleep(n)
    print("Done", n, s)
    return s + 1

@ds.defer
def bar(n):
    import time
    print("Running",n)
    time.sleep(n)
    print("Done",n)
    return n**2

@ds.defer
def broken():
    return "1" + 1

if __name__ == "__main__":
    a = foo(1,0)
    b = foo(2,a)
    c = foo(3,a)
    d = foo(4,a)
    e = foo(5,a)
    print("Printing results:")
    print(",".join(map(str,[a,b,c,d,e])))

    f = broken()
    print(str(f))

ds.stop()
