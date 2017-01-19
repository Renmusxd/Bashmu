from scylla.distserver import DistServer

ds = DistServer('localhost',1708)


@ds.defer
def foo(n, s):
    import time
    print("Running", n, s)
    time.sleep(n)
    print("Done", n, s)
    return s + 1

if __name__ == "__main__":
    # with ds:
    #     a = foo(1,0)
    #     b = foo(2,a)
    #     c = foo(3,a)
    #     d = foo(4,a)
    #     e = foo(5,a)
    #     print("Starting to wait...")
    #     ds.wait()
    #     print("Printing results:")
    #     print(",".join([str(a),str(b),str(c),str(d),str(e)]))
    print("Mapping function:")
    print([str(x) for x in [foo(i%4,i) for i in range(32)]])

ds.stop()
