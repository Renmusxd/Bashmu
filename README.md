
# Bashmu v0.1
A pretty python distributed computing framework

## Name
Bashmu is the Mesopotamian origin story for the Hydra. This seven headed serpent could likely do many things with each head at the same time, I felt the metaphor was appropriate. 

## Purpose
Bashmu aims to add distributed computing with as little code as possible, it therefore makes use of python's decorators to add functions to the network. The results of those functions are wrapped in Deferred objects (much like async Futures) which act almost exactly like their original python counterpart. Nearly all attributes of the deferred object are forwarded to the value once it is computed.

## Example Code:

### Basic Example:
The following example computes 5 values, the last four depend on the result of the first one and are thus dispatched to workers after the first is completed. That being said you will notice that the first print statement is run immediately whereas the second print statement comes about 6 seconds afterwards.

```python
from bashmu.distserver import DistServer

ds = DistServer('localhost',1708)

@ds.defer
def foo(n, s):
    import time
    print("Running", n, s)
    time.sleep(n)
    print("Done", n, s)
    return s + 1
    
a = foo(1,0)
b = foo(2,a)
c = foo(3,a)
d = foo(4,a)
e = foo(5,a)

print("Printing results:")
print(",".join(map(str,[a,b,c,d,e])))   

ds.stop()
```

Results in:

```
Printing results:
1,2,2,2,2
Stopping
```

### Caveats
Currently the system does not check instance variables (recursively) of arguments to function for `Deferred` objects, as such only args which are `Deferred` or iterable containing `Deferred` are properly accounted for.

### Mapping Example:

Consider mapping the function, simply using python's `map` function isn't efficient because it creates a generator which doesn't actually call the function until the generator's `next` is called. As such mapping should be done pseudo-manually as seen below: 

```python
from bashmu.distserver import DistServer

ds = DistServer('localhost',1708)

@ds.defer
def bar(n):
    import time
    print("Running",n)
    time.sleep(n)
    print("Done",n)
    return n**2

if __name__ == "__main__":
    print("Mapping function:")
    arr = [x for x in map(bar,range(10))]
    print(arr)
    print([str(x) for x in arr])
    print(arr)

ds.stop()
```

Observe that the first print statement is immediate and reveals that the contained objects are `Deferred`, the second print statement requires 9 seconds to complete. The final print statement comes immediately after and described the new contained value:

```
Mapping function:
[(Deferred: [<function bar at 0x102507598>,(0,),{}]), ...]
['0', '1', '4', '9', '16', '25', '36', '49', '64', '81']
[(Deferred: 0), (Deferred: 1), ...]
Stopping
```

### Errors
Errors an exceptions are raised when the value of a `Deferred` object is requested. The traceback leads to the deferred call and in future versions further debug information will be available.