from scylla.distworker import Worker
import time

w = Worker('localhost',1708)
while True:
    try:
        w.run()
    except OSError:
        time.sleep(1)