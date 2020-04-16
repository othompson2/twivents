import streamz
from dask.distributed import Client
from time import time, sleep


def step1(i):
    print("step1:" + str(i))
    sleep(0.1)
    return i

def step2(i):
    # print("step2:" + str(i))
    sleep(0.1)
    return i

def nothing(_):
    pass

client = Client(processes=False)

source = streamz.Stream()
source.scatter().map(step1).buffer(5).gather().sink(nothing)
# source.map(step1).sink(nothing)

start = time()

for i in range(100):
    # print(i)
    source.emit(i)

print(time() - start)
