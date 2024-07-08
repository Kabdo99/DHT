import sys
from mpi4py import MPI #mpi4py library
from dht_globals import * #global variables

def commandNode(): 
    dummy = 0

    for i in range(1,101):
        keyval = [i, i+100]
        MPI.COMM_WORLD.send(keyval, dest=0, tag=PUT)
        MPI.COMM_WORLD.recv(source=0, tag=ACK)

    addArgs = [3, 50]
    MPI.COMM_WORLD.send(addArgs, dest=0, tag=ADD)
    MPI.COMM_WORLD.recv(source=0, tag=ACK)

    addArgs = [1, 12]
    MPI.COMM_WORLD.send(addArgs, dest=0, tag=ADD)
    MPI.COMM_WORLD.recv(source=0, tag=ACK)

    removeArg = 12
    MPI.COMM_WORLD.send(removeArg, dest=0, tag=REMOVE)
    MPI.COMM_WORLD.recv(source=0, tag=ACK)

    for i in range(1,101):
        key = i
        MPI.COMM_WORLD.send(key, dest=0, tag=GET)
        answer = MPI.COMM_WORLD.recv(source=0, tag=RETVAL)
        print(f"val is {answer[0]}, storage id is {answer[1]}")

    MPI.COMM_WORLD.send(dummy, dest=0, tag=END)
    print("command finalizing")
    exit(0)
