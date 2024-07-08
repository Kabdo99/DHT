##########################################################################################
# Skeleton code author: David Lowenthal
# Additional code author: Abdullah Alkhamis
# Class: CSc 422 - Parallel & Distributed Programming
# Assignment: Program 3 - Distributed Hash Tables
# Description: This program implements a distributed hash table (DHT) using MPI.
#              The DHT consists of a head node, storage nodes, and a command node.
#              The storage nodes store key-value pairs, and the command node issues
#              commands to add/remove storage nodes and put/get key-value pairs.
#              The program demonstrates the functionality of the DHT by redistributing
#              data when nodes are added or removed from the system.
##########################################################################################

import sys # system functions
from mpi4py import MPI #mpi4py library
from dht_globals import * #global variables
from command import commandNode #command node code

# on an END message, the head node is to contact all storage nodes and tell them
def headEnd():
    """
    Handles the END message for the head node.
    Contacts all storage nodes and tells them to end.
    """
    # tell all the storage nodes to END
    # the data sent is unimportant here, so just send a dummy value
    dummy = MPI.COMM_WORLD.recv(source=numProcesses-1, tag=END)
    for i in range(1,numProcesses-1):
        MPI.COMM_WORLD.send(dummy, dest=i, tag=END)
    MPI.Finalize()
    sys.exit(0)
    

# on an END message, a storage node just calls MPI_Finalize and exits
def storageEnd():
    """
    Handles the END message for a storage node.
    Finalizes MPI and exits the program.
    """ 
    # the data is unimportant for an END
    dummy = MPI.COMM_WORLD.recv(source=0, tag=END)
    MPI.Finalize()
    sys.exit(0)


def getKeyVal(source):
    """
    Handles the GET message for retrieving a key-value pair.
    Checks if the key is present in the current storage node or forwards the request to the child node.
    """
    # receive the GET message
    # note that at this point, we've only called MPI_Probe, which only peeks at the message
    # we are receiving the key from whoever sent us the message 
    key = MPI.COMM_WORLD.recv(source=source, tag=GET)

    # if the key is less than or equal to the current storage id, then the value is in the current storage node
    if key <= myStorageId:
        if key in storage:
            value = storage[key]
            # allocate a tuple with two integers: the first will be the value, the second will be this storage id
            argsAdd = (value, myStorageId)
            MPI.COMM_WORLD.send(argsAdd, dest=childRank, tag=RETVAL)
    else:
        # forward the request to the child node
        MPI.COMM_WORLD.send(key, dest=childRank, tag=GET)


def putKeyVal(source):
    """
    Handles the PUT message for storing a key-value pair.
    Stores the key-value pair in the current storage node if the key belongs to its range, otherwise forwards the request to the child node.
    """
    global storage
    # receive the PUT message
    keyVal = MPI.COMM_WORLD.recv(source=source, tag=PUT)
    key = keyVal[0]
    value = keyVal[1]

    # check if the key belongs to the current storage node
    if key <= myStorageId:
        storage[key] = value
        # send an acknowledgment to the parent node
        MPI.COMM_WORLD.send(keyVal, dest=childRank, tag=ACK)
    else:
        # forward the request to the child node
        MPI.COMM_WORLD.send(keyVal, dest=childRank, tag=PUT)


def addNode(source):
    """
    Handles the ADD message for adding a new storage node to the system.
    Initializes the new node, updates the parent and child pointers, and redistributes data if necessary.
    """
    global myRank, myStorageId,  storage, parentRank, parentStorageID, childRank, childStorageId\
    # receive the ADD message
    rankStorageId = MPI.COMM_WORLD.recv(source=source, tag=ADD)
    newNodeRank = rankStorageId[0]
    newNodeStorageId = rankStorageId[1]
    
    # we add once we're at the new node's child node
    if newNodeStorageId < myStorageId:
        newNodeData = (myRank, myStorageId, parentRank, parentStorageID, newNodeRank, newNodeStorageId, storage)
      
        # initialize the new node with the proper data
        MPI.COMM_WORLD.send(newNodeData, dest=newNodeRank, tag=INIT)
        MPI.COMM_WORLD.recv(source=newNodeRank, tag=COMPLETE)

  

        # update the parent pointers of the current child (before new node additon) to point to the new node
        MPI.COMM_WORLD.send(newNodeData, dest=parentRank, tag=UPDATE_PARENT_POINTERS)
        MPI.COMM_WORLD.recv(source=parentRank, tag=COMPLETE)

        # update the current node's child's pointers to point to the new node
        parentRank = newNodeRank
        # update the childStorageId to the new node's storage id
        parentStorageID = newNodeStorageId

        # redistribute data if necessary
        if bool(storage):
            for key in storage:
                if key <= newNodeStorageId:
                    MPI.COMM_WORLD.send(newNodeData, dest=parentRank, tag=REDISTRIBUTE_ADD)
                    dictionaries = MPI.COMM_WORLD.recv(source=parentRank, tag=COMPLETE)
                    storage = dictionaries[1]
                    
        MPI.COMM_WORLD.send(None, dest=childRank, tag=ACK)
    else:
        MPI.COMM_WORLD.send(rankStorageId, dest=childRank, tag=ADD)


def removeNode(source):
    """
    Handles the REMOVE message for removing a storage node from the system.
    Redistributes data from the removed node to the remaining nodes, updates parent and child pointers, and notifies the command node.
    """
    global myRank, myStorageId, parentRank, parentStorageID, childRank, childStorageId, storage
    storageId = MPI.COMM_WORLD.recv(source=source, tag=REMOVE)

    # if the storage id is the same as the current node's storage id, then remove the node
    if myStorageId == storageId:
        removedNodeData = (myRank, myStorageId, parentRank, parentStorageID, childRank, childStorageId, storage)

        # redistribute data if necessary
        if bool(storage):
            MPI.COMM_WORLD.send(removedNodeData, dest=childRank, tag=REDISTRIBUTE_REMOVE)
            MPI.COMM_WORLD.recv(source=childRank, tag=COMPLETE)
        
       # link the parent of current node (node to be removed) to the child of the current node
        MPI.COMM_WORLD.send(removedNodeData, dest=parentRank, tag=UPDATE_PARENT_POINTERS)
        MPI.COMM_WORLD.recv(source=parentRank, tag=COMPLETE)

        # link the child of the current node (node to be removed) to the parent of the current node
        MPI.COMM_WORLD.send((parentRank, parentStorageID), dest=childRank, tag=UPDATE_CHILD_POINTERS)
        MPI.COMM_WORLD.recv(source=childRank, tag=COMPLETE)

        # remove the parent and child pointers from the current node (node to be removed)
        previousParentRank = parentRank
        parentRank = None
        parentStorageID = None
        childRank = None
        childStorageId = None
        storage = None

        # notify command node that remove is complete
        MPI.COMM_WORLD.send(None, dest=previousParentRank, tag=ACK)
    else:
        MPI.COMM_WORLD.send(storageId, dest=childRank, tag=REMOVE)


def retvalHelper(source):
    """
    Helper function for handling the RETVAL message.
    Forwards the message to the parent or child node based on the current node's rank.
    """
    # receive the message using the RETVAL tag
    argsAdd = MPI.COMM_WORLD.recv(source=source, tag=RETVAL)

    # forward the message to the parent or child node based on the current node's rank
    if myRank == 0:
        MPI.COMM_WORLD.send(argsAdd, dest=parentRank, tag=RETVAL)
    else:
        MPI.COMM_WORLD.send(argsAdd, dest=childRank, tag=RETVAL)


def ackHelper(source):
    """
    Helper function for handling the ACK message.
    Forwards the message to the parent or child node based on the current node's rank.
    """
    # receive the message using the ACK tag
    data = MPI.COMM_WORLD.recv(source=source, tag=ACK)

    # forward the message to the parent or child node based on the current node's rank
    if myRank == 0:
        MPI.COMM_WORLD.send(data, dest=parentRank, tag=ACK)
    else:
        MPI.COMM_WORLD.send(data, dest=childRank, tag=ACK)


def completeHelper(source):
    """
    Helper function for handling the COMPLETE message.
    Sends the message back to the source node.
    """
    # receive the message using the COMPLETE tag
    data = MPI.COMM_WORLD.recv(source=source, tag=COMPLETE)

    # send the message with the proper data using the COMPLETE tag to the parent
    MPI.COMM_WORLD.send(data, dest=source, tag=COMPLETE)


def init(source):
    """
    Handles the INIT message for initializing a new storage node.
    Sets up the parent and child pointers, rank, storage ID, and storage dictionary.
    """
    global myRank, myStorageId, parentRank, parentStorageID, childRank, childStorageId, storage

    # receive the INIT message
    nodeData = MPI.COMM_WORLD.recv(source=source, tag=INIT)

    # initialize the new node with the proper data
    parentRank = nodeData[2]
    parentStorageID = nodeData[3]
    childRank = nodeData[0]
    childStorageId = nodeData[1]
    myRank = nodeData[4]
    myStorageId = nodeData[5]
    storage = {}

    # notify parent that initialization is complete
    MPI.COMM_WORLD.send(None, dest=source, tag=COMPLETE)

    
def updateChildPointers(source):
    """
    Handles the UPDATE_CHILD_POINTERS message for updating the child pointers of a storage node.
    Updates the parent rank and storage ID of the current node.
    """
    global parentRank, parentStorageID

    # receive the UPDATE_CHILD_POINTERS message
    nodeData = MPI.COMM_WORLD.recv(source=source, tag=UPDATE_CHILD_POINTERS)

    # update the current node's child's pointer to another node
    parentRank = nodeData[0]
    parentStorageID = nodeData[1]
    MPI.COMM_WORLD.send(None, dest=source, tag=COMPLETE)


def updateParentPointers(source):
    """
    Handles the UPDATE_PARENT_POINTERS message for updating the parent pointers of a storage node.
    Updates the child rank and storage ID of the current node.
    """
    global childRank, childStorageId

    # receive the UPDATE_PARENT_POINTERS message
    nodeData = MPI.COMM_WORLD.recv(source=source, tag=UPDATE_PARENT_POINTERS)

    # update current node's parent's pointer to another node
    childRank = nodeData[4]
    childStorageId = nodeData[5]
    MPI.COMM_WORLD.send(None, dest=source, tag=COMPLETE)


def redistributeRemove(source):
    """
    Handles the REDISTRIBUTE_REMOVE message for redistributing data when a node is removed.
    Transfers the data from the parent node to the current node.
    """
    global storage

    # receive the REDISTRIBUTE_REMOVE message
    nodeData = MPI.COMM_WORLD.recv(source=source, tag=REDISTRIBUTE_REMOVE)
    
    # redistribute data from the parent node to the current node
    parentStorage = nodeData[6]
    for key, value in parentStorage.items():
        storage[key] = value

    MPI.COMM_WORLD.send(None, dest=source, tag=COMPLETE)
    

def redistributeAdd(source):
    """
    Handles the REDISTRIBUTE_ADD message for redistributing data when a node is added.
    Transfers the data from the current node to the child node based on the storage ID.
    """
    global storage

    # receive the REDISTRIBUTE_ADD message
    nodeData = MPI.COMM_WORLD.recv(source=source, tag=REDISTRIBUTE_ADD)

    # redistribute data from the current node to the child node based on the storage ID
    childStorage = nodeData[6]
    for key, value in childStorage.items():
        if key <= myStorageId:
            storage[key] = value

    # update the child node's storage dictionary
    updatedChildStorage = {key: value for key, value in childStorage.items() if key not in storage}
    
    dictionaries = (storage, updatedChildStorage)
    # Send an acknowledgment to the parent node indicating that the data redistribution is complete
    MPI.COMM_WORLD.send(dictionaries, dest=source, tag=COMPLETE)


def handleMessages():
    status = MPI.Status() # get a status object
    while True:
        # Peek at the message
        MPI.COMM_WORLD.probe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)    

        # get the source and the tag---which MPI rank sent the message, and
        # what the tag of that message was (the tag is the command)
        source = status.Get_source()
        tag = status.Get_tag()

        # now take the appropriate action
        # code for END and most of GET is given; others require your code
        # python 3.10 adds switch statements, but lectura has python 3.8
        if tag == END:
            if myRank == 0:
                headEnd()
            else:
                storageEnd()
            pass
        elif tag == ADD:
            addNode(source)
        elif tag == REMOVE:
            removeNode(source)
        elif tag == PUT:
            putKeyVal(source)
        elif tag == GET:
            getKeyVal(source)
        elif tag == ACK:
            ackHelper(source)
        elif tag == RETVAL:
            retvalHelper(source)
        elif tag == INIT:
            init(source)
        elif tag == COMPLETE:
            completeHelper(source)
        elif tag == UPDATE_CHILD_POINTERS:
            updateChildPointers(source)
        elif tag == UPDATE_PARENT_POINTERS:
            updateParentPointers(source)
        elif tag == REDISTRIBUTE_REMOVE:
            redistributeRemove(source)
        elif tag == REDISTRIBUTE_ADD:
            redistributeAdd(source)
        # NOTE: you probably will want to add more cases here, e.g., to handle data redistribution
        else:
            # should never be reached---if it is, something is wrong with your code; just bail out
            print(f"ERROR, my id is {myRank}, source is {source}, tag is {tag}")
            sys.exit(1)


if __name__ == "__main__":
    # get my rank and the total number of processes
    numProcesses = MPI.COMM_WORLD.Get_size()
    myRank = MPI.COMM_WORLD.Get_rank()

    # set up the head node with the proper values
    if myRank == 0:
        myStorageId = 0
        parentRank = numProcesses - 1
        childRank = numProcesses - 2
        childStorageId = MAX
        parentStorageID = None
        storage = None

    # set up the last storage nodes with the proper values
    elif myRank == numProcesses - 2:
        myStorageId = MAX
        parentRank = 0
        childRank = 0
        childStorageId = 0
        parentStorageID = 0
        storage = {}

    # the command node is handled separately
    if myRank < numProcesses-1:
        handleMessages()
    else:
        commandNode()