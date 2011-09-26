
# import ezlog
# import dax, rax

# import workqueue

import zmq

import sys
import multiprocessing
import cPickle as pickle



class Connection(object):
    """
    Wraps a ZMQ socket for automatic serialization/deserialization of messages
    """

    def __init__(self, zmq_socket):
        self.socket = zmq_socket

    def send(self, obj, *args, **kws):
        """
        Send the object over the zmq.socket after serialization

        @param obj (object): any pickle-able python object
        @params *args, **kws: any other arguments to pass to zqm.socket.send
        """
        msg = pickle.dumps(obj)
        self.socket.send(msg, *args, **kws)

    def recv(self, *args, **kws):
        """
        Receive a deserialzed message

        @params *args, **kws: any further arguments to be passed to zmq.socket.recv
        @return (object): an unpickled object
        """

        msg = self.socket.recv(*args, **kws)
        obj = pickle.loads(msg)
        return obj


class WQMaster(object):
    pass

def worker():

    print 'worker: starting'

    context = zmq.Context()
    sock    = context.socket(zmq.PULL)
    sock.connect('tcp://*:5559')
    conn    = Connection(sock)

    while True:
        obj = conn.recv()

        if type(obj) is bool and not obj:
            break
        # else:
        #     print 'worker: got object', type(obj), obj


def master(n):

    print 'master: starting'

    context = zmq.Context()
    sock  = context.socket(zmq.PUSH)
    sock.bind('tcp://*:5559')
    conn = Connection(sock)

    for i in xrange(n):
        conn.send(i)
    conn.send(False)



def main():

    worker_proc = multiprocessing.Process(target=worker, args=())
    worker_proc.start()

    n = 400000
    import time
    start = time.time()
    master(n)
    end = time.time()
    duration = end - start
    msg_per_sec = n / float(duration)
    print n, duration
    print 'Duration:', duration
    print 'Message/sec:', msg_per_sec


if __name__ == '__main__':
    main()


# if __name__ == '__main__':

#     daxdata  = sys.argv[1]
#     mapperfn = sys.argv[2]
#     raxdata  = sys.argv[3]


#     setup_mapper(mapperfn)
#     from locale_max_mapper import mapper as max_mapper


#     daxproj = dax.Project()
#     daxproj.load_dax(daxdata)

#     master = Master()
#     master.start(max_mapper, daxproj)

#     raxproj = rax.Project()
#     raxproj.set_prefix(raxdata)

#     while True:

#         if master.isfinished():
#             break

#         result = master.recv()
#         raxproj.add_max(result)



#     raxproj.write_rax()
