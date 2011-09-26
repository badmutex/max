
# import ezlog
# import dax, rax

# import workqueue

import zmq

import sys
import multiprocessing
import cPickle as pickle



def worker():

    print 'worker: starting'

    context  = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.connect('tcp://*:5559')

    while True:
        msg = receiver.recv()
        obj = pickle.loads(msg)

        if type(obj) is bool and not obj:
            break
        # else:
        #     print 'worker: got object', type(obj), obj


def master(n):

    print 'master: starting'

    context = zmq.Context()
    sender  = context.socket(zmq.PUSH)
    sender.bind('tcp://*:5559')

    for i in xrange(n):
        msg = pickle.dumps(i)
        sender.send(msg)
    sender.send(pickle.dumps(False))



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
