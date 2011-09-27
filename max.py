
import dax
import ezlog

import zmq
import workqueue

import sys
import itertools
import multiprocessing


class Task(object):

    WRAPPER_NAME        = 'wrapper.sh'
    WORKER_NAME         = 'work'
    IN_NAME             = 'system.pkl'
    JOB_LOGFILE         = 'worker.log'
    WQW_LOGFILE         = 'wqw.log'
    WORKER_RESULTS      = 'results.tar.bz2'
    WORKER_RETURN       = [JOB_LOGFILE, WQW_LOGFILE]
    MODULES_HOME        = '/afs/crc.nd.edu/x86_64_linux/Modules/3.2.6'

    WORKAREA            = 'max.workarea'

    def __init__(self, func, data, **kws):

        self.function    = function
        self.data        = data

        self.modules     = kws.pop('modules', Modules())
        self.moduleshome = kws.pop('moduleshome', Task.MODULES_HOME)
        self.workername  = kws.pop('workername', Task.WORKER_NAME)
        self.infile      = kws.pop('infile', Task.IN_NAME)
        self.proclogfile = kws.pop('proclogfile', Task.JOB_LOGFILE)
        self.wqwlogfile  = kws.pop('wqwlogfile', Task.WQW_LOGFILE)

        self.chunkid     = kws.pop('chunkid', 0)

        self._langs      = {'bash' : self._bash}

    def to_wq_task(self):

        self.write_wrapper(Task.WRAPPER_NAME)
        self.write_worker(Task.WORKER_NAME)
        pickle.dump(self.function, Task.IN_NAME)

        task = workqueue.Task('./%(wrapper)s >wq.log' % {'wrapper' : Task.WRAPPER_NAME})
        task.specify_input_file(Task.WRAPPER_NAME, Task.WRAPPER_NAME)
        task.specify_input_file(Task.WORKER_NAME, Task.WORKER_NAME)
        task.specify_input_file(Task.IN_NAME, Task.IN_NAME)

        if not os.path.exists(Task.WORKAREA):
            os.makedirs(Task.WORKAREA)

        task.specify_output_file(os.path.join(Task.WORKAREA, Task.chunkname(self.id)), Task.WORKER_RESULTS)
        for outfile in Task.WORKER_RETURN:
            local = os.path.join(Task.WORKAREA, 'chunk-%04d-%s' % (self.chunkid, outfile), outfile)
            task.specify_output_file(local, outfile)

        return task
        


    @classmethod
    def chunkname(cls, chunkid):
        return 'chunk-%04d-tar.bz2' % chunkid


    def write_worker(self, path):

        code = inspect.getsource(self._pyworker)

        with open(path, 'w') as fd:
            fd.write('#!/usr/bin/env python\n\n')
            fd.write(code)

        os.chmod(path, stat.S_IRWXU)

    def _pyworker(self):
        import cPickle as pickle

        import sys
        import os
        import tempfile

        EXITCODE = 0

        infile     = sys.argv[1]
        outfile    = sys.argv[2]
        paramfiles = sys.argv[3:]

        tempdir = tempfile.mkdtemp(prefix='max-wa')

        with open(outfile, 'w') as fd_log:

            fd_log.write('Loading function and arguments\n')
            func = pickle.load(infile)


            for path in paramfiles:

                fd_log.write('Processing: %s\n' % path)

                header          = func.header()
                run, clone, gen = func.read_rcg(path)

                fd_log.write('\tHeader: %s\n' % header)
                fd_log.write('\tRUN %d CLONE %d GEN %d\n' % (run,clone,gen))

                workarea        = dax.cannonical_traj(run, clone)
                workarea        = os.path.join(tempdir, workarea)
                target_name     = 'GEN%04d.dat' % gen
                target          = os.path.join(workarea, target_name)

                with open(target, 'w') as fd:

                    if type(header) is str and header:
                        fd.write('# ' + header + '\n')

                    try:
                        results = func(path)
                    except e:
                        fd_log.write('\tFailed to work on %s: %s\n' % (path, e))
                        EXITCODE = 1
                        continue

                    fd_log.write('\tWriting results\n')

                    for frame, result in enumerate(results):
                        line = '%(run)d,%(clone)d,%(gen)d,%(frame)d,%(result)s' % {
                            'run' : run, 'clone' : clone, 'gen' : gen, 'frame' : frame,
                            'result' : result}

                        fd.write(line + '\n')


            cwd = os.getcwd()
            os.chdir(workarea)
            compress = 'tar cvf results.tar.bz2 RUN*'

            fd_log.write('Compressing results: %s\n' % compress)

            compress_success = os.system(compress)
            if compress_success == 0:
                fd_log.write('\tOK\n')
            else:
                fd_log.write('\tFail: %s\n' % compress_success)

            os.chdir(cwd)

        sys.exit(EXITCODE)



    def _bash(self):

        bash = """\
#!/usr/bin/env bash

function module()
{
    MODULESHOME=%(moduleshome)s
    eval $($MODULESHOME/bin/modulecmd sh $*)
}

%(load_modules)s

./%(work)s %(infile)s %(proclogfile)s %(paramfiles)s >%(wqlogfile)s 2>&1
""" % { 'moduleshome'  : self.moduleshome,
        'load_modules' : self.modules.get_modules_script(),
        'work'         : self.workername,
        'infile'       : self.infile,
        'proclogfile'  : self.proclogfile,
        'paramfiles'   : ' '.join(self.data),
        'wqlogfile'    : self.wqwlogfile,
        }


    def write_wrapper(self, outfile, kind='bash'):
        if kind not in self._langs:
            raise ValueError, 'Unknown kind %s. Try one of %s' % (kind, ' '.join(self._langs.keys()))

        lang       = self._langs[kind]
        executable = lang()

        with open(outfile, 'w') as fd:
            fd.write(executable)


class Modules(object):

    def __init__(self, modulefiles = set(), modules = set()):

        self._modulefiles = modulefiles
        self._modules     = modules


    def get_modules_script(self):

        def get_str():

            for modulefilesgroup, modulesgroup in itertools.izip_longest(self._modulefiles, self._modules, None):

                if modulefilesgroup is not None:
                    for modulefile in modulefilesgroup:
                        yield 'module use %s' % modulefile

                if modulesgroups is not None:
                    for module in modulesgroup:
                        yield 'module load %s' % module

        return '\n'.join(get_str())


    def add_modulefiles(self, *paths):
        self._modulefiles.append(list(paths))

    def add_modules(self, *modules):
        self._modules.append(list(modules))





class Result(object):

    def __init__(self, wqtask):
        chunkid = int(wqtask.tag)
        tempdir = tempfile.mkdtemp()
        self.data = list()


    def load(self):

        cmd = 'tar -C %(workarea)s -xvf %(tarfile)s' % {
            'workarea' : tempdir,
            'tarfile' : Task.chunkname(chunkid) }
        print 'Executing:', cmd
        os.system(cmd)

        pattern = os.path.join(tempdir, 'RUN*/CLONE*/GEN*.dat')
        for datafile in glob.iglob(pattern):
            run, clone, gen = dax.read_cannonical(datafile)
            results = np.loadtxt(datafile, delimiter=',', unpack=True, dtype=str)
            self.data.append((run, clone, gen, results[-1]))

    def __iter__(self):
        return iter(self.data)
        




class Function(object):

    def header(self):
        return None

    def read_rcg(path):
        import dax
        return dax.read_cannonical(path)

    def __call__(self, path):
        raise NotImplementedError





class Master(object):

    STOP = True

    def __init__(self, pushport=5559):

        self.pushport  = pushport
        self._protocol = 'tcp'
        self._addess   = 'localhost'


    def address(self, port=None):
        return '%(protocol)://%(address)%(port)s' % {
            'protocol' : self._protocol,
            'address' : self._address,
            'port' : '' if port is None else ':%d' % port }

    def input_files(self, *paths):
        raise NotImplementedError

    def output_fiels(self, *paths):
        raise NotImplementedError


    def __call__(self):

        context   = zmq.Context()

        insocket  = context.socket(zmq.PULL)
        outsocket = context.socket(zmq.PUSH)

        inport    = insocket.bind_to_random_port(self.address())
        outsocket.bind(self.address(self.pushport))

        WQ        = workqueue.WorkQueue()


        outsocket.send_pyobj(inport)

        while True:

            task = insocket.recv(zmq.NOBLOCK)

            if task == Master.STOP:
                break

            elif task:
                task.materialize()
                wqtask = task.to_wq_task()

                WQ.submit(wqtask)

            else:

                while not WQ.empty():
                    result_task = WQ.wait()
                    result      = Result(result_task)
                    result.load()
                    outsocket.send_pyobj(result)


class Pool(object):


    def __init__(self, modules=Modules()):

        self.modules = modules

        self._wqpool = None

        self.pullport = 5559

        self._masters = list()
        self._masterloads = dict()

    def start_masters(self, n=1):

        master = Master(pushport = self.pullport)
        master_proc = Process(master)
        self._master = master_proc


    def process(daxdata, func, raxdata, chunksize=42):

        self.start_masters()

        context    = zmq.Context()

        pullsocket = context.socket(zmq.PULL)
        pullsocket.bind('tcp://localhost:%s' % self.pullport)

        pushport   = pullsocket.recv()
        pushsocket = context.socket(zmq.PUSH)
        pushsocket.bind('tcp://localhost:%s' % pushport)

        for i, data in enumerate(chunk(daxdata, chunksize)):
            maxtask = Task(func, data, modules=self.modules, chunkid=i)
            pushsocket.send_pyobj(maxtask)

        while True:

            result = pullsocket.recv()
            for run, clone, gen, results in result:
                raxdata.add(run, clone, gen, results)

        raxdata.write()
        pushsocket.send_pyobj(Master.STOP)












    


def worker():

    print 'worker: starting'

    context = zmq.Context()
    sock    = context.socket(zmq.PULL)
    sock.connect('tcp://*:5559')

    while True:
        obj = sock.recv_pyobj()

        if type(obj) is bool and not obj:
            break
        # else:
        #     print 'worker: got object', type(obj), obj


def master(n):

    print 'master: starting'

    context = zmq.Context()
    sock  = context.socket(zmq.PUSH)
    sock.bind('tcp://*:5559')

    for i in xrange(n):
        sock.send_pyobj(i)
    sock.send_pyobj(False)



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
