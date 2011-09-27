
import dax
import ezlog

import zmq
import workqueue

import os
import stat
import sys
import inspect
import marshal
import itertools
import multiprocessing
import cPickle as pickle

_logger = ezlog.setup(__name__)

PORT_MASTER = 5678
PORT_POOL   = 5689

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

        self.function    = func
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

        with open(Task.IN_NAME, 'w') as fd:
            marshal.dump(self.function.func_code, fd)

        task = workqueue.Task('./%(wrapper)s >wq.log' % {'wrapper' : Task.WRAPPER_NAME})
        task.specify_input_file(Task.WRAPPER_NAME, Task.WRAPPER_NAME)
        task.specify_input_file(Task.WORKER_NAME, Task.WORKER_NAME)
        task.specify_input_file(Task.IN_NAME, Task.IN_NAME)

        if not os.path.exists(Task.WORKAREA):
            os.makedirs(Task.WORKAREA)

        task.specify_output_file(os.path.join(Task.WORKAREA, Task.chunkname(self.chunkid)), Task.WORKER_RESULTS)
        for outfile in Task.WORKER_RETURN:
            local = os.path.join(Task.WORKAREA, 'chunk-%04d-%s' % (self.chunkid, outfile), outfile)
            task.specify_output_file(local, outfile)

        return task
        


    @classmethod
    def chunkname(cls, chunkid):
        return 'chunk-%04d-tar.bz2' % chunkid


    def write_worker(self, path):

        code = inspect.getsource(self._pyworker).split('\n')

        # remove the def ...
        code = code[1:]

        # shift everything over by 2 tabs
        code = map(lambda s: s.replace(' ','', 8), code)

        _logger.debug('Task.write_worker: code:')
        for line in code:
            _logger.debug('\t' + line)

        code = '\n'.join(code)

        with open(path, 'w') as fd:
            fd.write('#!/usr/bin/env python\n\n')
            fd.write(code)

        os.chmod(path, stat.S_IRWXU)

    def _pyworker(self):
        import dax

        import marshal, types, inspect

        import sys
        import os
        import shutil
        import tempfile

        RESULTS_NAME = 'results.tar.bz2'

        EXITCODE = 0

        infile     = sys.argv[1]
        outfile    = sys.argv[2]
        paramfiles = sys.argv[3:]

        tempdir = tempfile.mkdtemp(prefix='max-wa')

        with open(outfile, 'w') as fd_log:

            fd_log.write('Using python version %s\n' % ' '.join(sys.version.split('\n')))

            fd_log.write('Loading function and arguments\n')
            with open(infile) as fd:
                code = marshal.load(fd)
                func = types.FunctionType(code, locals(), 'userfunc')

                fd_log.write('Loaded function\n')
                fd_log.write('\tType: %s\n' % type(code))
                fd_log.write('\tCode: %s\n' % code)
                fd_log.write('\tFunction: %s\n' % func)
                fd_log.write('\tSourcecode: %s\n' % '\n\t\t'.join(inspect.getsource(func).split('\n')))


            for path in paramfiles:

                fd_log.write('Processing: %s\n' % path)

                run, clone, gen = dax.read_cannonical(path)

                fd_log.write('\tRUN %d CLONE %d GEN %d\n' % (run,clone,gen))

                workarea        = dax.cannonical_traj(run, clone)
                workarea        = os.path.join(tempdir, workarea)
                target_name     = 'GEN%04d.dat' % gen
                target          = os.path.join(workarea, target_name)

                if not os.path.exists(workarea):
                    fd_log.write('\tCreating workarea %s\n' % workarea)
                    os.makedirs(workarea)

                with open(target, 'w') as fd:

                    fd_log.write('\t Applying %s\n' % func)

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
            os.chdir(tempdir)

            fd_log.write('In %s, contents: %s\n' % (tempdir, os.listdir('.')))

            compress = 'tar cvf %s RUN*' % RESULTS_NAME

            fd_log.write('Compressing results: %s\n' % compress)
            fd_log.write('Results file: %s\n' % os.path.join(tempdir, RESULTS_NAME))

            compress_success = os.system(compress)
            if compress_success == 0:
                fd_log.write('\tOK\n')
            else:
                fd_log.write('\tFail: %s\n' % compress_success)

            os.chdir(cwd)

            fd_log.write('Copying results file %s to WQ Worker workarea %s\n' % (RESULTS_NAME, cwd))
            shutil.copyfile(os.path.join(tempdir, RESULTS_NAME),
                            os.path.join(cwd, RESULTS_NAME))

            fd_log.write('Finished. Exiting with %s\n' % EXITCODE)

        shutil.rmtree(tempdir, ignore_errors=True)
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

        return bash


    def write_wrapper(self, outfile, kind='bash'):
        if kind not in self._langs:
            raise ValueError, 'Unknown kind %s. Try one of %s' % (kind, ' '.join(self._langs.keys()))

        lang       = self._langs[kind]
        executable = lang()

        _logger.debug('Task.write_wrapper: executable:')
        for line in executable.split('\n'):
            _logger.debug('\t' + line)

        with open(outfile, 'w') as fd:
            fd.write(executable)

        os.chmod(outfile, stat.S_IRWXU)


class Modules(object):

    def __init__(self, modulefiles = list(), modules = list()):

        self._modulefiles = modulefiles
        self._modules     = modules


    def get_modules_script(self):

        def get_str():

            for modulefilesgroup, modulesgroup in itertools.izip_longest(self._modulefiles, self._modules, fillvalue=None):

                if modulefilesgroup is not None:
                    for modulefile in modulefilesgroup:
                        yield 'module use %s' % modulefile

                if modulesgroup is not None:
                    for module in modulesgroup:
                        yield 'module load %s' % module

        return '\n'.join(get_str())


    def add_modulefiles(self, *paths):
        sanitized = map(dax.sanitize, paths)
        self._modulefiles.append(sanitized)

    def add_modules(self, *modules):
        self._modules.append(modules)





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
        




class Master(object):

    STOP = True

    def __init__(self, pushport=5559):

        self.pushport  = pushport
        self._protocol = 'tcp'
        self._address   = '127.0.0.1'


    def address(self, port=None):
        # _logger.debug('Master.address: protocol=%s address=%s port=%s' % (self._protocol, self._address, port))

        if port is None: portstr = ''
        else:            portstr = ':%d' % port

        return '%(protocol)s://%(address)s%(port)s' % {
            'protocol' : self._protocol,
            'address' : self._address,
            'port' : portstr }

    def input_files(self, *paths):
        raise NotImplementedError

    def output_files(self, *paths):
        raise NotImplementedError


    def __call__(self):
        _logger.info('Master: starting')

        context   = zmq.Context()

        insocket  = context.socket(zmq.PULL)
        outsocket = context.socket(zmq.PUSH)

        inport    = PORT_POOL
        insocket.bind(self.address(inport))
        # inport    = insocket.bind_to_random_port(self.address())
        outsocket.connect(self.address(self.pushport))
        # outsocket.bind(self.address(PORT_MASTER))

        WQ        = workqueue.WorkQueue(workqueue.WORK_QUEUE_RANDOM_PORT)

        _logger.info('Master: PULLing from %s' % self.address(inport))
        _logger.info('Master: PUSHing to %s' % self.address(self.pushport))

        _logger.info('Master: Sending my listening port %d to the pool' % inport)
        outsocket.send_pyobj(inport, zmq.NOBLOCK)

        while True:

            _logger.debug('Master: Getting task')
            task = insocket.recv_pyobj()
            _logger.debug('Master: Got task %s' % task)

            if task == Master.STOP:
                _logger.info('Master: Recieved STOP')
                break

            elif task:
                _logger.debug('Master: Received Task %s' % type(task))
                task.materialize()
                wqtask = task.to_wq_task()

                WQ.submit(wqtask)

            else:

                _logger.info('Master: No new work, pulling from WQ')

                while not WQ.empty():
                    result_task = WQ.wait()
                    result      = Result(result_task)
                    result.load()
                    outsocket.send_pyobj(result)

        _logger.info('Master: finishing up')
        insocket.close()
        outsocket.close()


class Pool(object):


    def __init__(self, modules=Modules()):

        self.modules = modules

        self._wqpool = None

        self.pullport = 5559

        self._masters = list()
        self._masterloads = dict()

    def start_masters(self, n=1):

        _logger.debug('Pool.start_masters: starting %d masters pushing to %d' % (n, self.pullport))

        master = Master(pushport = self.pullport)
        master_proc = multiprocessing.Process(target=master)
        master_proc.start()

        _logger.debug('Pool.start_masters: OK')
        self._master = master_proc


    def process(self, daxdata, func, raxdata, chunksize=42):

        self.start_masters()

        _logger.info('Pool: Mapping %s -> %s using %s with chunksize=%d' % (daxdata, raxdata, func, chunksize))

        context    = zmq.Context()

        pullsocket = context.socket(zmq.PULL)
        pullsocket.connect('tcp://127.0.0.1:%s' % self.pullport)
        _logger.info('Pool: pulling from %d' % self.pullport)

        _logger.debug('Pool.process: pulling ports to push to')
        # pushport   = pullsocket.recv_pyobj()
        pushport = PORT_POOL
        _logger.debug('Pool.process: got pushport=%d' % pushport)

        pushsocket = context.socket(zmq.PUSH)
        pushsocket.connect('tcp://127.0.0.1:%s' % pushport)
        _logger.info('Pool: pushing to %d' % pushport)

        # for i, data in enumerate(chunk(daxdata, chunksize)):
        #     maxtask = Task(func, data, modules=self.modules, chunkid=i)
        #     pushsocket.send_pyobj(maxtask)

        # while True:

        #     result = pullsocket.recv_pyobj()
        #     for run, clone, gen, results in result:
        #         raxdata.add(run, clone, gen, results)

        # raxdata.write()

        _logger.info('Pool: sending STOP message to Masters')
        pushsocket.send_pyobj(Master.STOP)
        pullsocket.close()
        pushsocket.close()

        _logger.info('Pool: stopping Master(s)')
        self._master.join(10)
        self._master.terminate()

        _logger.info('Pool: Done')












    


def worker():

    print 'worker: starting'

    context = zmq.Context()
    sock    = context.socket(zmq.PULL)
    sock.connect('tcp://*:%d' % PORT_MASTER)

    outsock = context.socket(zmq.PUSH)
    outsock.bind('tcp://*:%d' % PORT_POOL)

    while True:
        obj = sock.recv_pyobj()

        if type(obj) is bool and not obj:
            break
        # else:
        #     print 'worker: got object', type(obj), obj

    outsock.send_pyobj('OK')


def master(n):

    print 'master: starting'

    context = zmq.Context()
    sock  = context.socket(zmq.PUSH)
    sock.bind('tcp://*:%d' % PORT_MASTER)

    insock = context.socket(zmq.PULL)
    insock.connect('tcp://*:%d' % PORT_POOL)

    for i in xrange(n):
        sock.send_pyobj(i)
    sock.send_pyobj(False)
    res = insock.recv_pyobj()
    print 'Master got', res



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


def test_module():
    modules = Modules()
    modules.add_modulefiles('~/Public/modulefiles')
    modules.add_modules('python/2.7.1', 'numpy', 'scipy', 'ezlog/deve', 'ezpool/devel')
    modules.add_modulefiles('~rnowling/Public/modulefiles')
    modules.add_modules('protomol/a', 'protomol/b')
    print modules.get_modules_script()


def MyFunc(path):
        import numpy as np
        return np.random.random_sample(42)


def test_Task():
    modules = Modules()
    modules.add_modulefiles('~/Public/modulefiles')
    modules.add_modules('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel')
    data   = 'foo/RUN0001/CLONE0002/GEN0003 foo/RUN0004/CLONE0005/GEN0006'.split()
    task   = Task(MyFunc, data)
    wqtask = task.to_wq_task()


def test_start_master():
    master = Master()
    master()


def test():

    modules = Modules()
    modules.add_modulefiles('~/Public/modulefiles')
    modules.add_modules('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel')

    def read_path(path):
        import re
        re_gen = re.compile(r'%(sep)sresults-([0-9]+)' % {'sep':os.sep})
        r,c = dax.read_cannonical_traj(path)
        m = re_gen.search(path)

        if not m:
            raise ValueError, 'Cannot parse generation from %s' % path

        g = int(m.group(1))

        return r,c,g

    daxproj = dax.Project('/tmp/test', 'lcls','fah', 10009)
    daxproj.load_file(read_path, 'p10009.xtclist.test')

    pool = Pool(modules=modules)
    pool.process(daxproj, MyFunc, None)



if __name__ == '__main__':
    ezlog.set_level(ezlog.DEBUG, __name__)
    test()


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
