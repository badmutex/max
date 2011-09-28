
import dax
import ezlog

import zmq
import workqueue
import numpy as np

import os
import sys
import glob
import stat
import shutil
import inspect
import marshal
import tempfile
import itertools
import multiprocessing
import cPickle as pickle

_logger = ezlog.setup(__name__)

def lazy_chunk(iterable, chunksize):
    if chunksize <= 0:
        chunksize = 1

    buf = []
    for val in iterable:
        buf.append(val)
        if len(buf) >= chunksize:
            yield buf
            buf = []
    if buf:
        yield buf

def chunkname(chunkid):
    return 'chunk-%04d-tar.bz2' % chunkid



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


    def to_wq_task(self):

        self.write_wrapper(Task.WRAPPER_NAME)
        self.write_worker(Task.WORKER_NAME)

        with open(self.infile, 'w') as fd:
            marshal.dump(self.function.func_code, fd)
            _logger.debug('Task: marsheled user function %s to %s' % (self.function.func_name, self.infile))

        task = workqueue.Task('./%(wrapper)s >wq.log' % {'wrapper' : Task.WRAPPER_NAME})
        task.tag = str(self.chunkid)

        task.specify_input_file(Task.WRAPPER_NAME, Task.WRAPPER_NAME)
        task.specify_input_file(Task.WORKER_NAME, Task.WORKER_NAME)
        task.specify_input_file(Task.IN_NAME, Task.IN_NAME)

        if not os.path.exists(Task.WORKAREA):
            os.makedirs(Task.WORKAREA)

        task.specify_output_file(os.path.join(Task.WORKAREA, chunkname(self.chunkid)), Task.WORKER_RESULTS)
        for outfile in Task.WORKER_RETURN:
            local = os.path.join(Task.WORKAREA, 'chunk-%04d-%s' % (self.chunkid, outfile))
            task.specify_output_file(local, outfile)

        return task
        


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

        import codeop, marshal, types, inspect

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
                func = types.FunctionType(code, globals(), 'userfunc')
                fd_log.write('Loaded function\n')



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
                    except Exception, e:
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

        langs = {'bash' : self._bash}

        if kind not in langs:
            raise ValueError, 'Unknown kind %s. Try one of %s' % (kind, ' '.join(langs.keys()))

        lang       = langs[kind]
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
        self.chunkid = int(wqtask.tag)
        self.tempdir = tempfile.mkdtemp(prefix='max-result')
        self.data = list()

        self.result = os.path.join(Task.WORKAREA, chunkname(self.chunkid))


    def load(self):

        cmd = 'tar -C %(workarea)s -xvf %(tarfile)s' % {
            'workarea' : self.tempdir,
            'tarfile' : self.result }
        print 'Executing:', cmd
        os.system(cmd)

        pattern = os.path.join(self.tempdir, 'RUN*/CLONE*/GEN*.dat')
        for datafile in glob.iglob(pattern):
            run, clone, gen = dax.read_cannonical(datafile)
            results         = np.loadtxt(datafile, delimiter=',', unpack=True, dtype=str)
            data            = results[-1]
            frames          = np.arange(0, len(data), step=1, dtype=int)
            self.data.append((run, clone, gen, frames, data))


    def __iter__(self):
        return iter(self.data)


    def __del__(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)



class WaitExceeded (Exception): pass


class PyMaster(object):
    """
    Wrapper around WQ master
    """

    def __init__(self, name='max', catalog=True, exclusive=False, port=workqueue.WORK_QUEUE_RANDOM_PORT, debug=None):

        self.name      = name
        self.catalog   = catalog
        self.exclusive = exclusive
        self.port      = port
        self.debug     = debug

        self._wq = None


    def start(self):

        _logger.debug('PyMaster: Starting')

        wq = workqueue.WorkQueue(port=self.port,
                                 name=self.name,
                                 catalog=self.catalog,
                                 exclusive=self.exclusive)

        if type(self.debug) is str and self.debug:
            workqueue.set_debug_flag(self.debug)

        self._wq = wq

    def submit(self, maxtask):
        if self._wq is None:
            self.start()

        _logger.debug('PyMaster: submitting task %s' % maxtask)

        wqtask = maxtask.to_wq_task()
        _logger.debug('PyMaster.submit: WQTask tag: %s' % wqtask.tag)

        self._wq.submit(wqtask)


    def empty(self):
        return self._wq.empty()

    def recv(self, block=True, wait=9999):

        def get(w=wait): return self._wq.wait(w)

        if block:
            while not self._wq.empty():
                wqtask = get(w=1)
                if wqtask: break
        else:
            wqtask = get()

        if wqtask:
            result = Result(wqtask)
            result.load()
            return result
        else:
            raise WaitExceeded, 'Did wait time %s exceeded' % wait




class Mapper(object):

    def __init__(self, function, modules=Modules(), master=PyMaster(debug='all')):

        self.function = function
        self.modules  = modules
        self.master   = master

    def process(self, daxdata, raxproject, chunksize=1):

        for chunkid, chunk in enumerate(lazy_chunk(daxdata, chunksize)):
            task = Task(self.function, chunk, modules=self.modules, chunkid=chunkid)
            self.master.submit(task)

        ## TODO: put results into R@X Project
        while not self.master.empty():
            try:
                for result in self.master.recv():
                    print result
            except WaitExceeded: pass




    


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

def test_dax_read_path(path):
    import re
    re_gen = re.compile(r'%(sep)sresults-([0-9]+)' % {'sep':os.sep})
    r,c = dax.read_cannonical_traj(path)
    m = re_gen.search(path)

    if not m:
        raise ValueError, 'Cannot parse generation from %s' % path

    g = int(m.group(1))

    return r,c,g



def test_pool():

    modules = Modules()
    modules.add_modulefiles('~/Public/modulefiles')
    modules.add_modules('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel')

    daxproj = dax.Project('/tmp/test', 'lcls','fah', 10009)
    daxproj.load_file(test_dax_read_path, 'p10009.xtclist.test2')
    data = daxproj.get_files('.+\.xtc', ignoreErrors=True)

    pool = Pool(modules=modules, name='max')
    pool.process(data, MyFunc, None, chunksize=5)



def test_wq():
    master = PyMaster(debug='all')

    modules = Modules()
    modules.add_modulefiles('~/Public/modulefiles')
    modules.add_modules('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel')

    daxproj = dax.Project('/tmp/test', 'lcls','fah', 10009)
    daxproj.load_file(test_dax_read_path, 'p10009.xtclist.test2')
    data = daxproj.get_files('.+\.xtc', ignoreErrors=True)
    data = lazy_chunk(data, 5)

    for i, d in enumerate(data):
        task = Task(MyFunc, d, modules=modules, chunkid=i)
        master.submit(task)

    print 'Getting results'
    while not master.empty():
        try:
            for result in  master.recv(1):
                print result
        except WaitExceeded: pass


def test_marshaling():
    import marshal, types

    with open('test.pkl', 'w') as fd:
        marshal.dump(MyFunc.func_code, fd)

    with open('test.pkl') as fd:
        code = marshal.load(fd)

    print code
    func = types.FunctionType(code, globals())
    print func(42)


def test():

    modules = Modules()
    modules.add_modulefiles('~/Public/modulefiles')
    modules.add_modules('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel')

    daxproj = dax.Project('/tmp/test', 'lcls','fah', 10009)
    daxproj.load_file(test_dax_read_path, 'p10009.xtclist.test2')
    data = daxproj.get_files('.+\.xtc', ignoreErrors=True)

    mapper = Mapper(MyFunc, modules)
    mapper.process(data, None, chunksize=5)


if __name__ == '__main__':
    ezlog.set_level(ezlog.DEBUG, __name__)
    ezlog.set_level(ezlog.INFO, dax.__name__)
    test()

