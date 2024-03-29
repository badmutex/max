
import dax
import curry
import ezlog

import workqueue
import numpy as np

import os
import sys
import glob
import stat
import shutil
import inspect
import tempfile
import itertools
import cPickle as pickle

_logger = ezlog.setup(__name__)


def abspath(path):
    return os.path.expandvars(os.path.expanduser(os.path.abspath(path)))


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

    def __init__(self, func, locations, **kws):
        """
        @param func (path -> [a])
        @param locations ([Location])
        """

        if not type(func) == curry.curry:
            func = curry.curry(func)

        self.function    = func
        self.locations   = locations

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
            pickle.dump(self.function, fd)
            _logger.debug('Task: pickled user function %s to %s' % (self.function.func_name, self.infile))

        urls = itertools.imap(lambda loc: loc.url, self.locations)
        taskcmd = './%(wrapper)s %(infile)s %(proclogfile)s %(paramfiles)s >wq.log 2>&1' % {
            'wrapper' : Task.WRAPPER_NAME,
            'infile'       : self.infile,
            'proclogfile'  : self.proclogfile,
            'paramfiles'   : ' '.join(self.locations)}
        _logger.debug('Task: WQ Task command: %s' % taskcmd)

        task = workqueue.Task(taskcmd)
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
        _logger.debug('\n\t\t\t\t\t'.join(['\n'] + code))

        code = '\n'.join(code)

        with open(path, 'w') as fd:
            fd.write('#!/usr/bin/env python\n\n')
            fd.write(code)

        os.chmod(path, stat.S_IRWXU)

    def _pyworker(self):
        import dax

        import codeop, inspect

        import sys
        import os
        import shutil
        import tempfile
        import cPickle as pickle

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
                func = pickle.load(fd)
                fd_log.write('Loaded function %s\n' % func)


            for path in paramfiles:
                run, clone, gen = dax.read_cannonical(path)

                fd_log.write('Loading Location from %s\n' % path)

                with dax.Location.from_file(path) as name:

                    fd_log.write('Processing: %s as %s\n' % (path, name))

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
                            results = func(name)
                        except Exception, e:
                            fd_log.write('\tFailed to work on %s as %s: %s\n' % (path, name, e))
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

./%(work)s $@ >%(wqlogfile)s 2>&1
""" % { 'moduleshome'  : self.moduleshome,
        'load_modules' : self.modules.get_modules_script(),
        'work'         : self.workername,
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
        _logger.debug('\n\t\t\t\t\t'.join(['\n'] + executable.split('\n')))

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


    def use(self, *paths):
        sanitized = map(dax.sanitize, paths)
        self._modulefiles.append(sanitized)

    def load(self, *modules):
        self._modules.append(modules)





class Result(object):

    def __init__(self, wqtask):
        self.chunkid = int(wqtask.tag)
        self.tempdir = tempfile.mkdtemp(prefix='max-result')
        self.data = list()

        self.result = os.path.join(Task.WORKAREA, chunkname(self.chunkid))


    def load(self):

        cmd = 'tar -C %(workarea)s -xvf %(tarfile)s %(redir)s' % {
            'workarea' : self.tempdir,
            'tarfile' : self.result,
            'redir'  : '' if _logger.getEffectiveLevel() < ezlog.INFO else '>/dev/null'}
        _logger.debug('Executing: %s' % cmd)
        os.system(cmd)

        pattern = os.path.join(self.tempdir, 'RUN*/CLONE*/GEN*.dat')
        for datafile in glob.iglob(pattern):
            run, clone, gen = dax.read_cannonical(datafile)
            try:
                results         = np.loadtxt(datafile, delimiter=',', unpack=True, dtype=str)
            except IOError, e:
                r,c,g = dax.read_cannonical(datafile)
                _logger.error('Could not load data from (%d,%d,%d): %s' % (r,c,g,e))
                continue
            data            = results[-1]
            frames          = np.arange(0, len(data), step=1, dtype=int)
            self.data.append((run, clone, gen, frames, data))


    def __iter__(self):
        return iter(self.data)


    def __del__(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)



class WaitExceeded (Exception): pass


class Master(object):
    """
    Wrapper around WQ master
    """

    def __init__(self, name='max', catalog=True, exclusive=False, port=workqueue.WORK_QUEUE_RANDOM_PORT, debug=None):

        self.name      = name
        self.catalog   = catalog
        self.exclusive = exclusive
        self.port      = port

        if _logger.getEffectiveLevel() < ezlog.INFO:
            self.debug     = True
        else:
            self.debug     = False

        self._wq = None


    def start(self):

        _logger.debug('Master: Starting')

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

        _logger.debug('Master: submitting task %s' % maxtask)

        wqtask = maxtask.to_wq_task()
        _logger.debug('Master.submit: WQTask tag: %s' % wqtask.tag)

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

    def __init__(self, function, modules=Modules(), master=Master(debug='all')):

        self.function = function
        self.modules  = modules
        self.master   = master

    def process(self, daxdata, raxproject, chunksize=1):

        _logger.info('Mapping %s over %s using %s' % (self.function, daxdata, self.modules))

        data = itertools.imap(abspath, daxdata)

        _logger.info('Submitting jobs')
        for chunkid, chunk in enumerate(lazy_chunk(data, chunksize)):
            task = Task(self.function, chunk, modules=self.modules, chunkid=chunkid)
            self.master.submit(task)

        _logger.info('Recieving results')
        while not self.master.empty():
            try:
                for run,clone,gen,frames,data in self.master.recv():
                    try:
                        raxproject.add_generation(run,clone,gen,data)
                    except ValueError, e:
                        _logger.warning(str(e))
            except WaitExceeded: pass






def _test_module():
    modules = Modules()
    modules.use('~/Public/modulefiles')
    modules.load('python/2.7.1', 'numpy', 'scipy', 'ezlog/devel', 'ezpool/devel')
    modules.load('~rnowling/Public/modulefiles')
    modules.load('protomol/a', 'protomol/b')
    print modules.get_modules_script()


def _test_MyFunc(path):
    import gmx
    import numpy as np

    devnull = '/dev/null'
    rmsd = gmx.g_rms(stdout=devnull, stderr=devnull)
    return rmsd(f=path, s='/afs/crc.nd.edu/user/c/cabdulwa/Public/Research/md/ww/folded/14.pdb', n='/afs/crc.nd.edu/user/c/cabdulwa/Public/Research/md/ww/ndx/gmx/14/System.ndx')


def _test_Task():
    modules = Modules()
    modules.use('~/Public/modulefiles')
    modules.load('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel')
    data   = 'chirp://localhost/foo/RUN0001/CLONE0002/GEN0003 chirp://localhost/foo/RUN0004/CLONE0005/GEN0006'.split()
    data   = 'file:///foo/RUN0001/CLONE0002/GEN0003 file:///foo/RUN0004/CLONE0005/GEN0006'.split()
    locs   = map(dax.Local, data)
    task   = Task(_test_MyFunc, locs)
    wqtask = task.to_wq_task()


def _test_start_master():
    master = Master()
    master()

def _test_dax_read_path(path):
    import re
    re_gen = re.compile(r'%(sep)sresults-([0-9]+)' % {'sep':os.sep})
    r,c = dax.read_cannonical_traj(path)
    m = re_gen.search(path)

    if not m:
        raise ValueError, 'Cannot parse generation from %s' % path

    g = int(m.group(1))

    return r,c,g



def _test_pool():

    modules = Modules()
    modules.use('~/Public/modulefiles')
    modules.load('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel')

    daxproj = dax.Project('/tmp/test', 'lcls','fah', 10009)
    daxproj.load_file(_test_dax_read_path, 'p10009.xtclist.test2')
    data = daxproj.get_files('.+\.xtc', ignoreErrors=True)

    pool = Pool(modules=modules, name='max')
    pool.process(data, _test_MyFunc, None, chunksize=5)



def _test_wq():
    master = Master(debug='all')

    modules = Modules()
    modules.use('~/Public/modulefiles')
    modules.load('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel')

    daxproj = dax.Project('/tmp/test', 'lcls','fah', 10009)
    daxproj.load_file(_test_dax_read_path, 'p10009.xtclist.test2')
    data = daxproj.get_files('.+\.xtc', ignoreErrors=True)
    data = lazy_chunk(data, 5)

    for i, d in enumerate(data):
        task = Task(_test_MyFunc, d, modules=modules, chunkid=i)
        master.submit(task)

    print 'Getting results'
    while not master.empty():
        try:
            for result in  master.recv(1):
                print result
        except WaitExceeded: pass


def _test_marshaling():
    import marshal, types

    with open('test.pkl', 'w') as fd:
        marshal.dump(_test_MyFunc.func_code, fd)

    with open('test.pkl') as fd:
        code = marshal.load(fd)

    print code
    func = types.FunctionType(code, globals())
    print func(42)


def _test_nth_arity(x,**kws):
    y = kws.pop('y', 1)
    z = kws.pop('z', 1)
    return x * y * z



def _test_curried_marshaling():
    import cPickle as pickle
    import curry
    import functools

    f = curry.curry(_test_nth_arity, y=6)
    g = functools.partial(f, z=9)
    print f(3, z=9), g(3) # 162 162

    with open('test.pkl', 'w') as fd:
        pickle.dump(f, fd)

    with open('test.pkl') as fd:
        h = pickle.load(fd)
    k = functools.partial(h, z=9)
    print h(3, z=9), k(3) # 162 162


def _test():

    import rax
    import curry

    modules = Modules()
    modules.use('~/Public/modulefiles')
    modules.load('python/2.7.1', 'fax/devel')

    daxproj = dax.Project('test', 'lcls','fah', 10009)
    locations = dax.read_filelist('tests/p10009.xtclist.test2.chirp', kind='chirp', host='lclsstor01.crc.nd.edu', port=9987)
    locations = dax.read_filelist('tests/p10009.xtclist.test2')
    daxproj.load_locations(_test_dax_read_path, locations)
    daxproj.write_dax()

    data = daxproj.locations('*.xtc', files=True)

    raxproj = rax.Project()

    mapper = Mapper(curry.curry(_test_MyFunc), modules)
    mapper.process(data, raxproj, chunksize=5)

    raxproj.write('/tmp/raxproj')


if __name__ == '__main__':
    import rax

    ezlog.set_level(ezlog.DEBUG, __name__)
    ezlog.set_level(ezlog.INFO, dax.__name__)
    ezlog.set_level(ezlog.DEBUG, rax.__name__)
    _test()
