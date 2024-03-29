
import dax, max, rax
import ezlog

import os
import re


def read_path(path):
    re_gen = re.compile(r'%(sep)sresults-([0-9]+)' % {'sep':os.sep})
    r,c = dax.read_cannonical_traj(path)
    m = re_gen.search(path)

    if not m:
        raise ValueError, 'Cannot parse generation from %s' % path

    g = int(m.group(1))

    return r,c,g



def rmsd(path):
    import gmx
    import os

    prefix = '/afs/crc.nd.edu/user/c/cabdulwa/fax.git/max/tests'

    struct = os.path.join(prefix, 'protein.pdb')
    ndx    = os.path.join(prefix, 'System.ndx')

    devnull = '/dev/null'
    fn = gmx.g_rms(stdout=devnull, stderr=devnull)
    return fn(f=path, s=struct, n=ndx)



modules = max.Modules()
modules.use('~cabdulwa/Public/modulefiles')
modules.load('python/2.7.1', 'numpy', 'ezlog/devel', 'ezpool/devel', 'dax/devel', 'gromacs', 'gmx')

daxproj = dax.Project('/tmp/test', 'lcls', 'fah', 10009)
locations = dax.read_filelist('tests/p10009.xtclist.test2.chirp',
                              kind='chirp',
                              host='lclsstor01.crc.nd.edu',
                              port=9987)
daxproj.load_locations(read_path, locations)
daxproj.write_dax()
data = daxproj.locations('*.xtc', files=True)

raxproj = rax.Project()

mapper = max.Mapper(rmsd, modules=modules)
mapper.process(data, raxproj, chunksize=5)

raxproj.write('/tmp/raxproj')
