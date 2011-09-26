

import dax, max, rax



class Func(max.Function):

    def __call__(self, path):
        pass

task = Func()

modules = max.Modules()
modules.add_modules('python/2.7.1', 'numpy', 'scipy')

data = dax.Project()
data.load()

results = rax.Project(data)


pool = max.Pool()
pool.modules = modules
pool.process(data, task, results, chunksize=42)
