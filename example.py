

import dax, max, rax



def myfunction(path):
    import numpy as np
    return np.random.random(42)


modules = max.Modules()
modules.add_modules('python/2.7.1', 'numpy', 'scipy')

data = dax.Project()
data.load()

results = rax.Project(data)


pool = max.Pool()
pool.modules = modules
pool.process(data, myfunction, results, chunksize=42)
