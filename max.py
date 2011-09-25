
import ezlog
import dax, rax


import sys



if __name__ == '__main__':

    daxdata  = sys.argv[1]
    mapperfn = sys.argv[2]
    raxdata  = sys.argv[3]


    setup_mapper(mapperfn)
    from locale_max_mapper import mapper as max_mapper


    daxproj = dax.Project()
    daxproj.load_dax(daxdata)

    master = Master()
    master.start(max_mapper, daxproj)

    raxproj = rax.Project()
    raxproj.set_prefix(raxdata)

    while True:

        if master.isfinished():
            break

        result = master.recv()
        raxproj.add_max(result)



    raxproj.write_rax()
