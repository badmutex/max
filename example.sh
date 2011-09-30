#!/usr/bin/env bash

source ~cabdulwa/.bash_modules

module load ezpool/devel ezlog/devel dax/devel max/devel rax/devel gmx
module load gromacs cctools python-workqueue


time python example.py