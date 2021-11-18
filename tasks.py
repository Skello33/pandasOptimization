import gc
import resource
import sys
from argparse import Namespace

import numpy as np
import pandas as pd
import modin.pandas as mpd
from resource import *
from subprocess import run

from distributed import Client, LocalCluster, performance_report
import dask.dataframe as dd
from dask import compute
from typing import Tuple
import timeit as ti
from multiprocessing import Pool, Process, Queue
from dask_memusage import install
from glob import glob


def set_usage() -> int:
    return RUSAGE_SELF
    # todo try to understand this and how to monitor the memory usage properly


dtype = {'ActualElapsedTime': 'float64',
         'ArrDelay': 'float64',
         'ArrTime': 'float64',
         'DepDelay': 'float64',
         'DepTime': 'float64',
         'Distance': 'float64',
         'CRSElapsedTime': 'float64',
         'CancellationCode': 'object',
         'TailNum': 'object',
         'AirTime': 'float64',
         'TaxiIn': 'float64',
         'TaxiOut': 'float64',
         'CRSDepTime': 'string'
         }

cols = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'DepDelay', 'CRSArrTime', 'ArrDelay', 'Origin',
        'Dest']


def format_usage(usage: resource.struct_rusage) -> int:
    """
    reformat usage statistics data

    :param usage: resource usage data
    :return: reformatted resource usage data
    """
    return usage.ru_maxrss


def pandas_main(args: Namespace) -> Tuple[str, int, float]:
    print('PANDAS started...')
    files = glob(args.path)
    start_time = ti.default_timer()
    if len(files) == 1:
        queue = Queue()
        p = Process(target=pandas_single, args=(args.path, queue), name='pandas')
        p.start()
        p.join()
        output = queue.get()
        # pandas_single(args.path)
    elif len(files) > 1:
        pandas_more(files)
    else:
        raise Exception('Something is wrong!')
    # output = format_usage(getrusage(set_usage()))
    duration = ti.default_timer() - start_time
    return 'pandas', output, duration


def pandas_single(file: str, queue):
    df = pd.read_csv(file, dtype=dtype, usecols=cols)
    result = df['DepDelay'].mean()
    print('Dep avg is {}'.format(result))
    output = format_usage(getrusage(set_usage()))
    queue.put(output)
    df.head()


def pandas_more(files: list):
    for file in files:
        df = pd.read_csv(file, dtype=dtype, usecols=cols)
        df.head()


def dask_subp(args: Namespace):
    queue = Queue()
    p = Process(target=dask_main, args=(args, queue), name='dask')
    start_time = ti.default_timer()
    p.start()
    p.join()
    output = queue.get()

    duration = ti.default_timer() - start_time
    return 'dask', output, duration


def dask_main(args: Namespace, queue) -> dict:
    if args.cluster is None:
        client = Client()
    else:
        client = Client(args.cluster)

    # start_time = ti.default_timer()
    print('DASK started...')
    dask_task(args.path)
    output = format_usage(getrusage(set_usage()))
    queue.put(output)

    # duration = ti.default_timer() - start_time
    if args.cluster is None:
        client.close()
    # return {'dask': (output, duration)}


def dask_task(files: str):
    df = dd.read_csv(files, dtype=dtype, usecols=cols)
    result = df['DepDelay'].mean().compute()
    df.head()
    # TODO add other tasks
    print('Dep avg is {}'.format(result))


def multiproc_subp(args: Namespace) -> Tuple[str, int, float]:
    queue = Queue()
    start_time = ti.default_timer()
    print('MULTIPROC started...')
    p = Process(target=multiproc_main, args=(args, queue), name='modin')
    start_time = ti.default_timer()
    p.start()
    p.join()
    output = queue.get()
    # output = format_usage(getrusage(set_usage()))
    duration = ti.default_timer() - start_time
    return 'multiproc', output, duration


def multiproc_main(args: Namespace, queue):
    num_cores = 4
    df = pd.read_csv(args.path, dtype=dtype, usecols=cols)
    df_split = np.array_split(df, num_cores)
    with Pool(num_cores) as pool:
        output = pool.map(multiproc_task, df_split)
        del_sum, del_cnt = 0, 0
        for x, y in output:
            del_sum += x
            del_cnt += y
        print('Dep avg is {}'.format(del_sum / del_cnt))
    usage = format_usage(getrusage(set_usage()))
    queue.put(usage)


def multiproc_task(df: pd.DataFrame):
    del_sum = df['DepDelay'].sum()
    del_cnt = df['DepDelay'].count()
    return del_sum, del_cnt


def modin_subp(args: Namespace):
    queue = Queue()
    p = Process(target=modin_main, args=(args, queue), name='modin')
    start_time = ti.default_timer()
    p.start()
    p.join()
    output = queue.get()
    duration = ti.default_timer() - start_time
    return 'modin', output, duration


def modin_main(args: Namespace, queue: Queue):
    files = glob(args.path)
    if args.cluster is None:
        client = Client()
        # install(client.cluster.scheduler, 'memusage.csv')
    else:
        client = Client(args.cluster)
    # start_time = ti.default_timer()
    print('MODIN started...')
    if len(files) == 1:
        modin_single(args.path)
    elif len(files) > 1:
        modin_more(files)
    output = format_usage(getrusage(set_usage()))
    queue.put(output)
    # duration = ti.default_timer() - start_time
    if args.cluster is None:
        client.close()
    # return {'modin': (output, duration)}


def modin_single(file: str):
    df = mpd.read_csv(file, dtype=dtype, usecols=cols)
    df.head()
    result = df['DepDelay'].mean()
    print('Dep avg is {}'.format(result))


def modin_more(files: list):
    pass
