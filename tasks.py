import multiprocessing
import resource
from argparse import Namespace

import dask
import numpy as np
import pandas as pd
import modin.pandas as mpd
from resource import getrusage, RUSAGE_SELF

from distributed import Client
import dask.dataframe as dd
from dask_memusage import install
from dask.distributed import performance_report

from typing import Tuple

import timeit as ti
from multiprocessing import Pool, Process, Queue
from glob import glob


def set_usage() -> int:
    """Sets the parameter for getrusage function.

    :return: Parameter for getrusage.
    """
    return RUSAGE_SELF


_dtype = {'ActualElapsedTime': 'float64',
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
    """Reformat usage statistics data.

    :param usage: Resource usage data.
    :return: Reformatted resource usage data.
    """

    return usage.ru_maxrss


def pandas_main(args: Namespace) -> Tuple[str, int, float]:
    """Runs the pandas task in a subprocess and monitors resource usage.

    :param args: Parsed command line arguments.
    :return: Usage statistics from pandas task.
    """

    print(u'PANDAS started...')
    files = glob(args.path)
    start_time = ti.default_timer()
    if len(files) == 1:
        queue = Queue()
        p = Process(target=pandas_single, args=(files[0], queue), name=u'pandas_single_file')
        p.start()
        p.join()
        mem_usage = queue.get()
    elif len(files) > 1:
        queue = Queue()
        p = Process(target=pandas_more, args=(files, queue), name=u'pandas_more_files')
        p.start()
        p.join()
        mem_usage = queue.get()
    else:
        raise FileNotFoundError(u'Something is wrong with the files!')
    duration = ti.default_timer() - start_time
    return 'pandas', mem_usage, duration


def pandas_single(file: str, queue: multiprocessing.Queue):
    """Execute the pandas task on a single data file.

    :param file: Path to the file containing task data.
    :param queue: Queue for subprocess data storing.
    """

    df = pd.read_csv(file, dtype=_dtype, usecols=cols)
    result = df['DepDelay'].mean()
    print(u'Dep avg is {}'.format(result))
    mem_usage = format_usage(getrusage(set_usage()))
    queue.put(mem_usage)
    df.head()


def pandas_more(files: list, queue: multiprocessing.Queue):
    """Executes the pandas task on multiple data files.

    :param files: Path to the data files.
    :param queue: Queue for subprocess data storing.
    """

    sums, counts = [], []
    for file in files:
        df = pd.read_csv(file, dtype=_dtype, usecols=cols)
        sums.append(df['DepDelay'].sum())
        counts.append(df['DepDelay'].count())
    print(u'Dep avg is {}'.format(sum(sums) / sum(counts)))
    mem_usage = format_usage(getrusage(set_usage()))
    queue.put(mem_usage)


def dask_subp(args: Namespace) -> Tuple[str, int, float]:
    """Runs the dask task in a subprocess.

    :param args: Parsed command line arguments.
    :return: Usage statistics from dask task.
    """

    queue = Queue()
    p = Process(target=dask_main, args=(args, queue), name='dask')
    p.start()
    p.join()

    # get usage data from subprocess
    mem_usage = queue.get()
    duration = queue.get()
    return 'dask', mem_usage, duration


def dask_main(args: Namespace, queue: multiprocessing.Queue):
    """Executes the dask task on the cluster.

    :param args: Parsed command line arguments.
    :param queue: Queue for subprocess data storing.
    """

    # cluster start/bind
    if args.cluster is None:
        client = Client()
    else:
        client = Client(args.cluster)

    print('DASK started...')
    start_time = ti.default_timer()
    # print(client.dashboard_link)

    # with performance_report('dask_report.html'):
    #     dask_task(args.path)

    dask_task(args.path)

    # gather usage data
    mem_usage = format_usage(getrusage(set_usage()))
    duration = ti.default_timer() - start_time
    queue.put(mem_usage)
    queue.put(duration)

    # cluster close if it was locally started
    if args.cluster is None:
        client.close()


def dask_task(files: str):
    """Runs dask tasks on the specified data file.

    :param files: Path to the data file(s) for task.
    """

    df = dd.read_csv(files, dtype=_dtype, usecols=cols)
    result = df['DepDelay'].mean().compute()
    df.head()
    print('Dep avg is {}'.format(result))


def multiproc_subp(args: Namespace) -> Tuple[str, int, float]:
    """Runs the multiprocessing task in a subprocess.

    :param args: Parsed command line arguments.
    :return: Usage statistics from multiprocessing task.
    """

    queue = Queue()
    print('MULTIPROC started...')

    p = Process(target=multiproc_main, args=(args, queue), name='modin')
    start_time = ti.default_timer()
    p.start()
    p.join()

    mem_usage = queue.get()
    duration = ti.default_timer() - start_time
    return 'multiproc', mem_usage, duration


def multiproc_main(args: Namespace, queue: multiprocessing.Queue):
    """Executes the multiprocessing task over the Pool of processes.

    :param args: Parsed command line arguments.
    :param queue: Queue for subprocess data storing.
    """

    num_cores = 4
    df = pd.read_csv(args.path, dtype=_dtype, usecols=cols)
    df_split = np.array_split(df, num_cores)
    with Pool(num_cores) as pool:
        output = pool.map(multiproc_task, df_split)
        del_sum, del_cnt = 0, 0
        for x, y in output:
            del_sum += x
            del_cnt += y
        print('Dep avg is {}'.format(del_sum / del_cnt))
    mem_usage = format_usage(getrusage(set_usage()))
    queue.put(mem_usage)


def multiproc_task(df: pd.DataFrame) -> Tuple[int, int]:
    """Runs multiprocessing task on the specified part of the DataFrame.

    :param df: Part of the DataFrame.
    :return: Tuple of intermediate task results.
    """

    del_sum = df['DepDelay'].sum()
    del_cnt = df['DepDelay'].count()
    return del_sum, del_cnt


def modin_subp(args: Namespace) -> Tuple[str, int, float]:
    """Runs the modin task in a subprocess.

    :param args: Parsed command line arguments.
    :return: Usage statistics from modin task.
    """

    queue = Queue()
    p = Process(target=modin_main, args=(args, queue), name='modin')
    # start_time = ti.default_timer()
    p.start()
    p.join()

    # get usage data from subprocess
    mem_usage = queue.get()
    duration = queue.get()
    # duration = ti.default_timer() - start_time
    return 'modin', mem_usage, duration


def modin_main(args: Namespace, queue: multiprocessing.Queue):
    """Executes the modin task on the cluster.

    :param args: Parsed command line arguments.
    :param queue: Queue for subprocess data storing.
    """

    # cluster start/bind
    files = glob(args.path)
    if args.cluster is None:
        client = Client()
    else:
        client = Client(args.cluster)

    start_time = ti.default_timer()
    print('MODIN started...')
    if len(files) == 1:
        modin_single(files[0])
    elif len(files) > 1:
        modin_more(files)

    # gather usage data
    mem_usage = format_usage(getrusage(set_usage()))
    duration = ti.default_timer() - start_time
    queue.put(mem_usage)
    queue.put(duration)

    # cluster close if it was locally started
    if args.cluster is None:
        client.close()


def modin_single(file: str):
    """Runs modin task on the specified data file.

    :param file: Path to the data file(s) for task.
    """

    df = mpd.read_csv(file, dtype=_dtype, usecols=cols)
    df.head()
    result = df['DepDelay'].mean()
    print('Dep avg is {}'.format(result))


def modin_more(files: list):
    """Executes the modin task on multiple data files.

    :param files: Path to the data files.
    """

    sums, counts = [], []
    for file in files:
        df = mpd.read_csv(file, dtype=_dtype, usecols=cols)
        sums.append(df['DepDelay'].sum())
        counts.append(df['DepDelay'].count())
    print(u'Dep avg is {}'.format(sum(sums) / sum(counts)))
