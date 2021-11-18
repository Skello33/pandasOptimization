import argparse
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import tasks


def run_tasks(arg: argparse.Namespace) -> list:
    """
    run the demo tasks and gather usage statistics

    :param arg: cmd line arguments
    :return: list of usage stats
    """
    functions = {
        'pandas': tasks.pandas_main,
        'modin': tasks.modin_subp,
        'multiproc': tasks.multiproc_subp,
        'dask': tasks.dask_subp
    }
    if arg.task is None:
        # start_time = ti.default_timer()
        result = [functions[func](arg) for func in functions]
        # print('It all took {} seconds.'.format(ti.default_timer() - start_time))
    else:
        result = []
        for task in arg.task:
            result.append(functions[task](arg))
        # start_time = ti.default_timer()
        # result = functions[arg.task](arg)
        # print('It all took {} seconds.'.format(ti.default_timer() - start_time))
    return result


def create_parser() -> argparse.ArgumentParser:
    """
    creates command line arguments parser

    :return: cmd arguments parser
    """
    new_parser = argparse.ArgumentParser(description='Demo pandas optimization solutions')
    new_parser.add_argument('-p', '--path', type=str, required=True, help='path to the file with dataset')
    # new_parser.add_argument('-r', '--runs', type=int, required=False, help='number of program runs')
    new_parser.add_argument('--cluster', type=str, required=False, metavar='ADDRESS',
                            help='address of the remote cluster that should be used, if not specified, program uses a '
                                 'locally created cluster')
    new_parser.add_argument('--task', type=str, required=False,
                            help='specify which tasks to execute, if not specified, all tasks will be run',
                            choices=['pandas', 'dask', 'multiproc', 'modin'], nargs='+')
    new_parser.add_argument('--file', type=str, required=False, default='usage_stats.csv',
                            help='specify the file where the program should write usage statistics from its runs, '
                                 'uses usage_stats.csv as default if not specified')
    new_parser.add_argument('--plot', required=False, help='display the usage statistics graph', action='store_true')
    return new_parser


def plot_results(file: str):
    """
    function for usage stats plotting

    :param file: plot data file
    """
    df = pd.read_csv(file)
    task_grp = df.groupby('task').mean()
    task = task_grp.index.tolist()
    # todo display only avg stats per each task
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Memory usage', 'Total time')
    )
    fig.add_trace(
        go.Bar(
            x=task,
            y=task_grp['max_mem_usage'],
            name='Memory',
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(
            x=task,
            y=task_grp['total_time'],
            name='Time'
        ),
        row=1, col=2
    )
    fig['layout']['yaxis']['title'] = 'Max memory used in KB'
    fig['layout']['yaxis2']['title'] = 'Total time in sec'
    fig.show()


def write_results(result: list, file: str):
    """
    writes usage stats from a single run into output file

    :param result: usage stats data to be written
    :param file: output file
    """
    try:
        f = open(file)
    except FileNotFoundError:
        with open(file, 'w') as f:
            print('Creating a new file for usage stats...')
            f.write('task,max_mem_usage,total_time\n')
    finally:
        f.close()
    with open(file, 'a') as f:
        # write if multiple tasks were run
        if type(result[0]) is tuple:
            for res in result:
                res = modify_stats(res)
                f.write('{}\n'.format(res))
        # write if only one task was run
        elif type(result[0]) is str:
            res = modify_stats(result)
            f.write('{}\n'.format(res))


def modify_stats(stats: list) -> str:
    """
    reformat the result into csv style

    :param stats: input line to reformat
    :return: string with a reformatted line for csv
    """
    stats = str(stats)[1:-1].split(',')
    stats[0] = stats[0].strip("'")
    stats = ','.join(stats)
    return stats


if __name__ == '__main__':

    """parse input options"""
    parser = create_parser()
    args = parser.parse_args()

    """run tasks and write the results"""
    usage_results = run_tasks(args)
    write_results(usage_results, args.file)

    if args.plot:
        plot_results(args.file)
