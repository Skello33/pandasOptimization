import argparse
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import tasks


def run_tasks(arg: argparse.Namespace) -> list:
    """Runs the demo tasks and gathers resource usage statistics.

    :param arg: Parsed arguments from the command line.
    :return: List of gathered usage statistics.
    """

    functions = {
        u'pandas': tasks.pandas_main,
        u'modin': tasks.modin_subp,
        u'multiproc': tasks.multiproc_subp,
        u'dask': tasks.dask_subp
    }
    if arg.task is None:
        result = [functions[func](arg) for func in functions]
    else:
        result = []
        for task in arg.task:
            result.append(functions[task](arg))
    return result


def create_parser() -> argparse.ArgumentParser:
    """Creates command line arguments parser.

    :return: Parser for command line arguments.
    """

    new_parser = argparse.ArgumentParser(
        description=u'Demo pandas optimization solutions.'
    )
    new_parser.add_argument(
        u'-p', u'--path',
        type=str,
        required=True,
        help=u'Path to the file with dataset.'
    )
    new_parser.add_argument(
        u'--cluster',
        type=str,
        required=False,
        metavar=u'ADDRESS',
        help=u'Address of the remote cluster that should be used, if not specified, program uses a '
             u'locally created cluster.'
    )
    new_parser.add_argument(
        u'--task',
        type=str,
        required=False,
        choices=[u'pandas', u'dask', u'multiproc', u'modin'],
        nargs=u'+',
        help=u'Specify which tasks to execute, if not specified, all tasks will be run.'
    )
    new_parser.add_argument(
        u'--file',
        type=str,
        required=False,
        default=u'usage_stats.csv',
        help=u'Specify the file where the program should write usage statistics from its runs, '
             u'uses usage_stats.csv as default if not specified.'
    )
    new_parser.add_argument(
        u'--plot',
        required=False,
        action=u'store_true',
        help=u'Display the usage statistics graph.'
    )
    return new_parser


def plot_results(file: str):
    """Displays usage statistics plot based on the data in file.

    :param file: Data for usage stats plotting.
    """

    df = pd.read_csv(file)
    task_grp = df.groupby('task').mean()
    task = task_grp.index.tolist()
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(u'Memory usage', u'Total time')
    )
    fig.add_trace(
        go.Bar(
            x=task,
            y=task_grp['max_mem_usage'],
            name=u'Memory',
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(
            x=task,
            y=task_grp['total_time'],
            name=u'Time'
        ),
        row=1, col=2
    )
    fig['layout']['yaxis']['title'] = u'Max memory used in KB'
    fig['layout']['yaxis2']['title'] = u'Total time in sec'
    fig.show()


def write_results(result: list, file: str):
    """Writes usage stats from a single run into output file.

    :param result: Gathered resource usage statistics.
    :param file: File for data storage.
    """

    try:
        f = open(file)
    except FileNotFoundError:
        with open(file, 'w') as f:
            print(u'Creating a new file for usage stats...')
            f.write(u'task,max_mem_usage,total_time\n')
    finally:
        f.close()
    with open(file, 'a') as f:
        # write if multiple tasks were run
        if isinstance(result[0], tuple):
            for res in result:
                res = modify_stats(res)
                f.write(u'{}\n'.format(res))
        # write if only one task was run
        elif isinstance(result[0], str):
            res = modify_stats(result)
            f.write(u'{}\n'.format(res))


def modify_stats(stats: list) -> str:
    """Reformat the result data into csv style.

    :param stats: List of inputs for reformat.
    :return: Reformatted string.
    """

    stats = str(stats)[1:-1].split(',')
    stats[0] = stats[0].strip("'")
    stats = u','.join(stats)
    return stats


if __name__ == '__main__':

    # parse input options
    parser = create_parser()
    args = parser.parse_args()

    # run tasks and write the results
    usage_results = run_tasks(args)
    write_results(usage_results, args.file)

    if args.plot:
        plot_results(args.file)
