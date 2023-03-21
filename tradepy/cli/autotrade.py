import argparse
import signal
import subprocess
import functools
import sys


def start_beat():
    return subprocess.Popen([
        'celery',
        '-A',
        'tradepy.bot.celery_app',
        'beat',
        '-s',
        '/tmp/beat-schedule',
        '--pidfile='
    ])


def start_workers(log_level):
    return subprocess.Popen([
        'celery',
        '-A',
        'tradepy.bot.celery_app',
        'worker',
        '-c',
        '2',
        '-P',
        'prefork',
        '-l',
        log_level
    ])


def terminate(signum, *args, processes=None):
    assert processes
    for proc in processes:
        proc.terminate()
    sys.exit(signum)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start the autotrade bot')
    parser.add_argument('--level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='set log level')
    args = parser.parse_args()

    try:
        beat_proc = start_beat()
        worker_proc = start_workers(args.level)

        signal.signal(signal.SIGTERM, functools.partial(terminate, processes=[beat_proc, worker_proc]))
        worker_proc.wait()
        beat_proc.terminate()
    except KeyboardInterrupt:
        terminate(signal.SIGINT, processes=[beat_proc, worker_proc])
