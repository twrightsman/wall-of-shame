"""
shamed - Wall of Shame daemon

by Travis Wrightsman
"""

import argparse
from collections import defaultdict, namedtuple
import logging
from pathlib import Path
import queue
import sys
import sqlite3
import threading
import time
from typing import List

import psutil

ShameRecord = namedtuple('ShameRecord', ['timestamp', 'username', 'shame'])


def poll_thread_main(poll_queue: queue.Queue, shutdown_event: threading.Event, poll_interval: int,
                     username_blacklist: List[str], proc_name_blacklist: List[str]):
    while not shutdown_event.is_set():
        # poll current processes
        shame = defaultdict(int)
        current_timestamp = int(time.time())
        for proc in psutil.process_iter():
            if (proc.nice() >= 0) and (proc.username() not in username_blacklist) and (
                    proc.name() not in proc_name_blacklist):
                shame[proc.username()] += ((20 - proc.nice()) * proc.cpu_num())

        # add to queue
        for username in shame:
            poll_queue.put(ShameRecord(
                timestamp=current_timestamp,
                username=username,
                shame=shame[username]
            ))

        shutdown_event.wait(poll_interval)


def write_thread_main(poll_queue: queue.Queue, db_path: str, shutdown_event: threading.Event,
                      write_interval: int):
    while not shutdown_event.is_set():
        # write each item in queue to database
        write_queue = []
        while not poll_queue.empty():
            # unload queue to write queue
            write_queue.append(poll_queue.get())
        if write_queue:
            logging.info("Writing {} items to database".format(len(write_queue)))
            db_con = sqlite3.connect(db_path)
            db_cur = db_con.cursor()
            for record in write_queue:
                db_cur.execute('INSERT INTO shame VALUES (?, ?, ?)', (record.timestamp, record.username, record.shame))
            db_con.commit()
            db_con.close()

        shutdown_event.wait(write_interval)


def report_thread_main(leaderboard_path: Path, db_path: str, shutdown_event: threading.Event,
                       report_interval: int):
    start_time = time.localtime()
    while not shutdown_event.is_set():
        # query the database
        db_con = sqlite3.connect(db_path)
        db_cur = db_con.cursor()
        db_cur.execute('SELECT * FROM shame')
        rows = db_cur.fetchall()

        column_names = [ele[0] for ele in db_cur.description]
        username_index = column_names.index('username')
        shame_index = column_names.index('shame')
        if rows:
            # build the shame dictionary from the query results
            shame = defaultdict(int)
            for row in rows:
                shame[row[username_index]] += row[shame_index]

            # draw the leaderboard
            logging.info('Regenerating the leaderboard')
            current_time = time.localtime()
            with open(leaderboard_path, 'w') as leaderboard_file:
                # write the header
                leaderboard_file.write("Wall of Shame\n")
                leaderboard_file.write("From {start} to {end}\n\n".format(start=time.asctime(start_time),
                                                                          end=time.asctime(current_time)))
                for index, username in enumerate(sorted(shame.keys(), key=lambda item: shame[item], reverse=True)[:10]):
                    # write the rankings
                    leaderboard_file.write("[#{rank}] {username} ({shame})\n".format(rank=index + 1,
                                                                                     username=username,
                                                                                     shame=shame[username]))
        db_con.close()

        shutdown_event.wait(report_interval)


def shamed(args):

    # initialize the database schema
    db_con = sqlite3.connect(str(args.database))
    db_cur = db_con.cursor()
    db_cur.execute('DROP TABLE IF EXISTS shame')
    db_cur.execute('CREATE TABLE shame(timestamp INT, username TEXT, shame INT)')
    db_con.commit()
    db_con.close()

    poll_queue = queue.Queue()

    # start the threads
    shutdown_event = threading.Event()
    threads = {
        'poll': threading.Thread(name='poll', target=poll_thread_main, args=(poll_queue, shutdown_event,
                                                                             args.poll_interval, args.ignore_users,
                                                                             args.ignore_names)),
        'write': threading.Thread(name='write', target=write_thread_main, args=(poll_queue, str(args.database),
                                                                                shutdown_event, args.write_interval)),
        'report': threading.Thread(name='report', target=report_thread_main, args=(args.leaderboard, str(args.database),
                                                                                   shutdown_event,
                                                                                   args.report_interval))
    }
    logging.debug('Starting threads')
    for task in threads:
        threads[task].start()
    try:
        for task in threads:
            threads[task].join()
    except KeyboardInterrupt:
        print('Shutting down gracefully')
        shutdown_event.set()
        for task in threads:
            # wait for threads to finish
            threads[task].join()
            logging.debug("{} thread successfully shut down".format(task))


def main():
    parser = argparse.ArgumentParser(
        prog='python3 shamed.py'
    )

    parser.add_argument(
        '-d', '--debug',
        help='Output detailed debugging messages',
        action='store_const',
        dest='log_level',
        const=logging.DEBUG,
        default=logging.WARNING
    )

    parser.add_argument(
        '-v', '--verbose',
        help='Output progress and other informative messages',
        action='store_const',
        dest='log_level',
        const=logging.INFO
    )

    parser.add_argument(
        '-l', '--leaderboard',
        metavar='/path/to/leaderboard.txt',
        help='Path to which the leaderboard will be written',
        type=Path,
        default=Path('wall_of_shame.txt')
    )

    parser.add_argument(
        '-b', '--database',
        metavar='/path/to/database.db',
        help='Path to which the process history database will be written',
        type=Path,
        default=Path('shame.db')
    )

    parser.add_argument(
        '-p', '--poll-interval',
        dest='poll_interval',
        metavar='seconds',
        help='Time between system process status queries',
        type=int,
        default=1
    )

    parser.add_argument(
        '--write-interval',
        dest='write_interval',
        metavar='seconds',
        help='Time between data dumps to database',
        type=int,
        default=60
    )

    parser.add_argument(
        '--report-interval',
        dest='report_interval',
        metavar='seconds',
        help='Time between leaderboard updates',
        type=int,
        default=300
    )

    parser.add_argument(
        '--ignore-users',
        dest='ignore_users',
        metavar='"user1, user2, userN"',
        help='Comma-separated list of usernames to ignore while shaming',
        default='root'
    )

    parser.add_argument(
        '--ignore-names',
        dest='ignore_names',
        metavar='"proc_name1, proc_name2, proc_nameN"',
        help='Comma-separated list of process names to ignore while shaming',
        default='bash'
    )

    args = parser.parse_args()

    # validate arguments
    if args.poll_interval <= 0:
        sys.exit('Poll interval must be 1 second or greater.')

    if args.write_interval <= 0:
        sys.exit('Write interval must be 1 second or greater.')

    if args.report_interval <=0:
        sys.exit('Leaderboard interval must be 1 second or greater.')

    args.ignore_users = [username.strip() for username in args.ignore_users.split(',')]
    args.ignore_names = [name.strip() for name in args.ignore_names.split(',')]

    # set up logging to stderr
    root = logging.getLogger()
    root.setLevel(args.log_level)
    stderr_log_handler = logging.StreamHandler(sys.stderr)
    stderr_log_handler.setLevel(args.log_level)
    stderr_log_formatter = logging.Formatter("{asctime} [{module}:{levelname}] {message}", style='{')
    stderr_log_handler.setFormatter(stderr_log_formatter)
    root.addHandler(stderr_log_handler)

    # run the shame daemon
    shamed(args)


if __name__ == '__main__':
    main()