#!/usr/bin/env python3
#
# Simulation of a M/D/c queue.
#
# See http://en.wikipedia.org/wiki/M/M/c_queue for more background.

import sys
import math
import yaml
import random
import time
import heapq
import collections


if len(sys.argv) != 2:
    sys.stderr.write('Usage: {} <inputfile>\n')
    sys.exit(1)

fname = sys.argv[1]
with open(fname) as fin:
    simdata = yaml.load(fin)

def get_param(params, name, typ):
    value = params.get(name)
    if value is None or not isinstance(value, typ):
        sys.stderr.write('Error: wrong/missing parameter "{}".\n'.format(name))
        sys.exit(1)
    return value

# event types
ev_input, ev_done = range(2)

def simulate(params):
    """Run a simulation. Return a dictionary of statistics."""
    # simulation parameters
    l = get_param(params, 'l', (int, float))  # input rate
    u = get_param(params, 'u', (int, float))  # service rate
    d = 1/u  # service time
    c = get_param(params, 'c', int)  # number of servers
    twait = get_param(params, 'twait', (int, float))  # max wait time
    endtime = get_param(params, 'endtime', (int, float))

    # simulation state
    events = [(random.expovariate(l), ev_input, None)]
    queue = collections.deque()
    servers_occupied = 0
    last_queue_update = 0.0

    # simulation statistics
    stats = {
        'input_events': 0,
        'done_events': 0,
        'completed_without_queueing': 0,
        'completed_with_queueing': 0,
        'avg_wait_time': 0.0,
        'avg_completion_time': 0.0,
        'avg_queue_depth': 0.0,
        'max_queue_depth': 0,
        'served_immediately': 0,
        'wait_gt_twait': 0,
        'wait_lt_twait': 0
    }

    wallclock_start = time.time()

    while True:
        assert(len(events)) > 0
        now, evtype, start = heapq.heappop(events)
        if now > endtime:
            break
        if evtype == ev_input:
            # a new event arrived
            stats['input_events'] += 1
            if servers_occupied < c:
                # schedule immediately
                assert len(queue) == 0
                servers_occupied += 1
                stats['served_immediately'] += 1
                heapq.heappush(events, (now+d, ev_done, now))
            else:
                # otherwise queue
                stats['avg_queue_depth'] += (now - last_queue_update) * len(queue)
                stats['max_queue_depth'] = max(stats['max_queue_depth'], len(queue))
                last_queue_update = now
                queue.append(now)
            dt = random.expovariate(l)
            heapq.heappush(events, (now+dt, ev_input, None))  # schedule next input
        elif evtype == ev_done:
            # a job is done
            servers_occupied -= 1
            assert 0 <= servers_occupied < c
            stats['done_events'] += 1
            completion_time = now - start
            stats['avg_completion_time'] += completion_time
            # if there is a backlog, service oldest request
            if queue:
                stats['avg_queue_depth'] += (now - last_queue_update) * len(queue)
                last_queue_update = now
                arrived = queue.popleft()
                heapq.heappush(events, (now+d, ev_done, arrived))
                servers_occupied += 1
                wait_time = now - arrived
                stats['avg_wait_time'] += wait_time
                if wait_time > twait:
                    stats['wait_gt_twait'] += 1
                else:
                    stats['wait_lt_twait'] += 1

    wallclock_end = time.time()

    stats['avg_wait_time'] /= stats['done_events']
    stats['avg_completion_time'] /= stats['done_events']
    stats['avg_queue_depth'] /= endtime
    stats['elapsed_time'] = (wallclock_end - wallclock_start)
    stats['wait_lt_twait'] /= stats['done_events']
    stats['wait_gt_twait'] /= stats['done_events']
    stats['served_immediately'] /= stats['done_events']
    stats['queue_end_len'] = len(queue)
    stats['events_end_len'] = len(events)

    return stats


def show_statistics(params, stats):
    print('Simulation DONE!')
    print('Simulation took {:.2f} secs.'.format(stats['elapsed_time']))
    print()
    print('Input parameters:')
    print('  l = {}'.format(params['l']))
    print('  u = {} (d = {:.2})'.format(params['u'], params['d']))
    print('  c = {}'.format(params['c']))
    print('  endtime = {}'.format(params['endtime']))
    print()
    print('Simulation results:')
    print('  number of input events: {}'.format(stats['input_events']))
    print('  number of done events: {}'.format(stats['done_events']))
    print('  queue depth at end: {}'.format(stats['queue_end_len']))
    print('  in progress at end: {}'.format(stats['events_end_len']))
    print('  average queue depth: {:.2f}'.format(stats['avg_queue_depth']))
    print('  max queue depth: {}'.format(stats['max_queue_depth']))
    print('  average wait time: {:.2f}'.format(stats['avg_wait_time']))
    print('  average completion time: {:.2f}'.format(stats['avg_completion_time']))
    print('  P(wait_time = 0): {:.2f}'.format(stats['served_immediately']))
    print('  P(wait_time <= {:.2f}): {:.3f}'.format(params['twait'], stats['wait_lt_twait']))
    print('  P(wait_time > {:.2f}): {:.3f}'.format(params['twait'], stats['wait_gt_twait']))


results = []

params_fields = ('l', 'u', 'c', 'twait', 'endtime')
stats_fields = ('input_events', 'done_events', 'queue_end_len',
                'events_end_len', 'avg_queue_depth', 'max_queue_depth',
                'avg_wait_time', 'avg_completion_time', 'served_immediately',
                'wait_lt_twait', 'wait_gt_twait')

def write_csv_header(params, stats):
    fields = params_fields + stats_fields
    fields = ','.join(fields)
    print(fields)


def write_csv(params, stats):
    values = [ str(params[field]) for field in params_fields ]
    values += [ str(stats[field]) for field in stats_fields ]
    values = ','.join(values)
    print(values)

simdata['endtime'] = simdata['nevents'] / simdata['l']
simdata['d'] = 1.0/simdata['u']

stats = simulate(simdata)
show_statistics(simdata, stats)
