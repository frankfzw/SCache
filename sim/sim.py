#!/usr/bin/env python

import numpy as np
import os
import pandas as pd


trace_path = '/home/frankfzw/SCache/sim/2012-10/attempt.csv'
res_path = '/home/frankfzw/SCache/sim/res'
schedule = ['fifo', 'round_robin']
hosts_num = 10

def deal_na_int(x):
	if (x == None):
		return -1
	else:
		return x


field_names = {'jtid': int, 'jobid': int, 'tasktype': str, 'taskid': int, 'attempt': int, 'startTime': int, 'shuffleTime': int, 'sortTime': int, 'finishTime': int, 'status': int, 'rack': str, 'hostname': str}
converters = {'shuffleTime': deal_na_int, 'sortTime': deal_na_int}
raw_talbe = pd.read_csv(filepath_or_buffer=trace_path, dtype=field_names, converters=converters)
reduce_talbe = raw_talbe.loc[raw_talbe['tasktype'] == 'r']

def round_robin_schedule(reduce_tasks, num_hosts):
	times = np.zeros(num_hosts)
	tasks_size = len(reduce_tasks.index)
	for i in range(tasks_size):
		r = reduce_tasks.iloc[i]
		# print '{}\t{}'.format(r['startTime'], r['finishTime'])
		times[i % num_hosts] += r['finishTime'] - r['startTime']
	return np.amax(times)

def fifo_schedule(reduce_tasks, num_hosts):
	times = np.zeros(num_hosts)
	tasks_size = len(reduce_tasks.index)
	for i in range(min(num_hosts, tasks_size)):
		r = reduce_tasks.iloc[i]
		times[i] += r['finishTime'] - r['startTime']
	for i in range(num_hosts, tasks_size):
		times = np.sort(times)
		r = reduce_tasks.iloc[i]
		times[0] += r['finishTime'] - r['startTime']
	return np.amax(times)

def do_schedule(reduce_tasks, num_hosts, scheme):
	if (scheme == 'fifo'):
		return fifo_schedule(reduce_tasks, num_hosts)
	elif (scheme == 'round_robin'):
		return round_robin_schedule(reduce_tasks, num_hosts)
	else:
		print 'Wrong scheme %s' % scheme
		return None

def main():
	print 'Start simulation'
	print 'Trace: %s' % trace_path
	print 'Scheduler: %s' % schedule

	jobids = set(reduce_talbe['jobid'].values)
	jobids_list = list(jobids)
	# delete empty ones
	for jid in jobids_list:
		reduce_tasks = reduce_talbe.loc[(reduce_talbe['jobid'] == jid) & (reduce_talbe['status'] == 0)]
		if (len(reduce_tasks.index) == 0):
			jobids.remove(jid)

	jobids = np.array(list(jobids))

	for scheme in schedule:
		res = np.zeros(len(jobids))
		for i in range(len(jobids)):
			reduce_tasks = reduce_talbe.loc[(reduce_talbe['jobid'] == jobids[i]) & (reduce_talbe['status'] == 0)]
			t = do_schedule(reduce_tasks, hosts_num, scheme)
			res[i] = t
		df = pd.DataFrame({'jid':jobids, 'time':res})
		df.to_csv('{}/{}.csv'.format(res_path, scheme))




if __name__ == '__main__':
	main()