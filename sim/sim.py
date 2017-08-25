#!/usr/bin/env python

import numpy as np
import os
import pandas as pd
import sys
import random


trace_date = ['2012-10']
res_path = '.'
# schedule = ['fifo', 'round_robin_pre', 'round_robin']
# schedule = ['fifo', 'round_robin_pre', 'scache']
schedule = ['fifo', 'ideal', 'scache', 'round_robin_pre']
# schedule = ['reduce_cdf']
hosts_num = 10
round_num = 10
test_ids = ['20170324093238_1']

def deal_na_int(x):
	if (x == '' or x == None):
		return -1
	else:
		return int(x)

def expand_task(task, r):
	l = len(task)
	total = hosts_num * r
	res = task
	i = 0
	while len(res) < total:
		res = np.append(res, task[i])
		i = (i + 1) % l
	# np.random.shuffle(res)
	return res

def find_min(times):
	tag = times[0]
	index = 0
	for i in range(len(times)):
		if (times[i] < tag):
			tag = times[i]
			index = i
	return index


def round_robin_pre_schedule(reduce_tasks, num_hosts, r):
	h = max(num_hosts, hosts_num)
	times = np.zeros(h)
	tasks_size = len(reduce_tasks.index)
	finish_time = np.array(list(reduce_tasks['finishTime'].values))
	shuffle_time = np.array(list(reduce_tasks['shuffleTime'].values))
	sort_time = np.array(list(reduce_tasks['sortTime'].values))
	start_time = np.array(list(reduce_tasks['startTime'].values))
	shuffle_time = sort_time - shuffle_time
	run_time = finish_time - start_time - shuffle_time
	if num_hosts < hosts_num:
		run_time = expand_task(run_time, r)
	for i in range(tasks_size):
		# print '{}\t{}'.format(r['startTime'], r['finishTime'])
		times[i % h] += run_time[i]
	# print sum(times)
	# print times
	return np.amax(times)

def round_robin_schedule(reduce_tasks, num_hosts, r):
	h = max(num_hosts, hosts_num)
	times = np.zeros(h)
	tasks_size = len(reduce_tasks.index)
	finish_time = np.array(list(reduce_tasks['finishTime'].values))
	start_time = np.array(list(reduce_tasks['startTime'].values))
	run_time = finish_time - start_time
	if num_hosts < hosts_num:
		run_time = expand_task(run_time, r)
	for i in range(tasks_size):
		# print '{}\t{}'.format(r['startTime'], r['finishTime'])
		times[i % h] += run_time[i]
	# print sum(times)
	# print times
	return np.amax(times)


def fifo_schedule(reduce_tasks, num_hosts, r):
	h = max(num_hosts, hosts_num)
	times = np.zeros(h)
	tasks_size = len(reduce_tasks.index)
	finish_time = np.array(list(reduce_tasks['finishTime'].values))
	start_time = np.array(list(reduce_tasks['startTime'].values))
	run_time = finish_time - start_time
	if num_hosts < hosts_num:
		run_time = expand_task(run_time, r)
	if tasks_size < num_hosts:
		for i in range(tasks_size):
			times[i] += run_time[i]
	else:
		for i in range(tasks_size):
			index = find_min(times)
			times[index] += run_time[i]
	return np.max(times)

def scache_schedule(reduce_tasks, num_hosts, r):
	h = max(num_hosts, hosts_num)
	times = np.zeros(h)
	tasks_size = len(reduce_tasks.index)
	finish_time = np.array(list(reduce_tasks['finishTime'].values))
	start_time = np.array(list(reduce_tasks['startTime'].values))
	shuffle_time = np.array(list(reduce_tasks['shuffleTime'].values))
	sort_time = np.array(list(reduce_tasks['sortTime'].values))
	shuffle_time = sort_time - shuffle_time
	run_time = finish_time - start_time - shuffle_time
	if num_hosts < hosts_num:
		run_time = expand_task(run_time, r)
	if tasks_size < num_hosts:
		for i in range(tasks_size):
			times[i] += run_time[i]
	else:
		for i in range(tasks_size):
			index = find_min(times)
			times[index] += run_time[i]
	return np.max(times)


def ideal_schedule(reduce_tasks, num_hosts, r):
	h = max(num_hosts, hosts_num)
	times = np.zeros(h)
	tasks_size = len(reduce_tasks.index)
	finish_time = np.array(list(reduce_tasks['finishTime'].values))
	shuffle_time = np.array(list(reduce_tasks['shuffleTime'].values))
	sort_time = np.array(list(reduce_tasks['sortTime'].values))
	start_time = np.array(list(reduce_tasks['startTime'].values))
	shuffle_time = sort_time - shuffle_time
	run_time = finish_time - start_time - shuffle_time
	if num_hosts < hosts_num:
		run_time = expand_task(run_time, r)
	run_time = np.sort(run_time)
	tid = tasks_size - 1
	while tid >= 0:
		index = find_min(times)
		times[index] += run_time[tid]
		tid -= 1
	return np.max(times)

def reduce_cdf(reduce_tasks, num_hosts):
	tasks_size = len(reduce_tasks.index)
	finish_time = np.array(list(reduce_tasks['finishTime'].values))
	start_time = np.array(list(reduce_tasks['startTime'].values))
	shuffle_time = np.array(list(reduce_tasks['shuffleTime'].values))
	sort_time = np.array(list(reduce_tasks['sortTime'].values))
	shuffle_time = sort_time - shuffle_time
	run_time = finish_time - start_time
	tmp = shuffle_time.astype(float) / run_time.astype(float)
	return np.average(tmp)

def do_schedule(reduce_tasks, num_hosts, r, scheme):
	if (scheme == 'fifo'):
		return fifo_schedule(reduce_tasks, num_hosts, r)
	elif (scheme == 'round_robin_pre'):
		return round_robin_pre_schedule(reduce_tasks, num_hosts, r)
	elif (scheme == 'round_robin'):
		return round_robin_schedule(reduce_tasks, num_hosts, r)
	elif (scheme == 'ideal'):
		return ideal_schedule(reduce_tasks, num_hosts, r)
	elif (scheme == 'scache'):
		return scache_schedule(reduce_tasks, num_hosts, r)
	elif (scheme == 'reduce_cdf'):
		return reduce_cdf(reduce_tasks, num_hosts)
	else:
		print 'Wrong scheme %s' % scheme
		return None

def extend_array(a, b):
	a.extend(b)
	return a

def min_idx(arr, left, right):
	if arr[left][1] < arr[right][1]:
		return left
	else:
		return right
def max_idx(arr, left, right):
	if arr[left][1] > arr[right][1]:
		return left
	else:
		return right

def swap(arr, a, b):
	tmp  = arr[a]
	arr[a] = arr[b]
	arr[b] = tmp

def sift_down(arr, idx):
	if idx >= len(arr):
		return 
	left = idx * 2
	right = idx * 2 + 1
	if left < len(arr):
		if right < len(arr):
			min_c = min_idx(arr, left, right)
			if arr[idx][1] < arr[min_c][1]:
				return
			else:
				swap(arr, idx, min_c)
				sift_down(arr, min_c)
		else:
			if arr[idx][1] < arr[left][1]:
				swap(arr, idx, left)
				sift_down(arr, left)
			else:
				return
	else:
		return

def swap_task(arr, tid, h_ori, h_tar, sizes, task_map):
	# print arr
	# print '{}, from {} to {}'.format(tid, h_ori, h_tar)
	ori_size = sizes[tid]
	size = 0
	tids = []
	for t in arr[h_tar][2]:
		if size + sizes[t] > ori_size * 1.1:
			continue
		size += sizes[t]
		tids.append(t)
	if len(tids) > 0:
		# we can swap
		for t in tids:
			arr[h_tar][2].remove(t)
			arr[h_ori][2].append(t)
			# update task_host map
			task_map[t] = h_ori
		arr[h_tar][3].append(tid)
		arr[h_tar][1] = arr[h_tar][1] - size + ori_size
		# add t to h_ori's array
		arr[h_ori][2].remove(tid)
		arr[h_ori][1] = arr[h_ori][1] - ori_size + sizes[t]

# return [[hostid, size, [taskids]], ...]
def schedule_with_pre(reduce_tasks, num_hosts, num_map):
	id_size = reduce_tasks[['reduceid', 'pre_size', 'prob']].values.tolist()
	id_size = map(lambda x: [int(x[0]), x[1], x[2]], id_size)
	res = [0]
	tmp = {}
	size_map = {}
	# res [[hostid, size, [taskid], [swap]], ...]
	for i in range(1, num_hosts + 1):
		res.append([i - 1, 0, [], []])
	# sort tasks
	sorted_id_size = sorted(id_size, key=lambda x: x[1])
	tid = len(sorted_id_size) - 1
	while tid >= 0:
		res[1][1] += sorted_id_size[tid][1]
		res[1][2].append(sorted_id_size[tid][0])
		size_map[sorted_id_size[tid][0]] = sorted_id_size[tid][1]
		tmp[sorted_id_size[tid][0]] = res[1][0]
		sift_down(res, 1)
		tid -= 1
	# swap for locality
	res = res[1:]
	res = sorted(res, key=lambda x: x[0])
	print 'Before swap: {}'.format(res)
	t = max(map(lambda x: x[1], res))
	print 'Stage completion time {}'.format(t)
	tid = len(sorted_id_size) - 1
	while tid >= 0:
		prob = reduce_tasks[reduce_tasks['reduceid'] == tid]['prob'].values
		nor = (prob - 1/num_map) / (1 - 1/num_map)
		host = int(reduce_tasks[reduce_tasks['reduceid'] == tid]['host'].values)
		if host != tmp[tid] and nor > random.random():
			swap_task(res, tid, tmp[tid], host, size_map, tmp)
		else:
			res[host][3].append(tid)
			res[host][2].remove(tid)
		tid -= 1
	print 'After swap: {}'.format(res)
	at = max(map(lambda x: x[1], res))
	print 'Stage completion time {}, {}'.format(at, (at-t)/t)
	res = map(lambda x: [x[0], x[1], extend_array(x[2], x[3])], res)
	return res

def main():
	if len(sys.argv) == 2:
		# run test
		for d in test_ids:
			path = '{}/{}_sample_pre_reduce.csv'.format(res_path, d)
			df = pd.read_csv(path)
			hosts = set(df['host'].values.tolist())
			res = schedule_with_pre(df, len(hosts), len(df['reduceid'].values.tolist()))
			t = max(map(lambda x: x[1], res))
			print 'Stage completion time of {}: {}'.format(d, t)
		return
	for trace in trace_date:
		trace_path = '{}/{}/attempt.csv'.format(res_path, trace)
		field_names = {'jtid': int, 'jobid': int, 'tasktype': str, 'taskid': int, 'attempt': int, 'startTime': int, 'shuffleTime': int, 'sortTime': int, 'finishTime': int, 'status': int, 'rack': str, 'hostname': str}
		converters = {'shuffleTime': deal_na_int, 'sortTime': deal_na_int, 'finishTime': deal_na_int, 'status': deal_na_int}
		raw_talbe = pd.read_csv(filepath_or_buffer=trace_path, dtype=field_names, converters=converters)
		reduce_talbe = raw_talbe.loc[raw_talbe['tasktype'] == 'r']
		print 'Start simulation'
		print 'Trace: %s' % trace_path
		print 'Scheduler: {}'.format(schedule)

		jobids = set(reduce_talbe['jobid'].values)
		jobids_list = list(jobids)
		# delete empty ones
		for jid in jobids_list:
			reduce_tasks = reduce_talbe.loc[(reduce_talbe['jobid'] == jid) & (reduce_talbe['status'] == 0)]
			if (len(reduce_tasks.index) == 0 or len(reduce_tasks.index) < 6):
				jobids.remove(jid)

		jobids = np.array(list(jobids))

		base_line = [0] * round_num
		for r in range(1, round_num+1, 1):
			print 'Processing base line round %d' % (r)
			res = np.zeros(len(jobids))
			for i in range(len(jobids)):
				# if jobids[i] != 4817:
				# 	continue
				reduce_tasks = reduce_talbe.loc[(reduce_talbe['jobid'] == jobids[i]) & (reduce_talbe['status'] == 0)]
				num_host = len(reduce_tasks.index) / r
				t = do_schedule(reduce_tasks, num_host, r, 'fifo')
				if t is None:
					break
				res[i] = t
			base_line[r-1] = res

		for scheme in schedule:
			f = open('{}/{}_{}.csv'.format(res_path, trace, scheme), 'w')
			f.write('round,time\n')
			for r in range(1, round_num+1, 1):
				print 'Processing %s with round %d' % (scheme, r)
				res = np.zeros(len(jobids))
				for i in range(len(jobids)):
					# if jobids[i] != 4817:
					# 	continue
					reduce_tasks = reduce_talbe.loc[(reduce_talbe['jobid'] == jobids[i]) & (reduce_talbe['status'] == 0)]
					num_host = len(reduce_tasks.index) / r
					# if num_host == 0:
					# 	continue
					t = do_schedule(reduce_tasks, num_host, r, scheme)
					if t is None:
						break
					res[i] = (base_line[r-1][i] - t) / base_line[r-1][i]
				df = pd.DataFrame({'jid':jobids, 'time':res})
				# df.to_csv('{}/{}_{}_{}.csv'.format(res_path, scheme, trace, r))
				print 'Average completion time of {} with round {}: {}'.format(scheme, r, np.average(res))
				f.write('{},{}\n'.format(r, np.average(res)))




if __name__ == '__main__':
	main()
