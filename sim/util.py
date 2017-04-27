#!/usr/bin/env python

import json
from datetime import datetime
import csv

app_id = 'app-20170327074031-0002'
dir_name = '/mnt/d/tmp'
file_path = '{}/{}.json'.format(dir_name, app_id)
output_path = '{}/{}.csv'.format(dir_name, app_id)
host = 'ip-172-31-38-238.us-west-2.compute.internal'

cpu = '{}/cpu.txt'.format(dir_name)
net = '{}/net.txt'.format(dir_name)
disk = '{}/disk.txt'.format(dir_name)

metrics_tag = 'Task Metrics'
s_read_tag = 'Shuffle Read Metrics'
s_write_tag = 'Shuffle Write Metrics'

f = open(file_path)

# dict for taskid : [launch time, shuffle read time ns, deserialize time ms, execution time ms, shuffle write time us]
task_info = {}

for line in f:
	data = json.loads(line)
	if 'Task' not in data['Event']:
		continue
	if 'TaskStart' in data['Event'] and data['Task Info']['Host'] == host:
		tid = int(data['Task Info']['Task ID'])
		launch_ts = long(data['Task Info']['Launch Time'])
		task_info[tid] = [datetime.fromtimestamp(launch_ts / 1000.0).strftime("%H:%M:%S:%f")]
	elif 'TaskEnd' in data['Event'] and data['Task Info']['Host'] == host:
		tid = int(data['Task Info']['Task ID'])
		deser_t = long(data[metrics_tag]['Executor Deserialize Time'])
		exe_t = long(data[metrics_tag]['Executor Run Time'])
		s_read_t = 0
		s_write_t = 0
		if s_read_tag in data[metrics_tag]:
			s_read_t = long(data[metrics_tag][s_read_tag]['Fetch Wait Time'])
		if s_write_tag in data[metrics_tag]:
			s_write_t = long(data[metrics_tag][s_write_tag]['Shuffle Write Time'])
		task_info[tid].extend([s_read_t, deser_t, exe_t, s_write_t])

output = open(output_path, 'w')
wr = csv.writer(output, quoting=csv.QUOTE_ALL)
for k, v in task_info.iteritems():
	l = [k]
	l.extend(v)
	wr.writerow(l)


# convert cpu disk and net to csv
f = open(cpu, 'r')
f.readline()
res = []
for l in f:
	args = l.split()
	ts = datetime.fromtimestamp(long(args[0]) / 1000.0).strftime("%H:%M:%S:%f")
	v = args[1]
	res.append([ts, v])
output = open('{}/cpu.csv'.format(dir_name), 'w')
wr = csv.writer(output, quoting=csv.QUOTE_ALL)
for r in res:
	wr.writerow(r)

target = ['net', 'disk']
for t in target:
	f = open('{}/{}.txt'.format(dir_name, t), 'r')
	f.readline()
	res = []
	for l in f:
		args = l.split()
		ts = datetime.fromtimestamp(long(args[0]) / 1000.0).strftime("%H:%M:%S:%f")
		v1 = args[1]
		v2 = args[2]
		res.append([ts, v1, v2])
	output = open('{}/{}.csv'.format(dir_name, t), 'w')
	wr = csv.writer(output, quoting=csv.QUOTE_ALL)
	for r in res:
		wr.writerow(r)

