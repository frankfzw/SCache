#!/usr/bin/env python

import numpy as np
from sklearn import linear_model
import pandas as pd
import os


class Train:
	def __init__(self, map_num, reduce_num, alpha):
		self.map_num = map_num
		self.reduce_num = reduce_num
		self.reg = linear_model.LinearRegression()
		

	# samples_in and samples_out are matrices which have same row number
	# e.g.
	# samples_in: [[mapid_0, host_0, mapsize_0], ... [mapid_n, host_n, mapsize_n]]
	# samples_out: [[reduce_size_1, ... reduce_size_reduce_num], ... [reduce_size_1, ... reduce_size_reduce_num]]
	def train(self, samples_in, samples_out):
		self.reg.fit(samples_in, samples_out)

	def predict(self, inputs):
		return self.reg.predict(inputs)

	def score(self, X_validate, Y_validate):
		return self.reg.score(X_validate, Y_validate)

def parse_log(file_path):
	hosts_map = {}
	hid = 0
	res = []	
	f = open(file_path, 'r')
	for line in f:
		if 'Added' not in line:
			continue
		arrays = line.split(' ')
		block_id = arrays[5]
		ids = block_id.split('_')
		jobid = int(ids[1])
		shuffleid = int(ids[2])
		mapid = int(ids[3])
		reduceid = int(ids[4])
		size = float(arrays[11])
		host = arrays[9]
		if host not in hosts_map:
			hosts_map[host] = hid
			hid += 1
		hostid = hosts_map[host]
		res.append([jobid, shuffleid, mapid, hostid, reduceid, size])

	# field_name = [('jobid', 'int'), ('shuffleid', 'int'), ('mapid', 'int'), ('host', 'int'), ('reduceid', 'int'), ('size', 'float')]
	# df = pd.DataFrame(columns=['jobid', 'shuffleid', 'mapid', 'host', 'reduceid', 'size'])
	df = pd.DataFrame(data=res, columns=['jobid', 'shuffleid', 'mapid', 'host', 'reduceid', 'size'])
	return df

def parse_spark_log(file_dir):
	res = []	
	for root, dirs, names in os.walk(file_dir):
		for file in names:
			arrays = file.split('-')
			application = arrays[1]
			exe_id = int(arrays[-1])
			f = open(os.path.join(root, file))
			for line in f:
				if 'frankfzw: shuffleid' in line:
					datas = line.split()
					s_id = int(datas[6])
					m_id = int(datas[8])
					r_id = int(datas[10])
					s = long(datas[-1])
					res.append([application, s_id, m_id, exe_id, r_id, s])
	df = pd.DataFrame(data=res, columns=['jobid', 'shuffleid', 'mapid', 'host', 'reduceid', 'size'])
	return df

			


def main():
	# file_path = '/home/frankfzw/SCache/sim/master-spark0-log.out'
	# df = parse_log(file_path)
	# map_ids = df.groupby(['mapid']).groups.keys()
	# reduce_ids = df.groupby(['reduceid']).groups.keys()
	# map_sizes = [134217728, 134217728, 134217728, 134217728, 134217728, 134217728, 76885773]
	# hosts = []
	# Y = []
	# X = []
	# for m in map_ids:
	# 	hosts.append(df.loc[df['mapid'] == m]['host'].values[0])
	# 	Y.append(df.loc[df['mapid'] == m]['size'].values)

	# tmp = zip(map_ids, hosts, map_sizes)
	# for x in tmp:
	# 	X.append(list(x))

	# model = Train(len(map_ids), len(reduce_ids), 0.5)
	# model.train(X[0:2], Y[0:2])
	# print model.score(X[2:], Y[2:])
	file_path = '/home/frankfzw/Spark/evaluation/spark/single shuffle'
	df = parse_spark_log(file_path)
	applications = df.groupby(['jobid']).groups.keys()
	for app_id in applications:
		# if app_id != '20170313105547':
		# 	continue
		app_data = df.loc[df['jobid'] == app_id]
		shuffles = app_data.groupby(['shuffleid']).groups.keys()
		for s_id in shuffles:
			shuffle_data = app_data.loc[app_data['shuffleid'] == s_id]

			# calculate the final reduce size
			reduces = shuffle_data.groupby(['reduceid']).sum()
			reduces_size = zip(reduces.index.values, reduces['size'].values)
			
			# build test dataset
			map_ids = shuffle_data['mapid']
			hosts = shuffle_data['host']
			reduce_ids = shuffle_data['reduceid']
			output_sizes = shuffle_data['size']
			tmp = zip(map_ids, hosts, reduce_ids, output_sizes)
			map_nums = max(map_ids) + 1
			num_hosts = len(set(hosts))
			map_one_turn = map_nums / num_hosts
			predict_turn = 1
			if map_one_turn <= 1:
				continue
			train_set = filter(lambda x: x[0] < predict_turn * num_hosts, tmp)
			predict_set = filter(lambda x: x[0] >= predict_turn * num_hosts, tmp)
			train_set_X = map(lambda x: list(x)[0:3], train_set)
			train_set_Y = map(lambda x: list(x)[-1], train_set)
			predict_set_X = map(lambda x: list(x)[0:3], predict_set)
			predict_set_Y = map(lambda x: list(x)[-1], predict_set)
			model = Train(map_nums, len(reduces_size), 0.2)
			model.train(train_set_X, train_set_Y)

			# do predict
			pre_y = model.predict(predict_set_X)
			for i in range(len(predict_set)):
				predict_set[i] = (predict_set[i][0], predict_set[i][1], predict_set[i][2], pre_y[i])
			whole_list = map(lambda x: list(x), (train_set + predict_set))
			predict_df = pd.DataFrame(data=whole_list, columns=['mapid', 'hostid', 'reduceid', 'size'])
			predict_reduce = predict_df.groupby(['reduceid']).sum()
			predict_reduce_size = zip(predict_reduce.index.values, predict_reduce['size'].values)

			# evaluation
			precentage = map(lambda x, y: (x[-1] - float(y[-1])) / float(y[-1]), predict_reduce_size, reduces_size)
			std = np.std(precentage)
			avg = np.average(precentage)
			print 'job: {} shuffle: {} avg: {} std: {}'.format(app_id, s_id, avg, std)
						








if __name__ == '__main__':
	main()
