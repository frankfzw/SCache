#!/usr/bin/env python

import numpy as np
from sklearn import linear_model
import pandas as pd


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


def main():
	file_path = '/home/frankfzw/SCache/sim/master-spark0-log.out'
	df = parse_log(file_path)
	map_ids = df.groupby(['mapid']).groups.keys()
	reduce_ids = df.groupby(['reduceid']).groups.keys()
	map_sizes = [134217728, 134217728, 134217728, 134217728, 134217728, 134217728, 76885773]
	hosts = []
	Y = []
	X = []
	for m in map_ids:
		hosts.append(df.loc[df['mapid'] == m]['host'].values[0])
		Y.append(df.loc[df['mapid'] == m]['size'].values)

	tmp = zip(map_ids, hosts, map_sizes)
	for x in tmp:
		X.append(list(x))

	model = Train(len(map_ids), len(reduce_ids), 0.5)
	model.train(X[0:2], Y[0:2])
	print model.score(X[2:], Y[2:])
	



if __name__ == '__main__':
	main()
