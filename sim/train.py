#!/usr/bin/env python

import numpy as np
from sklearn import linear_model
import pandas as pd
import os

tmp_dir = '/mnt/d/tmp'


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

# df_map [host, mapid, size]
# df_reduce [mapid, reduceid, prob]
# return [host, reduceid, pred_size, pre]
def predict_with_sample(df_map, df_reduce):
	map_sum = np.sum(df_map['size'].values)
	map_num = len(df_map['mapid'].values)
	map_size = df_map[['mapid', 'size']].values.tolist()
	map_size = sorted(map_size, key=lambda x:x[0])
	res = []
	reduceid = list(set(df_reduce['reduceid'].values.tolist()))
	for rid in reduceid:
		# prob = df_reduce.loc[df_reduce['reduceid'] == rid][['mapid', 'prob']].values.tolist()
		prob = df_reduce.loc[df_reduce['reduceid'] == rid][['mapid', 'prob']]
		join_df = prob.join(df_map.set_index('mapid'), how='left', on='mapid').values.tolist()
		pre_size = np.sum(map(lambda x: float(x[3]) * float(x[1]), join_df))
		max_host = 0
		max_pre = 0
		# find max
		for p in join_df:
			pre = float(p[1]) * float(p[3]) / pre_size 
			if pre > max_pre:
				max_pre = pre
				max_host = p[2]
		res.append([max_host, rid, pre_size, max_pre])
		# map_size = map_line[0][2]
		# host = map_line[0][0]
		# reduce_size = map_size * prob + (map_sum - map_size) / (map_num - 1) * (1 - prob)
		# res.append([host, rid, reduce_size])
	# print res
	return res



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
	distribution = []	
	dis_dic = {}
	for root, dirs, names in os.walk(file_dir):
		if 'sim' in root:
			break
		for file in names:
			if 'csv' in file:
				continue
			arrays = file.split('-')
			application = arrays[1]
			exe_id = int(arrays[-1])
			f = open(os.path.join(root, file))
			sid = 0
			for line in f:
				if 'frankfzw: shuffleid' in line:
					datas = line.split()
					s_id = int(datas[6])
					m_id = int(datas[8])
					r_id = int(datas[10])
					s = long(datas[12])
					res.append([application, s_id, m_id, exe_id, r_id, s])
					sid = s_id
				if 'reduce distribution' in line:
					datas = line.split()
					m_id = int(datas[6])
					prob = map(lambda x: int(x), datas[9:])
					total_prob = np.sum(prob)
					for i in range(len(prob)):
						p = float(prob[i])/float(total_prob)
						entry = [application, sid, m_id, i, p]
						key = (application, sid, m_id, i)
						if key not in dis_dic:
							distribution.append(entry)
							dis_dic[key] = 1

	df = pd.DataFrame(data=res, columns=['jobid', 'shuffleid', 'mapid', 'host', 'reduceid', 'size'])
	df_dis = pd.DataFrame(data=distribution, columns=['jobid', 'shuffleid', 'mapid', 'reduceid', 'prob'])
	return (df, df_dis)

def extend_array(a, b):
	a.extend(b)
	return a

def normalize(arr):
	new_arr = np.asarray(arr)
	min_val = np.amin(new_arr)
	max_val = np.amax(new_arr)
	ret = map(lambda x: float(x - min_val)/float(max_val - min_val), arr)
	return ret

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
	file_path = '/mnt/d/Spark/evaluation/spark/single shuffle'
	# file_path = '/home/frankfzw/Spark/evaluation/spark/single shuffle'
	res = []
	map_res = []
	df, df_dis = parse_spark_log(file_path)
	# print df_dis
	applications = df.groupby(['jobid']).groups.keys()
	for app_id in applications:
		if app_id != '20170324093238':
		# if app_id != '20170324093817':
			continue
		app_data = df.loc[df['jobid'] == app_id]
		shuffles = app_data.groupby(['shuffleid']).groups.keys()
		app_data.to_csv('{}/{}_out.csv'.format(tmp_dir, app_id))
		reduce_array = []
		for s_id in shuffles:
			# if s_id != 1 or app_id != '20170313113940':
			# 	continue
			shuffle_data = app_data.loc[app_data['shuffleid'] == s_id]

			# calculate the final reduce size
			reduces = shuffle_data.groupby(['reduceid']).sum()
			reduces_size = zip(reduces.index.values, reduces['size'].values)
			save_array = map(lambda x: extend_array([app_id, s_id], x), reduces_size)
			reduce_array.extend(save_array)

			# calculate the map output size
			maps = shuffle_data.groupby(['mapid']).sum()
			tmp_map = zip(maps.index.values, reduces['size'].values)
			map_size = {}
			for d in tmp_map:
				map_size[d[0]] = d[1]
			
			
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

			# normalize reduce
			norsize = normalize(reduces['size'].values.tolist())

			# build reduce distribution
			reduce_distribution = df_dis[(df_dis.jobid == app_id) & (df_dis.shuffleid == s_id)]
			if len(reduce_distribution.index.values) > 0:
				tmp_map = shuffle_data.groupby(['mapid']).sum()
				tmp_map_arrays = zip(tmp_map.index.values, tmp_map['size'].values)
				df_map_arrays = map(lambda x: \
					extend_array(list(set(shuffle_data.loc[shuffle_data['mapid'] == x[0]]['host'].values)), list(x)), tmp_map_arrays)
				df_map = pd.DataFrame(data=df_map_arrays, columns=['host', 'mapid', 'size'])
				df_reduce = reduce_distribution[['mapid', 'reduceid', 'prob']]
				# print df_map
				# print df_reduce
				result = predict_with_sample(df_map, df_reduce)
				pre_df = pd.DataFrame(data=result, columns=['host', 'reduceid', 'pre_size', 'prob'])
				pre_norsize = normalize(pre_df['pre_size'].values.tolist())
				pre_df['pre_nor'] = pd.Series(pre_norsize, index=pre_df.index)
				pre_df.to_csv('{}/{}_{}_sample_pre_reduce.csv'.format(tmp_dir, app_id, s_id))

				# evaluation
				diff = map(lambda x, y: abs(x - y), norsize, pre_norsize)
				std = np.std(norsize)
				avg = np.average(norsize)
				print 'sampling job: {} shuffle: {} avg: {} std: {}'.format(app_id, s_id, avg, std)
			# else:
			train_set = filter(lambda x: x[0] < predict_turn * num_hosts, tmp)
			predict_set = filter(lambda x: x[0] >= predict_turn * num_hosts, tmp)
			train_set_X = map(lambda x: [x[2], map_size[x[0]]], train_set)
			train_set_Y = map(lambda x: x[3], train_set)
			predict_set_X = map(lambda x: [x[2], map_size[x[0]]], predict_set)
			predict_set_Y = map(lambda x: x[3], predict_set)
			model = Train(map_nums, len(reduces_size), 0.2)
			# print train_set_X
			# print train_set_Y
			model.train(train_set_X, train_set_Y)

			# normalize map
			observation_df = pd.DataFrame(data=train_set, columns=['mapid', 'host', 'reduceid', 'size'])
			observation_reduce = observation_df.groupby(['reduceid']).sum()
			nor_ob = normalize(observation_reduce['size'].values.tolist())
			observation = zip(observation_reduce.index.values, observation_reduce['size'].values, nor_ob)
			observation_df = pd.DataFrame(data=observation, columns=['reduceid', 'observation_size', 'nor_size'])
			observation_df.to_csv('{}/{}_{}_observation_map.csv'.format(tmp_dir, app_id, s_id))

			# do predict
			pre_y = model.predict(predict_set_X)
			for i in range(len(predict_set)):
				predict_set[i] = (predict_set[i][0], predict_set[i][1], predict_set[i][2], pre_y[i])
			whole_list = map(lambda x: list(x), (train_set + predict_set))
			predict_df = pd.DataFrame(data=whole_list, columns=['mapid', 'hostid', 'reduceid', 'pre_size'])
			predict_reduce = predict_df.groupby(['reduceid']).sum()
			pre_norsize = normalize(predict_reduce['pre_size'].values.tolist())
			predict_reduce_size = zip(predict_reduce.index.values, predict_reduce['pre_size'].values, pre_norsize)

			# evaluation
			diff = map(lambda x, y: abs(x - y), norsize, pre_norsize)
			std = np.std(diff)
			avg = np.average(diff)
			print 'job: {} shuffle: {} avg: {} std: {}'.format(app_id, s_id, avg, std)

			# generate output
			output = map(lambda x, y, a, b: [app_id, s_id, x[0], x[1], a, y[1], b], reduces_size, predict_reduce_size, norsize, pre_norsize)
			res += output

		# save_df = pd.DataFrame(data=reduce_array, columns=['jobid', 'shuffleid', 'reduceid', 'size'])
		# save_df.to_csv('{}/{}_reduce.csv'.format(tmp_dir, app_id))
	out_df = pd.DataFrame(data=res, columns=['appid', 'shuffleid', 'reduceid', 'size', 'nor', 'predictsize', 'pre_nor'])
	out_df.to_csv('{}/pre_reduce.csv'.format(tmp_dir))
	# save_df = pd.DataFrame(data=res, columns=['appid', 'shuffleid', 'reduceid', 'reducesize', 'predictsize'])					
	# save_df.to_csv('{}/out.csv'.format(file_path))








if __name__ == '__main__':
	main()
