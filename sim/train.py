#!/usr/bin/env python

import numpy as np


class Train:
	def __init__(self, map_num, reduce_num):
		self.map_num = map_num
		self.reduce_num = reduce_num
		self.para_matrix = np.zeros((3, reduce_num))

	# samples_in and samples_out are matrices which have same row number
	# e.g.
	# samples_in: [[mapid_0, host_0, mapsize_0], ... [mapid_n, host_n, mapsize_n]]
	# samples_out: [[reduce_size_1, ... reduce_size_reduce_num], ... [reduce_size_1, ... reduce_size_reduce_num]]
	def train(samples_in, samples_out):
