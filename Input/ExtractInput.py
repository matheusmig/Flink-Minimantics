#!/usr/bin/env python
# -*- coding: utf-8 -*- 

# Extract Input from Corpus

## Imports
import sys, argparse, os, re

from flink.plan.Environment import get_environment
from flink.plan.Constants import WriteMode
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction

# """
# Name: inputArgs
# 
# Process the input arguments and return it in a list
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def inputArgs():
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input',  dest='InFile')        #Nome do arquivo de entrada
	parser.add_argument('-o', '--output', dest='OutFile')       #Nome do arquivo de saída
	args, unknown = parser.parse_known_args()
	return args,unknown;

class LineZipper(FlatMapFunction):
	def flat_map(self, line, collector):
		for w1, w2 in zip(line.split(), line.split()[1:]):
			w1 = re.sub('[^A-Za-z0-9]+', '', w1) #Remove caracteres especiais w1
			w2 = re.sub('[^A-Za-z0-9]+', '', w2) #Remove caracteres especiais w2
			if (len(w1) > 0) and (len(w2) > 0): 
				collector.collect( (w1, w2) );


if __name__ == "__main__":
	# get the base path out of the runtime params
	basePath = sys.argv[0]

	# read input arguments
	args, unknown = inputArgs() 

	# set paths
	corpusPath = vars(args)['InFile'];
	outputPath = vars(args)['OutFile'];

	# set up the environment with a source (in this case from a text file)
	env = get_environment()
	data = env.read_text(corpusPath)

	# build the job flow
	data\
	.flat_map(LineZipper())\
	.write_csv(outputPath, line_delimiter='\n', field_delimiter=' ', write_mode=WriteMode.OVERWRITE );

	# execute
	env.execute(local=True)