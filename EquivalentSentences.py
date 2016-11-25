
# -*- coding: utf-8 -*- 
import sys, os, argparse

from flink.plan.Environment import get_environment
from flink.plan.DataSet import *

from flink.functions.MapFunction         import MapFunction
from flink.functions.FlatMapFunction     import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.CoGroupFunction     import CoGroupFunction
from flink.functions.JoinFunction        import JoinFunction
from flink.functions.ReduceFunction      import ReduceFunction
from flink.functions.FilterFunction      import FilterFunction
from flink.plan.Constants import Order

import random
from decimal import *


# Plain implementation of Flink Python - Minimantics

__author__ = 'matheusmignoni'

# """
# Name: inputArgs
# 
# Process the input arguments and return it in a list
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def inputArgs():
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input',         dest='InFile') 		        #Nome do arquivo de entrada
	parser.add_argument('-o', '--output',        dest='OutFile')		        #Nome do arquivo de saída
	parser.add_argument('-w',         	        dest='Word')		         	#palavra a ser substituída	
	parser.add_argument('-s', type=int, default=2, dest='Substitutions') 
	args, unknown = parser.parse_known_args()
	return args,unknown;

# """
# Name: NothingReduce
# 
# Classe utilizada para especificar uma GroupReduceFunction
# Repassa todos os valores adiante
# 
#
# Author: 23/07/2016 Matheus Mignoni
# """
class NothingReduce2(GroupReduceFunction):
	def reduce(self, iterator, collector):
		while iterator.has_next():
			value = iterator.next();
			collector.collect(value);

class NothingReduce(GroupReduceFunction):
	def reduce(self, iterator, collector):
		value = iterator.next();
		collector.collect(value[1]);


class Adder(GroupReduceFunction):
	def reduce(self, iterator, collector):
		count, word = iterator.next()
		count += sum([x[0] for x in iterator])
		collector.collect((count, word))

"""
Main
"""
if __name__ == "__main__":

    # get the args via parameters
	args, unknown = inputArgs()

    # get paths to input and output files
	strInputFile 	= vars(args)['InFile'];
	strOutputFile 	= vars(args)['OutFile'];

	# get other args
	wordToBeReplaced   = vars(args)['Word']
	substitutionsCount = vars(args)['Substitutions']

    # remove the output file, if there is one there already
	if os.path.isfile(strOutputFile):
		os.remove(strOutputFile)

    # set up the environment with a text file source
	env = get_environment()
	data = env.read_text(strInputFile); #Lê do arquivo

	# Sentença a ser analisada, transforma em lista de palavras
	#sentence = env.read_text("/Users/mmignoni/Desktop/EtapaIV/Sentence.txt").map(lambda line: line.split(" ")).output();
	sentence = "Most English sentences follow a similar structure";

	# Transforma entrada em tripla => (word, context, cosine)
	triple = data.map(lambda line: line.split("\t")).map(lambda line: (line[0], line[1], format(float(line[2]), '.6f')  ));

	# Busca apenas as triplas da palavra. E ordena por ordem de maiores cosenos
	synonyms = triple.filter(lambda triple: triple[0] == wordToBeReplaced)\
							.group_by(0)\
							.sort_group(2, Order.DESCENDING)\
							.reduce_group(NothingReduce())\
							.first(substitutionsCount);

	EquivalentSentences = synonyms.flat_map(lambda word,c: [sentence.replace(wordToBeReplaced, word)] )
	EquivalentSentences.write_text(strOutputFile, WriteMode.OVERWRITE );

    # execute the plan locally.
	env.execute(local=True)