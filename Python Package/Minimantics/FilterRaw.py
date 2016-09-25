#!/usr/bin/env python
# -*- coding: utf-8 -*- 

""" 
Filter Raw Module
"""
from flink.plan.Environment import get_environment
from flink.plan.DataSet import *
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.plan.Constants import Order

from .utils     import *


# """
# Name: filter
# 
# Retorna a entrada filtrada
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def filterRawInput(env, inputFile, args):
	"""
	" Inicialização de variáveis a partir do argumento de entrada "
	"""
	wordLengthThreshold = vars(args)['FilterWordThresh']
	pairCountThreshold  = vars(args)['FilterPairThresh']
	bSaveOutput         = vars(args)['GenerateSteps']
	strOutputFile       = vars(args)['OutFile'];

	"""
	" Lê input file linha a linha, dividindo a linha em uma tupla chamada pairWords = (targetWord, contextWord) "
	"""
	pairWords = inputFile.map( lambda line: line.split(" ") )\
						 .map( lambda tuple: (tuple[0].lower().strip(), tuple[1].lower().strip()));
	
	"""
	"Filtering, sorting and uniquing raw triples in inputfile"
	"""
#	print('Filtering Input...');
	
	""" targets word count, filtering targets that occur more than threshold, with size greater than 1 and sorting by key alphabetcally"""
	#file.1.targets.filter10 => targetsFiltered
	targetsFiltered = pairWords.map( lambda tuple : (tuple[0], 1) ) \
						.group_by(0).reduce_group(Adder())\
						.filter(lambda targetTuple: len(targetTuple[0]) > 1 and targetTuple[1] > int(wordLengthThreshold))\
						.project(0);
	
	if bSaveOutput:
		targetsFiltered.write_text(strOutputFile+".targets.filter"+str(wordLengthThreshold)+".txt", WriteMode.OVERWRITE );
	
	""" contexts word count, filtering targets that occur more than threshold, with size greater than 1 and sorting by key alphabetcally"""
	#file.1.contexts.filter10 => contextsFiltered
	contextsFiltered = pairWords.map( lambda tuple : (tuple[1], 1) ) \
						.group_by(0).reduce_group(Adder())\
						.filter(lambda contextTuple: len(contextTuple[0]) > 1 and contextTuple[1] > int(wordLengthThreshold))\
						.project(0);

	if bSaveOutput:
		contextsFiltered.write_text(strOutputFile+".contexts.filter"+str(wordLengthThreshold)+".txt", WriteMode.OVERWRITE );


	"""
	" Joining "
	"""	
	
	""" select only triples containing verbs from the filtered list, sort in object order """
	""" and filter keeping only triples whose objects are in the filtered list """
	print('Starting Join...' )

	#file.1.filter.t10.c10 => PairWordsFiltered
	PairWordsFiltered  =  pairWords.join(targetsFiltered).where(0).equal_to(0).project_first(0,1)\
								   .join(contextsFiltered).where(1).equal_to(0).project_first(0,1);
								   #.group_by(0)\
								   #.sort_group(0, Order.DESCENDING )\
								   #.reduce_group(NothingReduce());
	
	if bSaveOutput:		
		PairWordsFiltered.write_text(strOutputFile+".filter.t"+str(wordLengthThreshold)+".c"+str(wordLengthThreshold)+".txt",  WriteMode.OVERWRITE );


	""" uniquing and couting """
	#file.1.filter.t10.c10.tc2.u => sortedPairWordsFilteredUniqueCount
	PairWordsFilteredUniqueCount = PairWordsFiltered.map( lambda tuple : ( (tuple[0], tuple[1]) , 1) )\
													.group_by(0).reduce(Adder())\
													.filter(lambda tuple : tuple[1] >= pairCountThreshold)\
													.group_by(0)\
													.sort_group((lambda tuple : tuple[0][0]), Order.ASCENDING )\
													.sort_group((lambda tuple : tuple[0][1]), Order.ASCENDING )\
													.reduce_group(NothingReduce());
										            
	
	if bSaveOutput:
		PairWordsFilteredUniqueCount.write_text(strOutputFile+".filterRawOutput.txt", WriteMode.OVERWRITE );
	
	return PairWordsFilteredUniqueCount

	