#!/usr/bin/env python
# -*- coding: utf-8 -*- 

# Calculate similarity - Python

## Imports
from flink.plan.Environment import get_environment
from flink.plan.DataSet import *
from operator import add

from Minimantics.utils import *
from Minimantics.DataTypes import *
from Minimantics.FilterRaw import *
from Minimantics.BuildProfiles import *
from Minimantics.CalculateSimilarity import *

import sys, argparse
from datetime import datetime
from ast import literal_eval

# """
# Name: inputArgs
# 
# Process the input arguments and return it in a list
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def inputArgs():
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input',                                            dest='InFile') 		            #Nome do arquivo de entrada
	parser.add_argument('-o', '--output',                              default='',  dest='OutFile')		            #Nome do arquivo de saída
	parser.add_argument('--steps',              action='store_const', const=False,  dest='GenerateSteps')           #Flag que indica se deve gerar TODOS arquivos intermediários de saída durante as etapas do algoritmo
	parser.add_argument('--sort',               action='store_const', const=False,  dest='SortOutput')              #Flag que indica se o arquivo de saída gravado será ordenado
	parser.add_argument('--stage',                                     default='',  dest='StageSelector')           #Flag que indica quais estágios do algritmo iremos rodar. '' = todos, FR = FilterRaw, BP = BuildProfiles, CS = CalcSimilarity
	parser.add_argument('-a',                                 default="cond_prob",  dest='AssocName')   	        #used in CalculateSimilarity
	parser.add_argument('-s',                                          default='',  dest='Scores')		            #used in CalculateSimilarity
	parser.add_argument('-t',                                          default=[],  dest='TargetsWordsFiltered')    #used in CalculateSimilarity. Lista de targets que serão ignorados e removidos durante processamento
	parser.add_argument('-n',                                          default=[],  dest='NeighborWordsFiltered')   #used in CalculateSimilarity. Lista de neighbors que serão ignorados e removidos durante processamento
	parser.add_argument('-c',                                          default=[],  dest='ContextWordsFiltered')    #used in CalculateSimilarity. Lista de contexts que serão ignorados e removidos durante processamento
	parser.add_argument('-A',                         type=float, default=-9999.0,  dest='AssocThresh')             #used in CalculateSimilarity. Threshold mínimo para a medida de associação entre um target e um context, pares de target,context que tiverem uma força de associação abaixo disso serão filtrado fora.
	parser.add_argument('-S',                         type=float, default=-9999.0,  dest='SimThresh')  		        #used in CalculateSimilarity. Threshold mínimo para as medidas de similaridade, targets com medidas abaixo disso serão filtrados fora.
	parser.add_argument('-D',                         type=float, default=-9999.0,  dest='DistThresh')              #used in CalculateSimilarity. Threshold máximo para as medidas de distância, targets com medidas a cima disso serão filtrados fora.
	parser.add_argument('--calculate_distances', action='store_const', const=True,  dest='CalculateDistances')      #used in CalculateSimilarity. Flag que indica se calcularemos todas as medidas de distância, senão mediremos apenas as medidas de similaridade 
	parser.add_argument('--only_cosines',        action='store_const', const=True,  dest='OnlyCosines')             #Flag que indica se queremos gerar o arquivo de saída contendo como unica medida de similaridade a similiridade por coseno 
	parser.add_argument('-FW',                         type=int,        default=0,  dest='FilterWordThresh')        #used in FilterRaw. Número mínimo de vezes que uma palavra tem que aparecer no arquivo de entrada para ter sua semelhança calculada.
	parser.add_argument('-FP',                         type=int,        default=0,  dest='FilterPairThresh')        #used in FilterRaw. Número mínimo de vezes que uma dupla de palavras deve repetir-se no arquivo de arquivo de entrada, para que seja levada em consideração .
	parser.add_argument('-P',                          type=int,        default=1,  dest='FlinkParallelism')        #Set Flink environment parallelism level
	parser.add_argument('--local',              action='store_const', const=False,  dest='FlinkLocalExecution')     #Set Flink to run locally
	
	args, unknown = parser.parse_known_args()
	return args,unknown;


# """
# Name: process
# 
# Here the magic is made
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def process( args ):
	"""
	Start Flink Environment
	"""
	if ((args == None) and (vars(args)['InFile'] == None)
	                   and (vars(args)['OutFile'] == None)):
		sys.exit('Input File and/or Output File aren\'t defined')

	env = get_environment()
	nParallelism = vars(args)['FlinkParallelism'];
	env.set_parallelism(nParallelism);

	"""
	Custom types 
	"""
	env.register_type(Profile   	, ProfileSerializer()   	, ProfileDeserializer());
	env.register_type(Similarity    , SimilaritySerializer()	, SimilarityDeserializer());
	env.register_type(DictOfContexts, DictOfContextsSerializer(), DictOfContextsDeserializer());
	
	"""
	Input File
	"""
	strInputFile   = vars(args)['InFile'];
	strOutputFile  = vars(args)['OutFile'];
	bStageSelector = vars(args)['StageSelector']
	data 		   = env.read_text(strInputFile); #Lê do arquivo

	print ('\n------------------ PROCESSING!!! ------------')

	#only FilterRaw stage
	if bStageSelector == 'FR':
		output = filterRawInput(env, data, args);

	#only BuildProfiles stage
	elif bStageSelector == 'BP':
		data   = data.map( lambda line:  tuple(word.strip() for word in line.replace('(','').replace(')','').split(',')))\
				     .map( lambda tuple: ((tuple[0], tuple[1]),tuple[2])) ;
		output = buildProfiles(env, data, args);

	#only CalculateSimilarity stage
	elif bStageSelector == 'CS':
		output = calculateSimilarity(env, data, args);

	#ALL stages
	else:
		filterRawOutput     = filterRawInput(env, data, args);
		buildProfilesOutput = buildProfiles(env, filterRawOutput, args);
		output              = calculateSimilarity(env, buildProfilesOutput, args);


	output.write_text(strOutputFile, WriteMode.OVERWRITE );

	print ('------------------ END OF PROCESS!!! ------------\n')

	"""
	Execute
	"""
	if vars(args)['FlinkLocalExecution']:
		bRunLocal = True
	else:
		bRunLocal = False
	env.execute(bRunLocal)

# """
# Name: Main function
# 
# Read the arguments and call 'process'
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def main( opts ):
	"""
	Read input arguments
	"""
	args, unknown = inputArgs()  

	if not unknown:
		#PROCESS
		process (args)
	else:
		sys.exit("Unrecognized arguments: "+str(unknown));

if __name__ == "__main__":

	"""
	Main Method
	"""
	main(sys.argv);


