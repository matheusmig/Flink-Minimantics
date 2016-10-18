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

## Variáveis Globais
args = []  				 #Argumentos. OBS: para transformar em um dictionary, usar => vars(opts)['key']
scConf = None 			 #Config do SparkContext
sc = None 				 #SparkContext
bGenerateSteps = False   #Flag que indica se deve gerar arquivos durante as etapas intermediárias, senão, apenas salvará o resultado final em arquivo.


# """
# Name: inputOpt
# 
# Process the input arguments and return it in a list
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def inputArgs():
	parser = argparse.ArgumentParser()
	parser.add_argument('-i', '--input', 					     	dest='InFile') 					#default action is store the value
	parser.add_argument('-o', '--output',					     	dest='OutFile')
	parser.add_argument('-steps', action='store_const', const=True, dest='GenerateSteps')           #Flag que indica se deve gerar arquivos durante as etapas intermediárias
	parser.add_argument('-a',            default="cond_prob",    	dest='AssocName')   			#used in CalculateSimilarity
	parser.add_argument('-s',                      default='',   	dest='Scores')			        #used in CalculateSimilarity
	parser.add_argument('-t',                      default=[],   	dest='TargetsWordsFiltered')    #used in CalculateSimilarity
	parser.add_argument('-n',                      default=[],   	dest='NeighborWordsFiltered')   #used in CalculateSimilarity
	parser.add_argument('-c',                      default=[],   	dest='ContextWordsFiltered')    #used in CalculateSimilarity
	parser.add_argument('-FW',           type=int, default=0,    	dest='FilterWordThresh') 		#used in FilterRaw
	parser.add_argument('-FP',           type=int, default=0,    	dest='FilterPairThresh') 		#used in FilterRaw
	parser.add_argument('-A',            type=float, default=-99999, 	dest='AssocThresh')             #used in CalculateSimilarity
	parser.add_argument('-S',            type=float, default=-99999, 	dest='SimThresh')  				#used in CalculateSimilarity
	parser.add_argument('-D',            type=float, default=-99999, 	dest='DistThresh')              #used in CalculateSimilarity
	args = parser.parse_args()
	return args

# """
# Name: verifyFlagSteps
# 
# Process argument 
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def verifyFlagSteps():
	bGenerateSteps = vars(args)['GenerateSteps'];
	return bGenerateSteps;

# """
# Name: process
# 
# Here the magic is made
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def process( master ):
	if ((args != None) and (vars(args)['InFile'] != None)
	                   and (vars(args)['OutFile'] != None)):

		"""
		Reading Input File
		"""
		print ('Reading input file...' );
		strInputFile  = vars(args)['InFile'];
		stroutputFile = vars(args)['OutFile'];
		data 		 = env.read_text(strInputFile); #Lê do arquivo
		print ('\n------------------ COMEÇANDO PROCESSAMENTO!!! ------------')


		"""
		Filter Raw
		"""
#		print ('*** STEP 1 : FILTERING RAW ***')
#		filterRawOutput = filterRawInput(env, data, args);
		"""
		Build Profiles
		"""
		buildProfilesOutput = ""
#		print ('*** STEP 2 : BUILDING PROFILES ***')
#		buildProfilesOutput = buildProfiles(env, filterRawOutput, args);


		"""
		Calculate Similarity
		"""
		print ('*** STEP 3 : CALCULATING SIMILARITY ***')
		calculateSimilarity(env, buildProfilesOutput, args);



		print ('------------------ FINAL DO PROCESSAMENTO!!! ------------\n')

	else:
		print ('Input File and/or Output File aren\'t defined')



# """
# Name: Main function
# 
# Read the arguments and call 'process'
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def main( opts ):
	#Read input options
	print ("\n------------------ Reading arguments ------------------ ")
	global args  # modificador global permite acessar variável definida fora da funcao
	args = inputArgs()  

	#flag to save intermediate files in processing
	if verifyFlagSteps():
		print ("[ Will generate output files at each step ]")
	else:
		print ("[ Only will generate output file at the end ]")

	#Registra custom types 
	env.register_type(Profile   	, ProfileSerializer()   	, ProfileDeserializer());
	env.register_type(Similarity    , SimilaritySerializer()	, SimilarityDeserializer());
	env.register_type(DictOfContexts, DictOfContextsSerializer(), DictOfContextsDeserializer());

	#PROCESS
	process (None)

if __name__ == "__main__":
	"""
	Start Flink Environment
	"""
	env = get_environment()

	"""
	Main Method
	"""
	main(sys.argv);

	"""
	Execute
	"""
	env.execute(local=True)


