#!/usr/bin/env python
# -*- coding: utf-8 -*- 

""" 
Calculate Similarity Module
"""
from .utils import *
from .DataTypes import *

from operator import add
from math import *
from random import *
from flink.plan.DataSet import *

"""
   OBS: Esta função apenas leva em consideração os itens com cabeçalho:
   target, id_target, context, id_context, f_tc, f_t, f_c, cond_prob, pmi, npmi, lmi, tscore, zscore, dice, chisquare, loglike,
   affinity, entropy_target, entropy_context 
"""

#Argumentos
#
bCalculateDistance = True;

#Context Dictionary - contém o valor associado a um context
contextDictionary = {}


# """
# Name: calculateSimilarity
# 
#
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def calculateSimilarity(env, buildProfilesOutput, args):
	"""
	" Inicialização de variáveis conforme argumentos de entrada "
	"""
	strOutputFile = vars(args)['OutFile'];
	bSaveOutput 				= vars(args)['GenerateSteps']
	AssocName   				= vars(args)['AssocName']	
	minScores 					= vars(args)['Scores']				     
	lstTargetsWordsFiltered		= vars(args)['TargetsWordsFiltered']
	lstNeighborWordsFiltered	= vars(args)['NeighborWordsFiltered']
	lstContextWordsFiltered		= vars(args)['ContextWordsFiltered'] 
	nAssocThreshold				= vars(args)['AssocThresh']         
	nSimThreshold				= vars(args)['SimThresh']  			
	nDistThreshold				= vars(args)['DistThresh']     


	"""
	" Lê entrada
	"""
	if bSaveOutput: # Entrada da função será lida de arquivo
		profiles = env.read_text("/Users/mmignoni/Desktop/TCC/mini.1.profiles" ).map(lambda line: (line.split("\t")));
	else: 		    # Entrada é recebida em memória 
		profiles = buildProfilesOutput;

	"""
	" Processa entrada, extraindo header e filtrando dados
	"""

#   VERSAO 2 --> header não é mais utilizado
#
#	#Extrai Header e suas informações
#	header = profiles.first(1)  
#	if bSaveOutput:
#		#Veio do arquivo, contém cabecalho
#		lstHeader = header; 
#	else:
#		#Veio da memória, o cabeçalho está com os elementos separados por virgula
#		lstHeader = header.split(" "); #Transforma header em uma lista
#
#	#Conseguiremos acessar a posição correta dos itens, através da sua localização no header.
#	#p.ex, se quisermos pegar o target, basta acessamos o indice contido em header.index('target'));


	targetIndex 			= 0  #lstHeader.index('target');
	contextIndex 			= 2  #lstHeader.index('context')
	targetContextCountIndex	= 4  #lstHeader.index('f_tc')
	targetCountIndex 		= 5  #lstHeader.index('f_t')
	contextCountIndex		= 6  #lstHeader.index('f_c')
	entropy_targetIndex		= 7  #lstHeader.index('entropy_target')
	entropy_contextIndex 	= 8  #lstHeader.index('entropy_context')
	condProbIndex	  		= 9  #lstHeader.index('cond_prob')
	pmiIndex 				= 10  #lstHeader.index('pmi')
	npmiIndex	      		= 11  #lstHeader.index('npmi')
	lmiIndex 				= 12 #lstHeader.index('lmi')
	tscoreIndex	    		= 13 #lstHeader.index('tscore')
	zscoreIndex	    		= 14 #lstHeader.index('zscore')
	diceIndex	      		= 15 #lstHeader.index('dice')
	chisquareIndex	 		= 16 #lstHeader.index('chisquare')
	loglikeIndex 			= 17 #lstHeader.index('loglike')
	affinityIndex	  		= 18 #lstHeader.index('affinity')

	""" 
	" Dados 
	"""

	""" 
	Filtra dados: 
	   - score menores que o limite de AssocThresh
	   - targets na lista de targets a ignorar
	   - context na lista de context a ignorar
	   - targets na lista de neighboors a ignorar 
	"""
	filteredData = profiles.filter( lambda tuple: float(tuple[targetContextCountIndex]) >= nAssocThreshold)\
	                       .filter( FilterFromList(targetIndex , lstTargetsWordsFiltered))\
	                       .filter( FilterFromList(contextIndex, lstContextWordsFiltered))\
	                       .filter( FilterFromList(targetIndex , lstNeighborWordsFiltered));

	if bSaveOutput:
		filteredData.write_text(strOutputFile+".SimilarityFilteredData.txt", WriteMode.OVERWRITE );

	""" O código original em C, utilizava uma estrutura chamada targets_context para armazenar os targets, suas somas e sua lista de contexts. 
	# Replicaremos o mesmo comportamente através de um simples tupla, para facilitar processamento (apesar de dificultar manutenção(?))
	# Tupla é (key,value) onde: 
	# 	key   = target 
	# 	value = (sum, sum_square, contextsList) 
	#
	# contextsList é uma lista de (context, valor)
	#
	# Primeiramente mapearemos para as tuplas e depois faremos o agrupamento dos targets iguais """
	targetContexts = filteredData.map(lambda tuple: (tuple[targetIndex], (float(tuple[targetContextCountIndex]), float(tuple[targetContextCountIndex])*float(tuple[targetContextCountIndex]), ( tuple[contextIndex], float(tuple[targetContextCountIndex]) ) ) ))\
							     .group_by(0).reduce_group(TargetContextsGrouper());
    

	if bSaveOutput:
		targetContexts.write_text(strOutputFile+".SimilarityTargetContextes.txt", WriteMode.OVERWRITE );


	""" Cacula similaridade """
	# Faz a combinação cartesiana de todos os targets, junto com sua soma, soma quadrática e lista de contexts
	targetsCartesian = targetContexts.cross(targetContexts);

	if bSaveOutput:
		targetsCartesian.write_text(strOutputFile+".SimilarityTargetsCartesian.txt", WriteMode.OVERWRITE );

	# TODO: é possível aumentar a eficiência dessa operação?
	# OBS:
	# target1      = i[0][0]; 
	# sum1 		   = i[0][1][0];
	# sum_square1  = i[0][1][1];
	# contextDict1 = i[0][1][2];
	# target2      = i[1][0];
	# sum2 		   = i[1][1][0];
	# sum_square2  = i[1][1][1];
	# contextDict2 = i[1][1][2];
	CalculatedSimilarities = targetsCartesian.flat_map(Similaritier(True));
														                  

	""" Processa formato de saída """

	OutputData = CalculatedSimilarities.filter(OutputSim(lstTargetsWordsFiltered, lstNeighborWordsFiltered, nSimThreshold, nDistThreshold))\
									   .map(lambda similarity : similarity.returnResultAsStr());



	if bSaveOutput:
		#OutputHeader = env.from_elements(Similarity.returnHeader());
		#OutputHeader.write_text(strOutputFile+".CalculatesSimilarityOutput.txt", WriteMode.OVERWRITE );
		OutputData.write_text(strOutputFile+".CalculatesSimilarityOutput.txt", WriteMode.OVERWRITE );
		return OutputData;
	else:
		return OutputData;




