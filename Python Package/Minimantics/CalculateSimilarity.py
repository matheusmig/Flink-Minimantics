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
	strOutputFile               = vars(args)['OutFile'];
	bGenSteps 				    = vars(args)['GenerateSteps']
	bCalculateDistance          = vars(args)['CalculateDistances']
	AssocName   				= vars(args)['AssocName']
	minScores 					= vars(args)['Scores']				     
	lstTargetsWordsFiltered		= vars(args)['TargetsWordsFiltered']
	lstNeighborWordsFiltered	= vars(args)['NeighborWordsFiltered']
	lstContextWordsFiltered		= vars(args)['ContextWordsFiltered'] 
	nAssocThreshold				= vars(args)['AssocThresh']         
	nSimThreshold				= vars(args)['SimThresh']  			
	nDistThreshold				= vars(args)['DistThresh']     
	bOnlyCosinesOutput          = vars(args)['OnlyCosines'] 


	"""
	" Lê entrada
	"""
	profiles = buildProfilesOutput.map(lambda line: (line.split("\t")));

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
	condProbIndex	  		= 7  #lstHeader.index('cond_prob')
	pmiIndex 				= 8  #lstHeader.index('pmi')
	npmiIndex	      		= 9  #lstHeader.index('npmi')
	lmiIndex 				= 10 #lstHeader.index('lmi')
	tscoreIndex	    		= 11 #lstHeader.index('tscore')
	zscoreIndex	    		= 12 #lstHeader.index('zscore')
	diceIndex	      		= 13 #lstHeader.index('dice')
	chisquareIndex	 		= 14 #lstHeader.index('chisquare')
	loglikeIndex 			= 15 #lstHeader.index('loglike')
	affinityIndex	  		= 16 #lstHeader.index('affinity')
	entropy_targetIndex		= 17 #lstHeader.index('entropy_target')
	entropy_contextIndex 	= 18 #lstHeader.index('entropy_context')

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
						   .filter( FilterFromLists(lstTargetsWordsFiltered, lstContextWordsFiltered, lstNeighborWordsFiltered));

	if bGenSteps:
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
    

	if bGenSteps:
		targetContexts.write_text(strOutputFile+".SimilarityTargetContextes.txt", WriteMode.OVERWRITE );


	""" Cacula similaridade """
	# Faz a combinação cartesiana de todos os targets, junto com sua soma, soma quadrática e lista de contexts. 
	# E já cria os Similarities
	CalculatedSimilarities = targetContexts.cross(targetContexts) \
									       .using(Similaritier(bCalculateDistance));
										                  
	if bGenSteps:
		a = CalculatedSimilarities.map(lambda similarity : similarity.returnResultAsStr());
		a.write_text(strOutputFile+".CalculatedSimilarities.txt", WriteMode.OVERWRITE );
	
	""" Processa formato de saída """
	OutputData = CalculatedSimilarities.filter(OutputSim(lstTargetsWordsFiltered, lstNeighborWordsFiltered, nSimThreshold, nDistThreshold))\
								        .map(lambda similarity : similarity.returnResultAsStr( bOnlyCosinesOutput ));
	
	if bGenSteps:
		OutputData.write_text(strOutputFile+".CalculatedSimilarityOutput.txt", WriteMode.OVERWRITE );

	return OutputData;




