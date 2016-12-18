#!/usr/bin/env python
# -*- coding: utf-8 -*- 

""" 
Build Profiles Module
"""
from .utils import *
from .DataTypes import *

from flink.plan.Environment import get_environment
from flink.plan.DataSet import *
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.Aggregation import AggregationFunction, Min, Max, Sum
from flink.plan.Constants import Order

# """
# Name: buildProfiles
# 
# Constroi os perfis dos targets e contexts
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def buildProfiles(env, filterRawOutput, args):
	"""
	" Inicialização de variáveis a partir do argumento de entrada "
	"""
	bGenSteps     = vars(args)['GenerateSteps']
	bSaveOutput   = vars(args)['GenerateBuildProfile']
	strOutputFile = vars(args)['OutFile'];
	
	"""
	" Processa entrada para ficar no formato: (target, context, valor) 
	"""
	rawData = filterRawOutput.map(lambda tuple: (tuple[0][0], tuple[0][1], tuple[1])); #converte de ((target, context), valor) para: (target, context, valor)

	"""
	" nPairs => soma de todos os valores* (terceiro elemento da tupla: (target, context, valor))"
	"""
	nPairs = rawData.map(lambda tuple: tuple[2]).reduce(AddIntegers());

	"""
	" targetWithLinksAndCounts  => target com sua lista de links e sua contagem: (target,  (links, sum(target valor))) 
	" contextWithLinksAndCounts => context com sua lista de links e sua contagem: (context, (links, sum(context valor)))
	"""
	targetWithLinksAndCounts  = rawData.group_by(0).reduce_group(TargetsLinksAndCounts()); #Gera a lista de links e o somatório total para cada Target -->  #Gera: (go, ({'lot': 3, 'NNP': 3, 'season': 2}, 8))
	contextWithLinksAndCounts = rawData.group_by(1).reduce_group(ContextLinksAndCounts()); #Gera a lista de links e o somatório total para cada Context

	"""	
	" Calculate entropies for contexts and for targets ==> (palavra, totalCount, entropy)"
	"""
	TargetsEntropy  = targetWithLinksAndCounts.flat_map(EntropyCalculator());  #Gera: (target, count, entropy))
	ContextsEntropy = contextWithLinksAndCounts.flat_map(EntropyCalculator()); #Gera: (context, count, entropy))
	if bGenSteps:
		TargetsEntropy.write_text(strOutputFile+".TargetsEntropy.txt", WriteMode.OVERWRITE );
		ContextsEntropy.write_text(strOutputFile+".ContextsEntropy.txt", WriteMode.OVERWRITE );
	
	#Junta os dados
	JoinedTargetsEntropy = rawData.join(TargetsEntropy).where(0).equal_to(0).using(JoinTargetsCountAndEntropy());
	JoinedTargetsAndContextsEntropy = JoinedTargetsEntropy.join(ContextsEntropy).where(1).equal_to(0).using(JoinContextsCountAndEntropy()); #OBS: Esta é a parte com o maior volume de dados desta etapa
	if bGenSteps:
		JoinedTargetsAndContextsEntropy.write_text(strOutputFile+".JoinedTargetsAndContextsEntropy.txt", WriteMode.OVERWRITE );
	
	""" 
	" Calculate Profiles and prepare for output "
	"""
	OutputData = JoinedTargetsAndContextsEntropy.map(Profiler())\
												.with_broadcast_set("broadcastPairs", nPairs)\
												.map(lambda profile: profile.returnResultAsStr());
	"""
	" Output data "
	"""		
	if bGenSteps or bSaveOutput:
		#OutputHeader = env.from_elements(Profile.returnHeader());
		#OutputHeader.write_text(strOutputFile+".BuildProfilesOutput.txt", WriteMode.OVERWRITE );
		OutputData.write_text(strOutputFile+".BuildProfilesOutput.txt", WriteMode.OVERWRITE );
		return OutputData;
	else:
		return OutputData;



