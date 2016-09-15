""" 
Calculate Similarity Module
"""
from utils import *
from operator import add
from math import *
from random import *

"""
   OBS: Esta função apenas leva em consideração os itens com cabeçalho:
   target, id_target, context, id_context, f_tc, f_t, f_c, cond_prob, pmi, npmi, lmi, tscore, zscore, dice, chisquare, loglike,
   affinity, entropy_target, entropy_context 
"""

""" Global Vars"""
#Posição dos elementos nos dados
targetIndex				= 0
contextIndex			= 0
targetContextCountIndex = 0
targetCountIndex    	= 0
contextCountIndex		= 0
entropy_targetIndex		= 0
entropy_contextIndex	= 0
condProbIndex	  		= 0
pmiIndex	       		= 0
npmiIndex	      		= 0
lmiIndex	       		= 0
tscoreIndex	    		= 0
zscoreIndex	    		= 0
diceIndex	      		= 0
chisquareIndex	 		= 0
loglikeIndex	   		= 0
affinityIndex	  		= 0

#Argumentos
bSaveOutput = False;

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
def calculateSimilarity(sc, buildProfilesOutput, args):
	"""
	" Inicialização de variáveis conforme argumentos de entrada "
	"""
	global bSaveOutput
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
		input = sc.textFile(c_HdfsPath + "mini.1.profiles" ).map(lambda line: (line.split("\t")));
	else: 		    # Entrada é recebida em memória 
		input = buildProfilesOutput;

	"""
	" Processa entrada, extraindo header e filtrando dados
	"""
	(header, rawData, data) = extractInput(input, nAssocThreshold, lstTargetsWordsFiltered, lstNeighborWordsFiltered, lstContextWordsFiltered);


	""" Cacula similaridade """
	# Faz a combinação cartesiana de todos os targets, junto com sua soma, soma quadrática e lista de contexts
	targetsCartesian = data.cartesian(data);
	
	#As funções dentro de calc_sim são python puro, pois elas não conseguem ser processadas pela DAG do SPark.
	#TODO: Como aumentar a eficiência de processamento paralelo nessa parte?
	# OBS:
	# target1      = i[0][0]; 
	# sum1 		   = i[0][1][0];
	# sum_square1  = i[0][1][1];
	# contextDict1 = i[0][1][2];
	# target2      = i[1][0];
	# sum2 		   = i[1][1][0];
	# sum_square2  = i[1][1][1];
	# contextDict2 = i[1][1][2];
	listCalculatedSimilarities = targetsCartesian.map( lambda tuple: calc_sim(tuple[0][0], tuple[1][0], tuple[0][1][2], tuple[1][1][2], tuple[0][1][0], tuple[0][1][1], tuple[1][1][0], tuple[1][1][1]));
			

	""" Processa formato de saída """					      
	listOutputFinal = []; #Lista com o resultado final
	listOutputFinal = listCalculatedSimilarities.map( lambda sim: sim.returnResultAsStr()).collect();


# Antiga forma de percorrer a lista de targets cartesian, com python puro:
#	for i in targetsCartesian:
#		target1      = i[0][0][1]; 
#		sum1 		 = i[0][1][0];
#		sum_square1  = i[0][1][1];
#		contextDict1 = i[0][1][2];
#		target2      = i[1][0][1];
#		sum2 		 = i[1][1][0];
#		sum_square2  = i[1][1][1];
#		contextDict2 = i[1][1][2];
#		if (target1 not in lstTargetsWordsFiltered) and (target2 not in lstTargetsWordsFiltered):
#			result = calc_sim(contextDict1, contextDict2, sum1, sum_square1, sum2, sum_square2);
#			result.target1 = target1;
#			result.target2 = target2;
#			#OBS: Aqui não estamos processando a etapa de process_sim_scores, no qual calcula apenas os scores que foram selecionados na opçoes de argumento 's'
#			listOutputFinal.append(result.returnResultAsStr())



	"""
	" Output data "
	"""	
	outputHeaderRDD = sc.parallelize( [SimilarityResult.returnHeader()] ) ;
	outputDataRdd   = sc.parallelize(listOutputFinal)

	outputRdd = outputHeaderRDD.union(outputDataRdd)	

	if bSaveOutput:
		saveToSSV(outputRdd, "mini.1.sim-th0.2")

	return outputRdd




# """
# Name: extractInput
# 
# Dado o arquivo de entrada, extrai cabeçalho e filtra os dados
# Retorna os dados de entrada como uma lista de objetos 
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def extractInput(buildProfilesOutput, nAssocThresh, lstTargetsWordsFiltered, lstContextWordsFiltered, lstNeighborWordsFiltered):
	""" 
	" Cabeçalho 
	"""
	header = buildProfilesOutput.first()  

	""" 
	" Dados 
	"""
	data = buildProfilesOutput.filter(lambda x: x != header); #removeheader

	#Conseguiremos acessar a posição correta dos itens, através da sua localização no header.
	#p.ex, se quisermos pegar o target, basta acessamos o indice contido em header.index('target'));
	global targetIndex
	global contextIndex				
	global targetContextCountIndex	
	global targetCountIndex    		
	global contextCountIndex		
	global entropy_targetIndex		
	global entropy_contextIndex		
	global condProbIndex	  		
	global pmiIndex	       			
	global npmiIndex	      		
	global lmiIndex	       			
	global tscoreIndex	    		
	global zscoreIndex	    		
	global diceIndex	      		
	global chisquareIndex	 		
	global loglikeIndex	   			
	global affinityIndex	
	global bSaveOutput  		

	if bSaveOutput:
		#Veio do arquivo, o cabecalho já é uma lista
		lstHeader = header; 
	else:
		#Veio da memória, o cabeçalho está com os elementos separados por virgula
		lstHeader = header.split(" "); #Transforma header em uma lista

	targetIndex 			= lstHeader.index('target');
	contextIndex 			= lstHeader.index('context')
	targetContextCountIndex	= lstHeader.index('f_tc')
	targetCountIndex 		= lstHeader.index('f_t')
	contextCountIndex		= lstHeader.index('f_c')
	entropy_targetIndex		= lstHeader.index('entropy_target')
	entropy_contextIndex 	= lstHeader.index('entropy_context')
	condProbIndex	  		= lstHeader.index('cond_prob')
	pmiIndex 				= lstHeader.index('pmi')
	npmiIndex	      		= lstHeader.index('npmi')
	lmiIndex 				= lstHeader.index('lmi')
	tscoreIndex	    		= lstHeader.index('tscore')
	zscoreIndex	    		= lstHeader.index('zscore')
	diceIndex	      		= lstHeader.index('dice')
	chisquareIndex	 		= lstHeader.index('chisquare')
	loglikeIndex 			= lstHeader.index('loglike')
	affinityIndex	  		= lstHeader.index('affinity')

	""" 
	Filtra dados: 
	   - score menores que o limite de AssocThresh
	   - targets na lista de targets a ignorar
	   - context na lista de context a ignorar
	   - targets na lista de neighboors a ignorar 
	"""
	filteredData = data.filter(lambda tuple: float(tuple[targetContextCountIndex]) >= nAssocThresh\
									     and tuple[targetIndex]  not in lstTargetsWordsFiltered\
										 and tuple[contextIndex] not in lstContextWordsFiltered\
										 and tuple[targetIndex]  not in lstNeighborWordsFiltered);

	""" O código original em C, utilizava uma estrutura chamada targets_context para armazenar os targets, suas somas e sua lista de contexts. 
	# Replicaremos o mesmo comportamente através de um simples tupla, para facilitar processamento (apesar de dificultar manuteção(?))
	# Tupla é (key,value) onde: 
	# 	key = target 
	# 	value = (sum, sum_square, contexts) 
	#
	# contexts é uma lista de (context, valor)
	#
	# Primeiramente mapearemos para as tuplas e depois faremos o agrupamento dos targets iguais """
	targetContexts = filteredData.map(lambda tuple: (tuple[targetIndex], (float(tuple[targetContextCountIndex]), float(tuple[targetContextCountIndex])*float(tuple[targetContextCountIndex]), { tuple[contextIndex]: float(tuple[targetContextCountIndex]) } ) ))\
							     .combineByKey(lambda value:    (value[0], value[1], value[2]),
                           					   lambda x, value: (x[0] + value[0], x[1] + value[1], dict(x[2].items() | value[2].items())),
                           				  	   lambda x, y:     (x[0] + y[0], x[1] + y[1], x[2].update(y[2]) ))\
							     .sortByKey();
	#Uma linha do TargetContexts é ex: ( accept,    (28.0,  214.0,         { 'demand': 3.0, 'plan': 12.0, 'offer': 2.0, 'decision': 2.0, 'refugee': 2.0, 'proposal': 7.0}) )
    #                                      ^target  ^sum     ^sum_square      ^dicionario com context : value

	return (header, filteredData, targetContexts);

# """
# Name: calc_sim
# 
# Calcula a similaridade de duas listas de contextos,
# retorna no formato de um objeto SimilarityResult 
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def calc_sim(target, neighbor, contextList1, contextList2, sum1, sum_square1, sum2, sum_square2):
	""" Inicializações """
	result = SimilarityResult();
	sumsum = 0.0
	for key1, value1 in contextList1.items():
		v1 = contextList1[key1];
		if key1 in contextList2:  # The context is shared by both target
			v2 = contextList2[key1];
			sumsum += v1 + v2;
			result.cosine += v1 * v2
			if (bCalculateDistance):
				absdiff = fabs(v1 - v2);
				result.l1 	  += absdiff;
				result.l2 	  += absdiff * absdiff; 
				result.askew1 += relativeEntropySmooth( v1, v2 );
				result.askew2 += relativeEntropySmooth( v2, v1 );
				avg = (v1+v2)/2.0;
				result.jsd    += relativeEntropySmooth( v1, avg ) + relativeEntropySmooth( v2, avg );		
		else:
			if (bCalculateDistance):
				result.askew1 += relativeEntropySmooth( v1, 0 );
				result.jsd    += relativeEntropySmooth( v1, v1/2.0);
				result.l1     += v1;
				result.l2     += v1 * v1;


	""" Distance measures use the union of contexts and require this part """
	if bCalculateDistance :
		for key2, value2 in contextList2.items():
			v2 = contextList2[key2];
			if not (key2 in contextList1):  # The context is not shared by both target
				result.askew2 += relativeEntropySmooth( v2, 0 );
				result.jsd    += relativeEntropySmooth( v2, v2/2.0 );
				result.l1     += v2;      
				result.l2     += v2 * v2;   

		result.l2 = sqrt( result.l2 );   


	result.cosine = result.cosine / (sqrt(sum_square1) * sqrt(sum_square2));
	result.lin = sumsum / (sum1 + sum2);
	# Different version of jaccard: you are supposed to use it with 
	# assoc_measures f_c or entropy_context. In this case, the sumsum value is 
	# 2 * context_weights, and dividing by 2 is the same as averaging between 2 
	# equal values. However, when used with different assoc_scores, this can give
	# interesting results. To be tested. Should give similar results to Lin */
	result.wjaccard = (sumsum/2.0) / ( sum1 + sum2 - (sumsum/2.0) );
	result.randomic = random();

	result.target1 = target;
	result.target2 = neighbor;
	return result;







