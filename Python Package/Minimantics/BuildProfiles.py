""" 
Build Profiles Module
"""
from .utils import *
from .DataTypes import *


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
	bSaveOutput = vars(args)['GenerateSteps']
	

	"""
	" Processa entrada para ficar no formato: (target, context, valor) 
	"""
	if bSaveOutput: # Entrada da função será lida de arquivo => ((target, context), valor) 
		rawData = env.read_text("/Volumes/MATHEUS/TCC/mini.1.s.filter.t10.c10.tc2.u" ).map(lambda line: (line.split(" ")));

	else: # Entrada é recebida por parametro  
		rawData = filterRawOutput.map(lambda tuple: (tuple[0][0], tuple[0][1], tuple[1])); #converte de ((target, context), valor) para: (target, context, valor)

	"""
	" nPairs => soma de todos os valores* (terceiro elemento da tupla: (target, context, valor))"
	"""
	nPairs = rawData.sum(2);

	"""
	" targetCounts        => gera o target e sua contagem: (target,  sum(target valor)) 
	" contextsCounts      => pega o context e sua contagem: (context, sum(context valor))
	" targetContextCounts => dada uma tupla [target, context] retorna o valor associado a ela
	"""
#	targetCounts = rawData.map(lambda tuple: (tuple[0], int(tuple[2])))\
#						  .group_by(0).reduce_group(Adder());
#
#	contextsCounts = rawData.map(lambda tuple: (tuple[1], int(tuple[2])))\
#					 	    .group_by(0).reduce_group(Adder());
#
#	targetContextCounts = rawData.map(lambda tuple: ((tuple[0],tuple[1]), int(tuple[2])))
#
#	"""
#	" targetLinks =>   lista dos contexts que estão associados a um determinado target. ex (target,  (list))
#	" contextsLinks => lista dos targets que estão associados a um determinado context. ex (context, (list))
#	"""
#	targetLinks =  rawData.map(lambda tuple: (tuple[0], tuple[1]))\
#								   .group_by(0).reduce_group(Listter());
#
#	contextsLinks = rawData.map(lambda tuple: (tuple[1], tuple[0]))\
#								   .group_by(0).reduce_group(Listter());

	targetWithLinksAndCounts  = rawData.group_by(0).reduce_group(LinksAndCounts());
	contextWithLinksAndCounts = rawData.group_by(1).reduce_group(LinksAndCounts());

	"""	
	" Calculate entropies for contexts and for targets "
	"""
#	x = contextsCounts.collect();
#	y = targetCounts.collect();
#	z = targetContextCounts.collect();
	ContextsEntropy = contextWithLinksAndCounts.flat_map();

	ContextsEntropy = contextsLinks.map(lambda tuple: (tuple[0], calculateEntropy(True,  tuple[0], list(tuple[1]), x, y ,z )));
	
	TargetsEntropy  =   targetLinks.map(lambda tuple: (tuple[0], calculateEntropy(False, tuple[0], list(tuple[1]), x, y ,z )));
	
	ContextsEntropyAndCounts = ContextsEntropy.leftOuterJoin(contextsCounts)
	TargetsEntropyAndCounts  = TargetsEntropy.leftOuterJoin(targetCounts)

	x = TargetsEntropyAndCounts.collect()
	y = ContextsEntropyAndCounts.collect()
	outputRaw = targetContextCounts.map(lambda tuple: ( (tuple[0][0], tuple[0][1]), \
											  findInformationInList(tuple[0][0],x ),\
											  findInformationInList(tuple[0][1],y ),\
											  tuple[1] ))
	
	""" 
	" Calculate Profiles and prepapre for output "
	"""
	outputFinal = outputRaw.collect();
	listOutputFinal = []
	for tuple in outputFinal:
		obj = Profile(tuple[0][0], tuple[0][1], float(tuple[3]), float(tuple[1][1]), float(tuple[2][1]), float(tuple[1][0]), float(tuple[2][0]), int(nPairs))
		listOutputFinal.append(obj.returnResultAsList())


	"""
	" Output data "
	"""		
	outputHeaderRDD = sc.parallelize( [Profile.returnHeader()] ) ;
	outputDataRdd   = sc.parallelize(listOutputFinal)

	outputRdd = outputHeaderRDD.union(outputDataRdd)


	if bSaveOutput:
		utils.saveToSSV(outputRdd, "SPARKmini.1.profiles")

	return outputRdd



# """
# Name: findCount
# 
# Percorre uma lista de tuplas (targets ou contexts) e devolve seu count
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def findCount(element, countList):
	#print ('DEBUG: findCount: elemento recebido eh %s' %(element))
	#print ('DEBUG: findCount: lista recebida eh ');
	#utils.listDump(countList);
	dictCountList = dict(countList) #Converte a lista de tuplas em um dicionario, facilita a busca de elementos
	count = dictCountList[element]  # Facilmente conseguimos buscar o valor associado ao elemento
	#print ('DEBUG: findCount: valor retornado: %s ' %(count));
	return int(count) #A Função lookup retorna uma lista, então convertemos 'count' para int


# """
# Name: calculateEntropy
# 
# Recebe uma palavra e a lista de links associadas a ela, percorre as listas de contexts, targets e targets_contexts
# para calcular a entropia. 
# A primeira variável é um booleano que indica se a Palavra é contexto ou target
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def calculateEntropy(isContext, strPalavra, strListLinks, listContextsCounts, listTargetsCounts, listTargetContextCounts):
	finalEntropy = 0.0;
	listEntropy = [];
	if isContext:
		totalCount = findCount(strPalavra, listContextsCounts)
	else: 
		totalCount = findCount(strPalavra, listTargetsCounts)
	#print ('DEBUG: calculateEntropy: totalCount eh: %s' %(totalCount));
	#print ('DEBUG: calculateEntropy: Lista de links recebida eh:');
	#utils.listDump(strListLinks)

	"""
	" Percorre cada elemento da lista de links e cria uma lista com os counts de target_context correspondent
	"""
	for link in strListLinks:
		if isContext:
			key = (link, strPalavra)
		else:
			key = (strPalavra, link)
		listEntropy.append( findCount( key, listTargetContextCounts) );
	
	#print ("DEBUG: calculateEntropy: lista de entropias encontrado eh ");
	#utils.listDump(listEntropy)

	for entropy in listEntropy:
		p = (entropy / totalCount)
		finalEntropy = finalEntropy - (p * (math.log(p)));

	#print ('DEBUG: calculateEntropy: final entropy: %s ' %(finalEntropy));
	return finalEntropy

# """
# Name: calculateEntropy
# 
# procura em ContextsEntropyAndCounts ou TargetEntropyandCounts as informacoes do elemento
# 
# Author: 23/07/2016 Matheus Mignoni
# """
def findInformationInList(element, listElements):
	dictlistElements = dict(listElements) #Converte a lista de tuplas em um dicionario, facilita a busca de elementos
	information = dictlistElements[element]  # Facilmente conseguimos buscar o valor associado ao elemento
	#print ('DEBUG: findCount: valor retornado: %s ' %(count));
	return information

