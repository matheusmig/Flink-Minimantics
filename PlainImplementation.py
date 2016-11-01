#!/usr/bin/env python
# -*- coding: utf-8 -*- 

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
	parser.add_argument('-i', '--input',                                            dest='InFile') 		            #Nome do arquivo de entrada
	parser.add_argument('-o', '--output',                              default='',  dest='OutFile')		            #Nome do arquivo de saída
	parser.add_argument('--steps',               action='store_const', const=True,  dest='GenerateSteps')           #Flag que indica se deve gerar TODOS arquivos intermediários de saída durante as etapas do algoritmo
	parser.add_argument('--save_filterraw',      action='store_const', const=True,  dest='GenerateFilterRaw')       #Flag que indica se deve gerar o arquivo de saída para a fase 1 (FilterRaw)
	parser.add_argument('--save_buildprofile',   action='store_const', const=True,  dest='GenerateBuildProfile')    #Flag que indica se deve gerar o arquivo de saída para a fase 2 (BuildProfiles)
	parser.add_argument('--save_calcsimilarity', action='store_const', const=True,  dest='GenerateCalcSimilarity')  #Flag que indica se deve gerar o arquivo de saída para a fase 3 (CalculateSimilarity)
	parser.add_argument('-a',                                 default="cond_prob",  dest='AssocName')   	        #used in CalculateSimilarity
	parser.add_argument('-s',                                          default='',  dest='Scores')		            #used in CalculateSimilarity
	parser.add_argument('-t',                                          default=[],  dest='TargetsWordsFiltered')    #used in CalculateSimilarity. Lista de targets que serão ignorados e removidos durante processamento
	parser.add_argument('-n',                                          default=[],  dest='NeighborWordsFiltered')   #used in CalculateSimilarity. Lista de neighbors que serão ignorados e removidos durante processamento
	parser.add_argument('-c',                                          default=[],  dest='ContextWordsFiltered')    #used in CalculateSimilarity. Lista de contexts que serão ignorados e removidos durante processamento
	parser.add_argument('-A',                          type=float, default=-99999,  dest='AssocThresh')             #used in CalculateSimilarity. Threshold mínimo para a medida de associação entre um target e um context, pares de target,context que tiverem uma força de associação abaixo disso serão filtrado fora.
	parser.add_argument('-S',                          type=float, default=-99999,  dest='SimThresh')  		        #used in CalculateSimilarity. Threshold mínimo para as medidas de similaridade, targets com medidas abaixo disso serão filtrados fora.
	parser.add_argument('-D',                          type=float, default=-99999,  dest='DistThresh')              #used in CalculateSimilarity. Threshold máximo para as medidas de distância, targets com medidas a cima disso serão filtrados fora.
	parser.add_argument('--calculate_distances', action='store_const', const=True,  dest='CalculateDistances')      #used in CalculateSimilarity. Flag que indica se calcularemos todas as medidas de distância, senão mediremos apenas as medidas de similaridade 
	parser.add_argument('--only_cosines',        action='store_const', const=True,  dest='OnlyCosines')             #Flag que indica se queremos gerar o arquivo de saída contendo como unica medida de similaridade a similiridade por coseno 
	parser.add_argument('-FW',                         type=int,        default=0,  dest='FilterWordThresh')        #used in FilterRaw. Número mínimo de vezes que uma palavra tem que aparecer no arquivo de entrada para ter sua semelhança calculada.
	parser.add_argument('-FP',                         type=int,        default=0,  dest='FilterPairThresh')        #used in FilterRaw. Número mínimo de vezes que uma dupla de palavras deve repetir-se no arquivo de arquivo de entrada, para que seja levada em consideração .
	args, unknown = parser.parse_known_args()
	return args,unknown;


"""
Flink User-defined functions
"""

# """
# Name: Adder
# 
# Classe utilizada para especificar uma GroupReduceFunction
#
# Author: 23/07/2016 Matheus Mignoni
class Adder(GroupReduceFunction):
	def reduce(self, iterator, collector): #The reduce method. The function receives one call per group of elements.
		word, count = iterator.next()
		count += sum([x[1] for x in iterator])
		collector.collect((word, count));


# """
# Name: NothingReduce
# 
# Classe utilizada para especificar uma GroupReduceFunction
# Repassa todos os valores adiante
# 
#
# Author: 23/07/2016 Matheus Mignoni
# """
class NothingReduce(GroupReduceFunction):
	def reduce(self, iterator, collector):
		while iterator.has_next():
			value = iterator.next();
			collector.collect(value);

# """
# Name: AddIntegers
# 
# Classe utilizada para especificar uma ReduceFunction
# Retorna a soma dos dois elementos
# 
#
# Author: 23/07/2016 Matheus Mignoni
# """
class AddIntegers(ReduceFunction):
	def reduce(self, value1, value2):
		return int(value1) + int(value2);

# """
# Name: LinksAndCounts
# 
# Classe utilizada para especificar uma GroupReduceFunction
# Utilizada em BuildProfiles.py Gera a lista de links e o somatório count para cada palavra
# 
# Retorna uma tupla contendo (Palavra, links, countTotal)
# onde countTotal é count total da palavra
# e linkList é a lista de links que a palavra tem, juntamente com o valor de cada link => ((link1, count1), ..., (linkN, countN))
#
# Author: 23/07/2016 Matheus Mignoni
class TargetsLinksAndCounts(GroupReduceFunction):
	def reduce(self, iterator, collector):  
		dictLinks = {};
		count     = 0;
		for x in iterator:
			count = count + int(x[2]);
			if x[1] in dictLinks.keys():
				dictLinks[x[1]] = dictLinks[x[1]] + int(x[2]);
			else:
				dictLinks[x[1]] = int(x[2]);

		collector.collect((x[0], (dictLinks, count)));

class ContextLinksAndCounts(GroupReduceFunction):
	def reduce(self, iterator, collector):  
		dictLinks = {};
		count     = 0;
		
		for x in iterator:
			count = count + int(x[2]);
			if x[0] in dictLinks.keys():
				dictLinks[x[0]] = dictLinks[x[0]] + int(x[2]);
			else:
				dictLinks[x[0]] = int(x[2]);
		
		if x:
			collector.collect((x[1], (dictLinks, count)));

# """
# Name: EntropyCalculator
# 
# Classe utilizada para especificar uma FlatMapFunction
# Utilizada em BuildProfiles.py, calcula a entropia para uma palavra
# 
# Retorna a entropia de uma palavra
#
# Author: 23/07/2016 Matheus Mignoni
# Modif:  21/09/2016 Matheus Mignoni
#		  - Não retorna mais o dictionary de links
#
# """
class EntropyCalculator(FlatMapFunction):
	def flat_map(self, value, collector):
		finalEntropy = 0.0;
		palavra    = value[0];
		dictLinks  = value[1][0];
		totalCount = int(value[1][1]);

		for entry in dictLinks.values():
			p = (entry / totalCount)
			finalEntropy = finalEntropy - (p * (math.log(p)));

		collector.collect((palavra, totalCount, finalEntropy));

# """
# Name: RightJoinCoGroup
# 
# Classe utilizada para especificar uma JoinFunctions
# Utilizado em BuildProfiles.py, faz o append apenas do count total e da entropy de um palavra
# 
# Author: 23/07/2016 Matheus Mignoni
# """
class JoinTargetsCountAndEntropy(JoinFunction):
	def join(self, value1, value2):
		return (value1[0], value1[1], value1[2], (value2[1], value2[2]) );

class JoinContextsCountAndEntropy(JoinFunction):
	def join(self, value1, value2):
		return (value1[0], value1[1], value1[2], value1[3], (value2[1], value2[2]) );

# """
# Name: Profiler
# 
# Classe utilizada para especificar uma MapFunction
# Cria os objetos de Profile
# 
# Author: 23/07/2016 Matheus Mignoni
# """
class Profiler(MapFunction):
	def map(self, tuple):
		pairsSum = self.context.get_broadcast_variable("broadcastPairs");
		return Profile(tuple[0], tuple[1], int(tuple[2]), int(tuple[3][0]), int(tuple[4][0]), tuple[3][1], tuple[4][1], int(pairsSum[0]));



"""
Main
"""
if __name__ == "__main__":
    # get the args via parameters
    args, unknown = inputArgs()

    # get paths to input and output files
	strInputFile  = vars(args)['InFile'];
	strOutputFile = vars(args)['OutFile'];

	# get other args
	wordLengthThreshold 		= vars(args)['FilterWordThresh']
	pairCountThreshold  		= vars(args)['FilterPairThresh']
	bGenSteps     				= vars(args)['GenerateSteps']
	strOutputFile       		= vars(args)['OutFile'];
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

    # remove the output file, if there is one there already
    if os.path.isfile(strOutputFile):
        os.remove(strOutputFile)

    # set up the environment with a text file source
    env = get_environment()
    data = env.read_text(strInputFile); #Lê do arquivo

	"""
	Step1: Filter Raw
	"""	
    pairWords = data\
			        .map( lambda line: line.split(" ") )\
					.map( lambda tuple: (tuple[0].lower().strip(), tuple[1].lower().strip()));

	targetsFiltered = pairWords\
					.map( lambda tuple : (tuple[0], 1) )\
					.group_by(0).reduce_group(Adder())\
					.filter(lambda targetTuple: len(targetTuple[0]) > 1 and targetTuple[1] > int(wordLengthThreshold))\
					.project(0);

	contextsFiltered = pairWords\
					.map( lambda tuple : (tuple[1], 1) )\
					.group_by(0).reduce_group(Adder())\
					.filter(lambda contextTuple: len(contextTuple[0]) > 1 and contextTuple[1] > int(wordLengthThreshold))\
					.project(0);

	pairWordsFiltered = pairWords\
					.join(targetsFiltered).where(0).equal_to(0).project_first(0,1)\
					.join(contextsFiltered).where(1).equal_to(0).project_first(0,1);

	filterRawOutput = pairWordsFiltered\
					.map( lambda tuple : ( (tuple[0], tuple[1]) , 1) )\
					.group_by(0).reduce(Adder())\
					.filter(lambda tuple : tuple[1] >= pairCountThreshold)\
					.group_by(0)\
					.sort_group((lambda tuple : tuple[0][0]), Order.ASCENDING )\
					.sort_group((lambda tuple : tuple[0][1]), Order.ASCENDING )\
					.reduce_group(NothingReduce());
	"""
	Step2: Build Profiles
	"""	
	rawData = filterRawOutput\
					.map(lambda tuple: (tuple[0][0], tuple[0][1], tuple[1]))\
					
	nPairs = rawData\
					.map(lambda tuple: tuple[2]).reduce(AddIntegers());

	targetsEntropy  = rawData\
					.group_by(0).reduce_group(TargetsLinksAndCounts())\
					.flat_map(EntropyCalculator());
	
	contextsEntropy = rawData\
					.group_by(1).reduce_group(ContextLinksAndCounts())\
					.flat_map(EntropyCalculator()); 

	buildProfilesOutput = rawData\
					.join(targetsEntropy).where(0).equal_to(0).using(JoinTargetsCountAndEntropy())\
					.join(contextsEntropy).where(1).equal_to(0).using(JoinContextsCountAndEntropy())\
					.map(Profiler())\
					.with_broadcast_set("broadcastPairs", nPairs)\
					.map(lambda profile: profile.returnResultAsStr());
	
	"""
	Step3: Calculate Similarities
	"""	









    # execute the plan locally.
    env.execute(local=True)