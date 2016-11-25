
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


# """
# Name: FilterFromList
# 
# Filtra valores que não estão na lista passada por parâmetro
# 
# Author: 23/07/2016 Matheus Mignoni
# """
class FilterFromList(FilterFunction):
	def __init__(self, index, list):
		self.index = index;
		self.list = list;

	def filter(self, value):
		if value[self.index] not in self.list:
			return True
		else:
			return False

# """
# Name: TargetContextsGrouper
# 
# Classe utilizada para especificar uma GroupReduceFunction
# Mapeia as informações de um target e seus contexts
# 
# Author: 23/07/2016 Matheus Mignoni
class TargetContextsGrouper(GroupReduceFunction):
	def reduce(self, iterator, collector):
		target       = '';
		sum_ 	     = 0;
		sum_square   = 0;
		dictContexts = dict();

		for key,value in iterator:
			target     = key;
			sum_       += value[0]
			sum_square += value[1]
			if value[2][0] in dictContexts.keys():
				dictContexts[value[2][0]] = dictContexts[value[2][0]] + value[2][1];
			else:
				dictContexts[value[2][0]] = value[2][1];

		a = DictOfContexts(dictContexts)
		collector.collect( (target, (sum_, sum_square), a.returnResultAsStr())  );

# """
# Name: Similaritier
# 
# Classe utilizada para especificar uma FlatMapFunction
# Recebe dois targets com suas devidas listas de contexto, calcula a similaridade
# 
# Retorna a entropia de uma palavra
#
# Author: 23/07/2016 Matheus Mignoni
#
# """
class Similaritier(FlatMapFunction):
	def __init__(self, bCalculateDistance=None):
		if bCalculateDistance is None:
			self.bCalculateDistance = False; #Default value
		else:
			self.bCalculateDistance = bCalculateDistance;

	def flat_map(self, value, collector):
		target1     	= value[0][0]; 
		sum1 	     	= value[0][1][0];
		sum_square1   	= value[0][1][1];
		dictOfContexts1 = json.loads(value[0][2]);
		target2      	= value[1][0];
		sum2 	     	= value[1][1][0];
		sum_square2  	= value[1][1][1];
		dictOfContexts2	= json.loads(value[1][2]);
		""" Inicializações """
		result       = Similarity();
		sumsum       = 0.0
		#contextDict1 = dictOfContexts1.dict;
		contextDict1 = dictOfContexts1;
		#contextDict2 = dictOfContexts2.dict;
		contextDict2 = dictOfContexts2;

		#Percorre lista de contexto da 1a
		for k1, v1 in contextDict1.items():
			if k1 in contextDict2:  # The context is shared by both target
				v2 = contextDict2[k1];
				sumsum += v1 + v2;
				result.cosine += v1 * v2
				if (self.bCalculateDistance):
					absdiff = fabs(v1 - v2);
					result.l1 	  += absdiff;
					result.l2 	  += (absdiff * absdiff); 
					result.askew1 += relativeEntropySmooth( v1, v2 );
					result.askew2 += relativeEntropySmooth( v2, v1 );
					avg            = (v1+v2)/2.0;
					result.jsd    += relativeEntropySmooth( v1, avg ) + relativeEntropySmooth( v2, avg );		
			else:
				if (self.bCalculateDistance):
					result.askew1 += relativeEntropySmooth( v1, 0 );
					result.jsd    += relativeEntropySmooth( v1, v1/2.0);
					result.l1     += v1;
					result.l2     += v1 * v1;

		#Distance measures use the union of contexts and require this part
		if self.bCalculateDistance :
			for k2, v2 in contextDict2.items():
				if not (k2 in contextDict1):  # The context is not shared by both target
					result.askew2 += relativeEntropySmooth( v2, 0 );
					result.jsd    += relativeEntropySmooth( v2, v2/2.0 );
					result.l1     += v2;      
					result.l2     += v2 * v2;   

			result.l2 = sqrt( result.l2 );

		dividendo = sqrt(sum_square1) * sqrt(sum_square2);
		if dividendo != 0:
			result.cosine = result.cosine / dividendo

		dividendo = sum1 + sum2
		if dividendo != 0:
			result.lin = sumsum / dividendo;

		# Different version of jaccard: you are supposed to use it with 
		# assoc_measures f_c or entropy_context. In this case, the sumsum value is 
		# 2 * context_weights, and dividing by 2 is the same as averaging between 2 
		# equal values. However, when used with different assoc_scores, this can give
		# interesting results. To be tested. Should give similar results to Lin */
		dividendo = sum1 + sum2 - (sumsum/2.0);
		if dividendo != 0:
			result.wjaccard = (sumsum/2.0) / dividendo;

		result.randomic = random.random();

		result.target1 = target1;
		result.target2 = target2;

		#collector.collect((palavra, (dictLinks, int(totalCount), float(finalEntropy))));
		collector.collect(result);

# """
# Name: OutputSim
# 
# Filtra Similarities Results. Implementa o comportamento da função "output_sim" do código original em C
# 
# Author: 16/10/2016 Matheus Mignoni
# """
class OutputSim(FilterFunction):
	def __init__(self, lst_tfilter, lst_nfilter, simThresh, distThresh):
		self.lst_tfilter = lst_tfilter;
		self.lst_nfilter = lst_nfilter;

		self.simThresh  = simThresh;
		self.distThresh = distThresh;

	def filter(self, value):
		#Value is a Similariy object
		if value.target1 == value.target2:
			return False;
		elif value.target1 in self.lst_tfilter:
			return False
		elif value.target2 in self.lst_nfilter:
			return False
		else:
			#Filter simThreshold and distThreshold
			if ((self.simThresh  != -99999) and ((value.cosine < self.simThresh) or (value.wjaccard < self.simThresh) or (value.lin < self.simThresh))) or\
			   ((self.distThresh != -99999) and ((value.lin > self.distThresh) or (value.l1 > self.distThresh) or (value.l2 > self.distThresh) or (value.jsd > self.distThresh))):
				return False; #OBS: Não estamos levando em consideração a medida askew1 e askew2
			else:
				return True;


""" 
Classes
"""
class Profile(object):
	#Construtor com 8 parâmetros básicos, infere os outros 10 parâmetros a partir dos básicos.
	def __init__(self, target, context, targetContextCount, targetCount, contextCount, entropy_target, entropy_context, nPairs):
		decimal.getcontext().prec = 28

		self.target             = target;
		self.context            = context
		self.targetContextCount = decimal.Decimal(targetContextCount)       	
		self.targetCount        = decimal.Decimal(targetCount)
		self.contextCount       = decimal.Decimal(contextCount)
		self.entropy_target     = decimal.Decimal(entropy_target)
		self.entropy_context    = decimal.Decimal(entropy_context)
		self.nPairs             = decimal.Decimal(nPairs)

		self.cw1nw2  = decimal.Decimal(targetCount  - targetContextCount);
		self.cnw1w2  = decimal.Decimal(contextCount - targetContextCount);
		self.cnw1nw2 = decimal.Decimal(nPairs - targetCount - contextCount + targetContextCount);

		self.ew1w2   = decimal.Decimal(self.expected(         targetCount,          contextCount, nPairs))
		self.ew1nw2  = decimal.Decimal(self.expected(         targetCount, nPairs - contextCount, nPairs))
		self.enw1w2  = decimal.Decimal(self.expected(nPairs - targetCount,          contextCount, nPairs))
		self.enw1nw2 = decimal.Decimal(self.expected(nPairs - targetCount, nPairs - contextCount, nPairs))

		self.condProb  = self.condProb();
		self.pmi       = self.pmi();
		self.npmi      = self.npmi();
		self.lmi       = self.lmi();
		self.tscore    = self.tscore();
		self.zscore    = self.zscore();
		self.dice      = self.dice();
		self.chisquare = self.chisquare();
		self.loglike   = self.loglike();
		self.affinity  = self.affinity();


	def expected(self, cw1, cw2, n):
		return decimal.Decimal( (cw1 * cw2)/n );

	def PRODLOG(self, a, b): #Evita calcular log(0)
		if a != 0 and b != 0:
			return a * b.ln()
		else:
			return 0

	def condProb(self):
		return self.targetContextCount / self.targetCount;

	def pmi(self):
		return self.targetContextCount.ln() - self.ew1w2.ln();

	def npmi(self):
		return self.pmi / ( self.nPairs.ln() - self.targetContextCount.ln() )

	def lmi(self):
		return self.targetContextCount * self.pmi

	def tscore(self):
		return (self.targetContextCount - self.ew1w2 ) / self.targetContextCount.sqrt()

	def zscore(self):
		return (self.targetContextCount - self.ew1w2 ) / self.ew1w2.sqrt() 

	def dice(self):
		return (2 * self.targetContextCount) / (self.targetCount + self.contextCount)

	def chisquare(self):
		r1 = 0; r2 = 0; r3 = 0; r4 = 0;
		
		if self.ew1w2 != 0:
			r1 = ((self.targetContextCount - self.ew1w2) ** 2) / self.ew1w2 

		if self.ew1nw2 != 0:
			r2 = ((self.cw1nw2 - self.ew1nw2) ** 2) / self.ew1nw2 

		if self.enw1w2 != 0:
			r3 = ((self.cnw1w2 - self.enw1w2) ** 2) / self.enw1w2 

		if self.enw1nw2 != 0:
			r4 = ((self.cnw1nw2 - self.enw1nw2) ** 2) / self.enw1nw2 

		return r1+r2+r3+r4;

	def loglike(self):
		r1 = 0; r2 = 0; r3 = 0; r4 = 0;

		if self.ew1w2 != 0:
			r1 = self.PRODLOG( self.targetContextCount , self.targetContextCount / self.ew1w2 );

		if self.ew1nw2 != 0:
			r2 = self.PRODLOG( self.cw1nw2             , self.cw1nw2  / self.ew1nw2  );

		if self.enw1w2 != 0:
			r3 = self.PRODLOG( self.cnw1w2             , self.cnw1w2  / self.enw1w2  );

		if self.enw1nw2 != 0:
			r4 = self.PRODLOG( self.cnw1nw2            , self.cnw1nw2 / self.enw1nw2 );

		return 2 * (r1 + r2 + r3 + r4);
	
	def affinity(self):
		return decimal.Decimal(0.5) * (self.targetContextCount / self.targetCount + self.targetContextCount / self.contextCount);

	#Funçoes de I/O	
	def returnResultAsList(self):
		return (self.target, self.context, self.targetContextCount, self.targetCount, self.contextCount, self.condProb, self.pmi, self.npmi,\
				self.lmi, self.tscore, self.zscore, self.dice, self.chisquare, self.loglike, self.affinity, self.entropy_target, self.entropy_context)

	def returnResultAsStr(self):
		return str(self.target)+"\t"+str(self.context)+"\t"+str(self.targetContextCount)+"\t"+str(self.targetCount)+"\t"+str(self.contextCount)+"\t"+\
		       str(self.condProb)+"\t"+str(self.pmi)+"\t"+str(self.npmi)+"\t"+str(self.lmi)+"\t"+str(self.tscore)+"\t"+str(self.zscore)+"\t"+str(self.dice)+"\t"+str(self.chisquare)+"\t"+str(self.loglike)+"\t"+\
		       str(self.affinity)+"\t"+str(self.entropy_target)+"\t"+str(self.entropy_context);
	
	@staticmethod
	def returnHeader():
		return "target\tcontext\tf_tc\tf_t\tf_c\tentropy_target\tentropy_context\tcond_prob\tpmi\tnpmi\tlmi\ttscore\tzscore\tdice\tchisquare\tloglike\taffinity"


class Similarity(object):
	def __init__(self, target1="", target2="", cosine=0.0, wjaccard=0.0, lin=0.0, l1=0.0, l2=0.0, jsd=0.0, random=0.0, askew1=0.0, askew2=0.0):
		self.target1   = target1; 	#Target
		self.target2   = target2; 	#Neighbor
		self.cosine    = cosine; 	#Tipo de score: SIM
		self.wjaccard  = wjaccard; 	#Tipo de score: SIM
		self.lin       = lin; 		#Tipo de score: SIM
		self.l1        = l1 		#Tipo de score: DIST
		self.l2        = l2; 		#Tipo de score: DIST
		self.jsd       = jsd;		#Tipo de score: DIST
		self.randomic  = random; 	#Tipo de score: RAND
		self.askew1    = askew1; 	#Tipo de score: DIST
		self.askew2    = askew2; 	#Tipo de score: DIST

	#Funçoes de I/O	
	def returnResultAsStr(self, bOnlyCosine=None):
		#Retorna todas as medidas de similaridade
		if (bOnlyCosine is None) or (bOnlyCosine is False):
			return self.target1+"\t"+self.target2+"\t"+str(self.cosine)+"\t"+str(self.wjaccard)+"\t"+str(self.lin)+"\t"+\
		       str(self.l1)+"\t"+str(self.l2)+"\t"+str(self.jsd)+"\t"+str(self.randomic)+"\t"+str(self.askew1);
		#Retorna apenas a medida de cosseno
		else:
			return self.target1+"\t"+self.target2+"\t"+str(self.cosine);
		  

	#Funçoes de I/O	
	def returnCosineResultAsStr(self):
		return self.target1+"\t"+self.target2+"\t"+str(self.cosine);     
   

	@staticmethod
	def returnHeader():
		return "target\tneighbor\tcosine\twjaccard\tlin\tl1\tl2\tjsd\trandom\taskew";


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

	filterRawOutput.write_text(strOutputFile, WriteMode.OVERWRITE );


	"""
	Step2: Build Profiles
	"""	
#	rawData = filterRawOutput\
#					.map(lambda tuple: (tuple[0][0], tuple[0][1], tuple[1]))\
#					
#	nPairs = rawData\
#					.map(lambda tuple: tuple[2]).reduce(AddIntegers());
#
#	targetsEntropy  = rawData\
#					.group_by(0).reduce_group(TargetsLinksAndCounts())\
#					.flat_map(EntropyCalculator());
#	
#	contextsEntropy = rawData\
#					.group_by(1).reduce_group(ContextLinksAndCounts())\
#					.flat_map(EntropyCalculator()); 
#
#	buildProfilesOutput = rawData\
#					.join(targetsEntropy).where(0).equal_to(0).using(JoinTargetsCountAndEntropy())\
#					.join(contextsEntropy).where(1).equal_to(0).using(JoinContextsCountAndEntropy())\
#					.map(Profiler())\
#					.with_broadcast_set("broadcastPairs", nPairs)\
#					.map(lambda profile: profile.returnResultAsStr());
#	
	"""
	Step3: Calculate Similarities
	"""	
#	filteredProfiles = buildProfilesOutput\
#					.filter( lambda tuple: float(tuple[4]) >= nAssocThreshold)\
#	                .filter( FilterFromList(0 , lstTargetsWordsFiltered))\
#	                .filter( FilterFromList(2,  lstContextWordsFiltered))\
#	                .filter( FilterFromList(0 , lstNeighborWordsFiltered));
#
#	targetContexts = filteredProfiles\
#					.map(lambda tuple: (tuple[targetIndex], (float(tuple[targetContextCountIndex]), float(tuple[targetContextCountIndex])*float(tuple[targetContextCountIndex]), ( tuple[contextIndex], float(tuple[targetContextCountIndex]) ) ) ))\
#					.group_by(0).reduce_group(TargetContextsGrouper());
#
#	targetsCartesian = targetContexts\
#					.cross(targetContexts);
#
#	CalculatedSimilarities = targetsCartesian\
#					.flat_map(Similaritier( True ))\
#					.filter(OutputSim(lstTargetsWordsFiltered, lstNeighborWordsFiltered, nSimThreshold, nDistThreshold))\
#					.map(lambda similarity : similarity.returnResultAsStr( False ));
#
#
#	CalculatedSimilarities.write_text(strOutputFile, WriteMode.OVERWRITE );


    # execute the plan locally.
	env.execute(local=True)


