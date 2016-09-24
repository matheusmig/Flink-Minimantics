from .DataTypes import *
from math import *

from flink.functions.MapFunction         import MapFunction
from flink.functions.FlatMapFunction     import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.CoGroupFunction     import CoGroupFunction
from flink.functions.JoinFunction        import JoinFunction
"""
Utils Functions Module
"""
#print a list on terminal
def listDump (list):
	i = 0;
	#listColl = list.collect();
	for element in list:
		print (element);
		i = i + 1;
		if len(list) < i:
			break;
			
#print a rdd on a terminal
def dumpDataset(ds):
	ds.output();
	#ds.map(PrintValues);

#verify if element is contained in a dataset
def containsElement(ds, element, position):
	z = ds.flat_map(ExistElement(element, position));
	
	if sizeOfDataset(z) > 1:
		return True
	else:
		return False;

#Count dataset Size
def sizeOfDataset(ds):
	result = ds.map(lambda x: 1)\
	            .sum(0)\
	            .get_aggregate;
	return result;

##print a dictionary on terminal
#def dictDump(dict):
#	for keys,values in dict:
#		print(keys);
#		print(values);
#
##space separated values			
#def toSSVline (data):
#	return ' '.join(str(d) for d in data);
#
##save a rdd content to a SSV file
#def saveToSSV(rdd, fileName):
#	fileSSV = rdd.map(toSSVline)
#	fileSSV.saveAsTextFile(c_HdfsPath+fileName+'.ssv');
#	print("File: "+fileName+".ssv saved succesfuly!" )
#
##calculate and print diff time
#def showDiffTime(startTime, endTime):
#	totalTime = endTime-startTime;
#	print ("Total execution time was: %dms" %(int(totalTime.total_seconds() * 1000)))
#
##Relative Entropy Smooth
#def relativeEntropySmooth(p1, p2):
#	ALPHA  = 0.99  #Smoothing factors for a-skewness measure
#	NALPHA = 0.01
#	if (p1 == 0.0):
#		return 0.0; #If p1=0, then product is 0. If p2=0, then smoothed  
#	else:
#		return p1 * log( p1 / (ALPHA * p2 + NALPHA * p1 ) );


"""
Flink User-defined functions
"""


""" MapFunctions """

# """
# Name: PrintValues
# 
# Classe utilizada para especificar uma FlatMapFunction
# Printa na tela todos os valores de um dataset
# 
# Author: 23/07/2016 Matheus Mignoni
# """
class PrintValues(MapFunction):
	def map(self, value):
		print(value);


""" FlatMapFunctions """
# """
# Name: ExistElement
# 
# Classe utilizada para especificar uma FlatMapFunction
# Procura se um determinad elemento existe no dataset
# 
# Author: 23/07/2016 Matheus Mignoni
# """
class ExistElement(FlatMapFunction):
	def __init__(self, elementToFind, positionOnTuple):  #can be parameterized using init()
		self.elementToFind   = elementToFind;
		self.positionOnTuple = positionOnTuple;

	def flat_map(self, value, collector):
		if value[positionOnTuple] == self.elementToFind:
			collector.collect(True);

# """
# Name: EntropyCalculator
# 
# Classe utilizada para especificar uma GroupReduceFunction
# Utilizada em BuildProfiles.py, calcula a entropia para uma palavra
# 
# Retorna a entropia de uma palavra
#
# Author: 23/07/2016 Matheus Mignoni
# Modif:  21/09/2016 Matheus Mignoni
#		  - Não retorna mais o dictionary de links
#
class EntropyCalculator(FlatMapFunction):
	def flat_map(self, value, collector):
		finalEntropy = 0.0;
		palavra    = value[0];
		dictLinks  = value[1][0];
		totalCount = int(value[1][1]);

		for entry in dictLinks.values():
			p = (entry / totalCount)
			finalEntropy = finalEntropy - (p * (math.log(p)));

		#collector.collect((palavra, (dictLinks, int(totalCount), float(finalEntropy))));
		collector.collect((palavra, totalCount, finalEntropy));


""" GroupReduceFunction """
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
# Name: DistinctReduce
# 
# Classe utilizada para especificar uma GroupReduceFunction
# Remove keys duplicadas através de um inteiro
# 
# value = (word,count)
#
# Author: 23/07/2016 Matheus Mignoni
# """
class DistinctReduce(GroupReduceFunction):
	def reduce(self, iterator, collector):
		dic = dict()
		for value in iterator:
			dic[value[0]] = value[1]
		for key in dic.keys():
			collector.collect((key,dic[key]));

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
		collector.collect((word, count))

# """
# Name: Listter
# 
# Classe utilizada para especificar uma GroupReduceFunction
# Retorna todos os values agrupados em uma lista
# 
# Author: 23/07/2016 Matheus Mignoni
class Listter(GroupReduceFunction):
	def reduce(self, iterator, collector):
		for value in iterator:
			collector.collect(value);

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

""" CoGroupFunction """
# """
# Name: SimpleCoGroup
# 
# Classe utilizada para especificar uma CoGroupFunction
# Apenas concatena os values dos datasets
# 
# Author: 23/07/2016 Matheus Mignoni
# """
class SimpleCoGroup(CoGroupFunction):
	def co_group(self, ivals, dvals, collector):
		tupleIVals = ()
		for value in ivals:
			tupleIVals = tupleIVals + (value,)

		tupleDVals = ()
		for value in dvals:
			tupleDVals = tupleDVals + (value,)

		collector.collect( tupleIVals + tupleDVals);

# """
# Name: LeftJoinCoGroup
# 
# Classe utilizada para especificar uma CoGroupFunction
# Apenas concatena os values do dataset a esquerda
# 
# Author: 23/07/2016 Matheus Mignoni
# """
class LeftJoinCoGroup(CoGroupFunction):
	def co_group(self, ivals, dvals, collector):
		#Verifica quantos elementos há no iterator do dataset da direita
		listDVals = []
		for value in dvals:
			listDVals.append(value)


		#coletaremos os values das tuplas do dataseta a esquerda, apenas das tuplas que contém valor em ambos datasets
		if len(listDVals) > 0:
			for value in ivals:
				collector.collect( value );

# """
# Name: RightJoinCoGroup
# 
# Classe utilizada para especificar uma CoGroupFunction
# Apenas concatena os values do dataset a esquerda
# 
# Author: 23/07/2016 Matheus Mignoni
# """
class RightJoinCoGroup(CoGroupFunction):
	def co_group(self, ivals, dvals, collector):
		#Verifica quantos elementos há no iterator do dataset da direita
		listIVals = []
		for value in ivals:
			listIVals.append(value)


		#coletaremos os values das tuplas do dataseta a esquerda, apenas das tuplas que contém valor em ambos datasets
		if len(listIVals) > 0:
			for value in dvals:
				collector.collect( value );


""" JoinFunctions """
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


