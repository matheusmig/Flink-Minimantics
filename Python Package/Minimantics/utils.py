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
# Author: 23/07/2016 Matheus Mignoni
class LinksAndCounts(GroupReduceFunction):
  def reduce(self, iterator, collector):   
    linksList = [];
    count     = 0;

    for x in interator:
    	count += x[1]
    	linksList.append(x[0]);

    collector.collect((linksList, count))


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
class PointWeighter(JoinFunction):
	def join(self, pairWords, dataset2):
		if (dataset2[1]):
			return (pairWords[0], dataset2[1] * pairWords[1])
