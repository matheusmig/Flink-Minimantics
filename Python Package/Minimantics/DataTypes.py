#!/usr/bin/env python
# -*- coding: utf-8 -*- 

""" 
Data Types Module
"""
import math
from flink.functions.GroupReduceFunction import GroupReduceFunction

""" 
Constants
"""
c_pathSaveFiles = "/Users/mmignoni/Desktop/results/"
c_HdfsPath = "hdfs://localhost:9000/"+"Minimantics/" 

c_filteRawOutputFile = "filterRawOutput" 
c_BuildProfilesOutputFile = "BuildProfilesOutput"

""" 
Classes
"""
class Profile:
	#Construtor com 8 parâmetros básicos, infere os outros 10 parâmetros a partir dos básicos.
	def __init__(self, target, context, targetContextCount, targetCount, contextCount, entropy_target, entropy_context, nPairs):
		self.target = target;
		self.context = context
		self.targetContextCount = targetContextCount       	
		self.targetCount = targetCount
		self.contextCount = contextCount
		self.entropy_target = entropy_target
		self.entropy_context = entropy_context
		self.nPairs = nPairs

		self.cw1nw2  = targetCount  - targetContextCount;
		self.cnw1w2  = contextCount - targetContextCount;
		self.cnw1nw2 = nPairs - targetCount - contextCount + targetContextCount;

		self.ew1w2   = self.expected(         targetCount,          contextCount, nPairs)
		self.ew1nw2  = self.expected(         targetCount, nPairs - contextCount, nPairs)
		self.enw1w2  = self.expected(nPairs - targetCount,          contextCount, nPairs)
		self.enw1nw2 = self.expected(nPairs - targetCount, nPairs - contextCount, nPairs)

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
		return float((cw1 * cw2)/n);

	def PRODLOG(self, a, b): #Evita calcular log(0)
		if a != 0:
			return a * (math.log(b)) 
		else:
			return 0.0

	def condProb(self):
		return self.targetContextCount / self.targetCount;

	def pmi(self):
		return math.log(self.targetContextCount) - math.log(self.ew1w2);

	def npmi(self):
		return self.pmi / ( math.log(self.nPairs) - math.log(self.targetContextCount))

	def lmi(self):
		return self.targetContextCount * self.pmi

	def tscore(self):
		return (self.targetContextCount - self.ew1w2 ) / math.sqrt( self.targetContextCount )

	def zscore(self):
		return (self.targetContextCount - self.ew1w2 ) / math.sqrt( self.ew1w2 )

	def dice(self):
		return (2.0 * self.targetContextCount) / (self.targetCount + self.contextCount)

	def chisquare(self):
		return math.pow(self.targetContextCount - self.ew1w2, 2) / self.ew1w2  +\
			   math.pow(self.cw1nw2 - self.ew1nw2           , 2) / self.ew1nw2 +\
			   math.pow(self.cnw1w2 - self.enw1w2           , 2) / self.enw1w2 +\
			   math.pow(self.cnw1nw2 - self.enw1nw2         , 2) / self.enw1nw2;

	def loglike(self):
		return (2.0 * (self.PRODLOG( self.targetContextCount, self.targetContextCount / self.ew1w2 ) +
                       self.PRODLOG(             self.cw1nw2, self.cw1nw2  / self.ew1nw2  ) +
                       self.PRODLOG(             self.cnw1w2, self.cnw1w2  / self.enw1w2  ) +
                       self.PRODLOG(            self.cnw1nw2, self.cnw1nw2 / self.enw1nw2 )));
	def affinity(self):
		return 0.5 * (self.targetContextCount / self.targetCount + self.targetContextCount / self.contextCount);

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
		return "target context f_tc f_t f_c entropy_target entropy_context cond_prob pmi npmi lmi tscore zscore dice chisquare loglike affinity"


class SimilarityResult:
	def __init__(self):
		self.target1  = "";
		self.target2  = "";
		self.cosine   = 0.0;
		self.wjaccard = 0.0;
		self.lin      = 0.0;       	
		self.l1       = 0.0;
		self.l2       = 0.0;
		self.jsd      = 0.0;
		self.random   = 0.0;
		self.askew1   = 0.0;
		self.askew2   = 0.0;

	#Funçoes de I/O	
	def returnResultAsStr(self):
		return self.target1+"\t"+self.target2+"\t"+str(self.cosine)+"\t"+str(self.wjaccard)+"\t"+str(self.lin)+"\t"+\
		       str(self.l1)+"\t"+str(self.l2)+"\t"+str(self.jsd)+"\t"+str(self.random)+"\t"+str(self.askew1);     

	@staticmethod
	def returnHeader():
		return "target\tneighbor\tcosine\twjaccard\tlin\tl1\tl2\tjsd\trandom\taskew";
	
"""
Flink User-defined functions
"""
class Adder(GroupReduceFunction):
  def reduce(self, iterator, collector):
    count, word = iterator.next()
    count += sum([x[0] for x in iterator])
    collector.collect((count, word))


