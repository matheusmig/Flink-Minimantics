#!/usr/bin/env python
# -*- coding: utf-8 -*- 

""" 
Data Types Module
"""
import math
import struct
import decimal
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

class ProfileSerializer(object):
	def serialize(self, value):
		#Define formato de serialização das strings

		#Target String
		targetEncoded = value.target.encode('utf8')
		target_size  = len(targetEncoded)
		target_fmt   = "{}s".format(target_size)

		#Context String
		contextEncoded = value.context.encode('utf8')
		context_size = len(contextEncoded)
		context_fmt  = "{}s".format(context_size)

		#Final Format
		bufferFormat = ">i"+target_fmt+"i"+context_fmt+"qqqffq"; #target_size, target, con9text_size, context, targetContextCount, targetCount, contextCount, entropy_target, entropy_context, nPairs
		return struct.pack(bufferFormat, target_size, targetEncoded, context_size, contextEncoded, int(value.targetContextCount), int(value.targetCount), int(value.contextCount), float(value.entropy_target), float(value.entropy_context), int(value.nPairs))

class ProfileDeserializer(object):
	def _deserialize(self, read):
		#See: Slice Notation http://stackoverflow.com/questions/509211/explain-pythons-slice-notation
		nStart = 0;

		#Target
		target_size = struct.unpack(">i", read[nStart:nStart+4])[0] #unpack retorna sempre uma tupla
		nStart = nStart + 4;

		target = read[nStart:nStart+target_size].decode("utf-8");
		nStart = nStart + target_size;

		#Context
		context_size = struct.unpack(">i", read[nStart:nStart+4])[0]
		nStart = nStart + 4;

		context = read[nStart:nStart+context_size].decode("utf-8");
		nStart = nStart + context_size;

		#targetContextCount
		targetContextCount = struct.unpack(">q", read[nStart:nStart+8])[0]
		nStart = nStart + 8;        
		#targetCount
		targetCount = struct.unpack(">q", read[nStart:nStart+8])[0]
		nStart = nStart + 8;      
		#contextCount
		contextCount = struct.unpack(">q", read[nStart:nStart+8])[0]
		nStart = nStart + 8;      
		#entropy_target
		entropy_target = struct.unpack(">f", read[nStart:nStart+4])[0]
		nStart = nStart + 4;  
		#context_target
		context_target = struct.unpack(">f", read[nStart:nStart+4])[0]
		nStart = nStart + 4;  		
		#nPairs
		nPairs = struct.unpack(">q", read[nStart:nStart+8])[0]
		nStart = nStart + 8;    

		return Profile(target, context, targetContextCount, targetCount, contextCount, entropy_target, context_target, nPairs);

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
	


