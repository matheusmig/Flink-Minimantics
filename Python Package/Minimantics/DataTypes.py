#!/usr/bin/env python
# -*- coding: utf-8 -*- 

""" 
Data Types Module
"""
import math
import struct
import pickle
import decimal
import json
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
			aDec = decimal.Decimal(a)
			bDec = decimal.Decimal(b)
			return aDec * bDec.ln()
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
		return str(self.target)+"\t0.0\t"+str(self.context)+"\t0.0\t"+str(self.targetContextCount)+"\t"+str(self.targetCount)+"\t"+str(self.contextCount)+"\t"+\
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

class SimilaritySerializer(object):
	def serialize(self, value):
		#Define formato de serialização das strings

		#Target String
		target1Encoded = value.target1.encode('utf8')
		target1_size  = len(target1Encoded)
		target1_fmt   = "{}s".format(target1_size)

		#Context String
		target2Encoded = value.target2.encode('utf8')
		target2_size = len(target2Encoded)
		target2_fmt  = "{}s".format(target2_size)

		#Final Format
		bufferFormat = ">i"+target1_fmt+"i"+target2_fmt+"9f"; #target_size, target, target2_size, target2, cosine, wjaccard, lin, l1, l2 jsd, random, askew1, askew2
		return struct.pack(bufferFormat, int(target1_size), target1Encoded, int(target2_size), target2Encoded, float(value.cosine), float(value.wjaccard), float(value.lin), float(value.l1), float(value.l2), float(value.jsd), float(value.randomic), float(value.askew1), float(value.askew2))

class SimilarityDeserializer(object):
	def deserialize(self, read):
		#Target
		target1_size = struct.unpack(">i", read(4))[0] #unpack retorna sempre uma tupla

		target1 = read(target1_size).decode("utf-8");

		#Context
		target2_size = struct.unpack(">i", read(4))[0]

		target2 = read(target2_size).decode("utf-8");

		#cosine
		cosine = struct.unpack(">f", read(4))[0]   		
		#wjaccard
		wjaccard = struct.unpack(">f", read(4))[0]
		#lin
		lin = struct.unpack(">f", read(4))[0]
		#l1
		l1 = struct.unpack(">f", read(4))[0]
		#l2
		l2 = struct.unpack(">f", read(4))[0]
		#jsd
		jsd = struct.unpack(">f", read(4))[0]
		#random
		random = struct.unpack(">f", read(4))[0]
		#askew1
		askew1 = struct.unpack(">f", read(4))[0]
		#askew2
		askew2 = struct.unpack(">f", read(4))[0]

		return Similarity(target1, target2, cosine, wjaccard, lin, l1, l2, jsd, random, askew1, askew2);

class DictOfContexts(object):
	def __init__(self, dictContexts):
		self.dict = dictContexts;

	def returnResultAsStr(self):
		return json.dumps(self.dict);


class DictOfContextsSerializer(object):
	def serialize(self, value):
		#Serialize dicionario
		dataSerialized = pickle.dumps(value.dict);

		dataLength     = len(dataSerialized)
		dataFormat     = "{}s".format(dataLength)

		bufferFormat = ">i"+dataFormat;

		return struct.pack(bufferFormat, dataLength, dataSerialized)


class DictOfContextsDeserializer(object):
	def deserialize(self, read):		
		dataLength      = struct.unpack(">i", read(4))[0]

		dataSerialized  = read(dataLength);
		
		#Deserialize dictionary
		dictAux = pickle.loads(dataSerialized)
		return DictOfContexts(dictAux);







