""" 
Build Profiles Module
"""
from .utils import *
from .DataTypes import *

from flink.plan.Environment import get_environment
from flink.plan.DataSet import *
from flink.functions.GroupReduceFunction import GroupReduceFunction
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
	bSaveOutput   = vars(args)['GenerateSteps']
	strOutputFile = vars(args)['OutFile'];
	

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
	" targetWithLinksAndCounts  => target com sua lista de links e sua contagem: (target,  (links, sum(target valor))) 
	" contextWithLinksAndCounts => context com sua lista de links e sua contagem: (context, (links, sum(context valor)))
	"""
	targetWithLinksAndCounts  = rawData.group_by(0).reduce_group(TargetsLinksAndCounts()); #Gera a lista de links e o somatório total para cada Target -->  #Gera: (go, ({'lot': 3, 'NNP': 3, 'season': 2}, 8))
	contextWithLinksAndCounts = rawData.group_by(1).reduce_group(ContextLinksAndCounts()); #Gera a lista de links e o somatório total para cada Context
#	targetWithLinksAndCounts.write_text(strOutputFile+".targetWithLinksAndCounts.txt", WriteMode.OVERWRITE ); #ex gerado: (go, ({'lot': 3, 'NNP': 3, 'season': 2}, 8))
#	contextWithLinksAndCounts.write_text(strOutputFile+".contextWithLinksAndCounts.txt", WriteMode.OVERWRITE );


	"""	
	" Calculate entropies for contexts and for targets ==> (palavra, totalCount, entropy)"
	"""
	TargetsEntropy  = targetWithLinksAndCounts.flat_map(EntropyCalculator());  #Gera: (target, count, entropy))
	ContextsEntropy = contextWithLinksAndCounts.flat_map(EntropyCalculator()); #Gera: (context, count, entropy))

	#Junta os dados
#	TargetsEntropy.write_text(strOutputFile+".TargetsEntropy.txt", WriteMode.OVERWRITE );
	b = TargetsEntropy; so ESTA FUNCIONANDO SE FAZEMOS WRITE_TEXT, WHY?
#	rawData.write_text(strOutputFile+".rawData.txt", WriteMode.OVERWRITE );
	JoinedTargetsEntropy = rawData.join(b).where(0).equal_to(0).using(JoinTargetsCountAndEntropy());
#	JoinedTargetsEntropy.write_text(strOutputFile+".JoinedTargetsEntropy.txt", WriteMode.OVERWRITE );
	return 2;
	# Esta é a parte com o maior volume de dados desta etapa
#	JoinedTargetsAndContextsEntropy = JoinedTargetsEntropy.join(ContextsEntropy).where(1).equal_to(0).using(JoinContextsCountAndEntropy());
#	JoinedTargetsAndContextsEntropy.write_text(strOutputFile+".JoinedTargetsAndContextsEntropy.txt", WriteMode.OVERWRITE );
	
	""" 
	" Calculate Profiles and prepare for output "
	"""
	#OutputFinal = JoinedTargetsAndContextsEntropy.map(lambda tuple: Profile(tuple[0], tuple[1], float(tuple[2]), float(tuple[3][0]), float(tuple[4][0]), float(tuple[3][1]), float(tuple[4][1]), nPairs ))
	#OutputFinal.write_text(strOutputFile+".OutputFinal.txt", WriteMode.OVERWRITE );

	"""
	" Output data "
	"""		
#	outputHeaderRDD = sc.parallelize( [Profile.returnHeader()] ) ;
#	outputDataRdd   = sc.parallelize(listOutputFinal)
#
#	outputRdd = outputHeaderRDD.union(outputDataRdd)
#
#
#	if bSaveOutput:
#		utils.saveToSSV(outputRdd, "SPARKmini.1.profiles")
#
#	return outputRdd
