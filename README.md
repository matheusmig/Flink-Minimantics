# Flink-Minimantics

Ferramenta criada com o Framework Apache Flink (http://flink.apache.org/) utilizando linguagem Scala.

A ferramenta contém 5 funções distintas, onde cada uma gera uma saída para ser utilizada na função subsequente:

1) Criação dos arquivos N-Gram para utilizar como entrada no algoritmo
2) FilterRaw
3) BuildProfiles
4) CalculateSimilarity
5) EvaluateThesaurus



Exemplos de como executar:

1) NGRAM:

--trigram:
-i "hdfs://Minimantics:50040/raw_ukwac_Full" -o "hdfs://Minimantics:50040/Trigram" -stage "NG" -N 3 -group

--entrada:
-i "hdfs://Minimantics:50040/raw_ukwac_Full" -o "hdfs://Minimantics:50040/ukwac_Full" -stage "NG" -N 2 -removeSpecialChars

2) FILTER RAW:
-i "hdfs://Minimantics:50040/ukwac_Full" -o "hdfs://Minimantics:50040/ukwac_Full_FilterRaw" -FW 10 -FP 2 -S 0.2 -stage "FR" -P 16 

3) BUILD PROFILES
-i "hdfs://Minimantics:50040/ukwac_Full_FilterRaw" -o "hdfs://Minimantics:50040/ukwac_Full_BuildProfiles" -FW 10 -FP 2 -S 0.2 -stage "BP" -P 16

4) CALCULATE SIM
-i "hdfs://Minimantics:50040/ukwac_Full_BuildProfiles" -o "hdfs://Minimantics:50040/ukwac_Full_CalculateSimilarity" -FW 10 -FP 2 -S 0.2 -stage "CS" -P 16 -calculateDistances

5) EVALUATE THESAURUS
-i "hdfs://Minimantics:50040/ukwac_1M_CalculateSimilarity" -o  "hdfs://Minimantics:50040/ukwac_1M_Evaluation" -freqFile "hdfs://Minimantics:50040/englishFrequencyList" -trigramFile "hdfs://Minimantics:50040/Trigram" -trigramGrouped -stage "ET" -complexWord "find" -sentencesFile "hdfs://Minimantics:50040/phrases/phrases"

