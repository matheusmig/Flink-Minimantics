package minimantics

import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.concurrent.TimeUnit
import java.text.DecimalFormat

import org.apache.flink.api.common.operators.Order

import scala.collection.mutable.ArrayBuffer
import scala.math._

import java.util.zip.Deflater
import java.util.zip.Inflater
import java.io.ByteArrayOutputStream

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.util.zip.{GZIPOutputStream, GZIPInputStream}

import scala.util.Try

/******************************************************
  **           Main Function
  **
  **  Created by mmignoni on 2017-04-18.
  *****************************************************/
object Minimantics {

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    //////////////////////////////////////////////////////////////////////
    // Initializations
    val strInputFile  = params.getRequired("i")
    val strOutputFile = params.getRequired("o")
    val nParallelism  = params.getInt("P", 1)
    val nRetries      = params.getInt("execution_retries", 0)

    //////////////////////////////////////////////////////////////////////
    // Environment Configuration
    val env =  ExecutionEnvironment.getExecutionEnvironment

    if (params.has("P"))
      env.setParallelism(nParallelism)
    //env.getConfig.setGlobalJobParameters(params)     // make parameters available in the web interface

    //////////////////////////////////////////////////////////////////////
    // Read Inputs
    val data  = env.readTextFile(strInputFile)

    val Output =
      if (params.has("stage")){
        val strStage = params.get("stage")

        if (strStage == "FR") {
          FilterRaw(env, params, data)
        } else if (strStage == "BP") {
          //Format input => http://stackoverflow.com/questions/24682905/converting-array-of-variable-length-to-tuple-in-scala
          val editedData =  data.map{line =>(line.filterNot(_ == '(').filterNot(_ == ')').split(',') ) match {case Array(a,b,c) => ((a.trim,b.trim),c.toInt)} }
          BuildProfiles(env, params, editedData)
        } else if (strStage == "CS") {
          CalculateSimilarity(env, params, data)
        } else if (strStage == "ET") {
          EvaluateThesaurus(env, params, data)
        } else if (strStage == "NG") {
          NGramsDictionary(env, params, data)
        } else
           null

      } else {
        val FROutput = FilterRaw(env, params, data)
        val BPoutput = BuildProfiles(env, params, FROutput)
        val CSoutput = CalculateSimilarity(env, params, BPoutput)
        CSoutput
      }


    //////////////////////////////////////////////////////////////////////
    // Write Results
    Output.writeAsText(strOutputFile, WriteMode.OVERWRITE)

    //////////////////////////////////////////////////////////////////////
    // Execute this crazy program
    val ExecutionResult = env.execute("Scala Minimantics")
    println("Finished with Runtime: " + ExecutionResult.getNetRuntime(TimeUnit.MILLISECONDS) + "ms")

  }


  /******************************************************
    **  Auxiliary Functions
    *****************************************************/
  private def FilterRaw(env: ExecutionEnvironment, params: ParameterTool, input: DataSet[(String)]) : DataSet[((String,String), Int)] = {

    // Inicialização de variáveis
    val nWordLengthThreshold = params.getInt("FW", 0)
    val nPairCountThreshold = params.getInt("FP", 1)
    val bGenSteps = params.has("steps")
    val strOutputFile  = params.getRequired("o")


    // Lê input file linha a linha, dividindo a linha em uma tupla chamada pairWords = (targetWord, contextWord)
    val pairWords: DataSet[(String, String, Int)] =
      input.flatMap{ (line, out: Collector[(String,String,Int)]) => {
        if (line.contains(" ")) {
          line.split(" ") match {
            case Array(a, b) => out.collect((a, b, 1))
            case _ => out.collect(("", "", 0))
          }
        }
      }}

    // Filtering, sorting and uniquing raw triples in inputfile
	  // targets word count, filtering targets that occur more than threshold,
    // with size greater than 1 and sorting by key alphabetcally
    val targetsFiltered: DataSet[(String, Int)] =
      pairWords.map { x => (x._1, 1) }
               .groupBy(0)
               .reduceGroup(reducer = AdderGroupReduce[String])
               .filter { x => (x._1.length > 1) && (x._2 > nWordLengthThreshold) }

    if (bGenSteps)
      targetsFiltered.writeAsText(strOutputFile + ".targets.filter"+nWordLengthThreshold.toString+".txt", WriteMode.OVERWRITE)

    val contextsFiltered: DataSet[(String, Int)] =
      pairWords.map { x => (x._2, 1) }
        .groupBy(0)
        .reduceGroup(reducer = AdderGroupReduce[String])
        .filter { x => (x._1.length > 1) && (x._2 > nWordLengthThreshold) }

    if (bGenSteps)
      contextsFiltered.writeAsText(strOutputFile + ".contexts.filter"+nWordLengthThreshold.toString+".txt", WriteMode.OVERWRITE)


    val PairWordsFiltered : DataSet[(String,String)] =
      pairWords.joinWithTiny(targetsFiltered).where(0).equalTo(0)  { (dataset1, dataset2) => (dataset1._1, dataset1._2)}
               .joinWithTiny(contextsFiltered).where(1).equalTo(0) { (dataset1, dataset2) => (dataset1._1, dataset1._2)}



    if (bGenSteps)
      PairWordsFiltered.writeAsText(strOutputFile + ".filter.t"+nWordLengthThreshold.toString+".c"+nWordLengthThreshold.toString+".txt", WriteMode.OVERWRITE)

    //""" uniquing and couting """
    val PairWordsFilteredUniqueCount : DataSet[((String,String),Int)] =
      PairWordsFiltered.map { x => ((x._1, x._2), 1) }
        .groupBy(0)
        .reduceGroup(reducer = AdderGroupReduce[(String,String)])
        .filter {x => (x._2 >= nPairCountThreshold) }

    if (bGenSteps)
      PairWordsFilteredUniqueCount.writeAsText(strOutputFile + ".filterRawOutput.txt", WriteMode.OVERWRITE)

    //Return value
    PairWordsFilteredUniqueCount
  }

  private def BuildProfiles(env: ExecutionEnvironment, params: ParameterTool, input: DataSet[((String,String),Int)]) : DataSet[String] = {

    // Inicialização de variáveis
    val bGenSteps = params.has("steps")
    val strOutputFile  = params.getRequired("o")

    //Processa entrada para ficar no formato: (target, context, valor)
    val rawData : DataSet[(String,String,Int)] = input.map{x => (x._1._1, x._1._2, x._2 )}

    //nPairs => soma de todos os valores* (terceiro elemento da tupla: (target, context, valor))"
    val nPairs : DataSet[Int] = input.map{ x => x._2}.reduce (_ + _)

    val targetWithLinksAndCounts : DataSet[(String,Int,Double)] =
      rawData.groupBy(0).reduceGroup(reducer = TargetsLinksAndCounts)
        .flatMap(flatMapper = EntropyCalculator)

    val contextWithLinksAndCounts : DataSet[(String,Int,Double)] =
      rawData.groupBy(1).reduceGroup(reducer = ContextsLinksAndCounts)
        .flatMap(flatMapper = EntropyCalculator)

    if (bGenSteps){
      targetWithLinksAndCounts.writeAsText(strOutputFile + ".TargetsEntropy.txt", WriteMode.OVERWRITE)
      contextWithLinksAndCounts.writeAsText(strOutputFile + ".ContextsEntropy.txt", WriteMode.OVERWRITE)
    }

    val TargetsContextsEntropy : DataSet[(String,String,Int,(Int, Double),(Int,Double))]=
      rawData.join(targetWithLinksAndCounts).where(0).equalTo(0) {
        (value1, value2) => (value1._1, value1._2, value1._3, value2._2, value2._3)
      }.join(contextWithLinksAndCounts).where(1).equalTo(0){
        (value1, value2) => (value1._1, value1._2, value1._3, (value1._4, value1._5), (value2._2, value2._3))
      }

    if (bGenSteps)
      TargetsContextsEntropy.writeAsText(strOutputFile + ".JoinedTargetsAndContextsEntropy.txt", WriteMode.OVERWRITE)


	  //Calculate Profiles and prepare for output
    val OutputData = TargetsContextsEntropy.map(new Profiler).withBroadcastSet(nPairs, "broadcastPairs").map{ x => x.toString }

    if (bGenSteps)
      OutputData.writeAsText(strOutputFile + ".BuildProfilesOutput.txt", WriteMode.OVERWRITE)

    OutputData

    }

  private def CalculateSimilarity(env: ExecutionEnvironment, params: ParameterTool, input: DataSet[(String)]) : DataSet[String] = {
    // Inicialização de variáveis
    val bGenSteps       = params.has("steps")
    val strOutputFile   = params.getRequired("o")
    val nAssocThreshold = params.getDouble("A",-9999.0)
    val nSimThreshold   = params.getDouble("S",-9999.0)
    val nDistThreshold  = params.getDouble("D",-9999.0)
    val bCalculateDist  = params.has("calculateDistances")
    val bCompressData   = params.has("compressData")

    //Processa entrada
    val profiles : DataSet[(
      String,String,String,String,String,String,String,String,String,String,
        String,String,String,String,String,String,String,String,String)] =

      //Esperamos ler Linhas com 18 elementos
      input.map{x => x.split("\t")  match {
        case Array(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s) => (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s)
        case _ => ("","","","","0","","","","","","","","","","","","","","")
      }}

    // Os seguintes itens estao acessiveis nas respectivas posicoes:
    //_1  = targetIndex          //_3  = contextIndex  //_5  = targetContextCountIndex  //_6  = targetCountIndex  //_7  = contextCountIndex
    //_8  = condProbIndex        //_9  = pmiIndex      //_10 = npmiIndex                //_11 = lmiIndex          //_12 = tscoreIndex
    //_13 = zscoreIndex          //_14 = diceIndex     //_15 = chisquareIndex           //_16 = loglikeIndex     //_17 = affinityIndex
    //_18 = entropy_targetIndex  //_19 = entropy_contextIndex


    //Filtra dados: - score menores que o limite de AssocThresh
    val filteredData = profiles.filter(x => x._5.toInt > nAssocThreshold)
    //TO DO - implementar filtro de listas de palavras

    if (bGenSteps)
      filteredData.writeAsText(strOutputFile + ".SimilarityFilteredData.txt", WriteMode.OVERWRITE)

    //Primeiramente mapearemos para as tuplas e depois faremos o agrupamento dos targets iguais
    val targetContextsWithZipIndex = filteredData.map{ x => (x._1, x._5.toInt, x._3) }
      .groupBy(0).reduceGroup(reducer = TargetContextsGrouperWithZipIndex(bCompressData))

    if (bGenSteps)
      targetContextsWithZipIndex.writeAsText(strOutputFile + ".SimilarityTargetContexts.txt", WriteMode.OVERWRITE)

    // here we generate the join set where we say that (idx, element) will be joined with all
    // elements whose index is at most idx
    val joinSet = targetContextsWithZipIndex.flatMap{
      input => for (i <- 0 to input._1.toInt) yield (i.toInt, input._2)
    }

    if (bGenSteps)
      joinSet.writeAsText(strOutputFile + ".SimilarityTargetJoinSet.txt", WriteMode.OVERWRITE)

    // doing the join operation
    val CalculatedSimilarities = targetContextsWithZipIndex.joinWithHuge(joinSet).where(_._1).equalTo(_._1).apply {
       (a, b) => {
         if (bCompressData) {
           val mapDecompressedA = deserializeMap(a._2._2._3.asInstanceOf[String])
           val mapDecompressedB = deserializeMap(b._2._2._3.asInstanceOf[String])
           val targetA = a._2._1.asInstanceOf[String]
           val targetB = b._2._1.asInstanceOf[String]

           SimilarityCalculator(targetA, a._2._2._1, a._2._2._2, mapDecompressedA, targetB, b._2._2._1, b._2._2._2, mapDecompressedB , bCalculateDist)
         } else {
           val targetA = a._2._1.asInstanceOf[String]
           val targetB = b._2._1.asInstanceOf[String]
           val dictA   = a._2._2._3.asInstanceOf[Map[String,Int]]
           val dictB   = b._2._2._3.asInstanceOf[Map[String,Int]]
           SimilarityCalculator(targetA, a._2._2._1, a._2._2._2, dictA, targetB, b._2._2._1, b._2._2._2, dictB , bCalculateDist)
         }
       }
    }

    if (bGenSteps)
      CalculatedSimilarities.writeAsText(strOutputFile + ".CalculatedSimilarities.txt", WriteMode.OVERWRITE)

    val OutputData = CalculatedSimilarities.filter(OutputSim(nSimThreshold, nDistThreshold))
                                           .map{ s => s.toStringWithEquivalents }
    OutputData


  }

  private def EvaluateThesaurus(env: ExecutionEnvironment, params: ParameterTool, input: DataSet[(String)]) : DataSet[(String, String, Double)] = {
    //Estamos levando em consideração apenas a medida de Cosine

    //Initializations
    val bGenSteps       = params.has("steps")
    val strOutputFile   = params.getRequired("o")
    val strComplexWord  = params.get("complexWord", "" )      //Palavra alvo a ser substituída
    val nSynonyms       = params.getInt("synonymsCount", 10)  //Número de sinônimos que iremos gerar
    val nThesaurusSize  = params.getInt("thesaurusSize", 50)  //Número de tuplas que pegaremos do thesauro criado em CalcSimilarity
    val strFreqFile     = params.get("freqFile", "")          //Nome do arquivo de frequencias
    val strTrigramsFile = params.get("trigramFile", "")       //Nome do arquivo de TriGrams
    val bGroupedTrigram = params.has("trigramGrouped")        //Flag que indica se o arquivo de Trigram já estava agrupado
    val InputSentences  : DataSet[String] =                   //Frase de entrada
      if (params.has("sentence")){
        val strInputSentence = params.getRequired("sentence")
        env.fromElements(strInputSentence)
      } else {
        val strSentencesFile = params.getRequired("sentencesFile")
        env.readTextFile(strSentencesFile)
      }


    //Lê tesauro de entrada e separa as N tuplas mais similares a palavra alvo
    //Campos: ('Target', 'Neighbor', 'CosineSimilarity')
    val dsReducedThesaurus : DataSet[(String,String, Double)] =
    input.flatMap{ (line,out: Collector[(String,String,Double)]) => {
        line.split("\t")  match {
          case Array(a,b,c,d,e,f,g,h,i,j,k)   => if (a == strComplexWord) out.collect((a,b,c.toDouble)) //Tesauro de entrada foi gerado no passo CalcSimilarity
          case Array(a,b,c,d,e,f,g,h,i,j,k,l) => if (a == strComplexWord) out.collect((a,c,e.toDouble)) //Tesauro de entrada foi gerado no algoritmo original
        }
    }}.groupBy(0)
      .sortGroup(2, Order.DESCENDING)
      .first(nThesaurusSize)

    //Lê arquivo de frequencia
    //Campos: ('palavra', frequencia)
    val dsFrequencies : DataSet[(String,Double)] =
    env.readTextFile(strFreqFile)
      .map{line => line.split(" ") match {
        case Array(a, b) => (a.toLowerCase, b.toDouble)
        case _ => ("",0)
      }}

    //Lê TriGram e faz o count de quantas triplas iguais temos
    //Campos: ('word-1', 'word', 'word+1', count)
    val dsTriGram : DataSet[(String,String,String, Int)] =
      if (bGroupedTrigram){
        env.readTextFile(strTrigramsFile)
          .map { line =>
            line.split(",") match {
              case Array(a, b, c, d) => (a.toLowerCase, b.toLowerCase, c.toLowerCase, d.toInt)
              case _ => ("", "", "", 1)
            }
          }
      } else {
        env.readTextFile(strTrigramsFile)
          .map { line =>
            line.split(" ") match {
              case Array(a, b, c) => ((a.toLowerCase, b.toLowerCase, c.toLowerCase), 1)
              case _ => (("", "", ""), 1)
            }
          }
          .groupBy(0)
          .reduceGroup(reducer = AdderGroupReduce[(String, String, String)])
          .map { x => (x._1._1, x._1._2, x._1._3, x._2) }
      }


    //Obtem a freqüência de cada um dos M vizinhos da palavra alvo.
    //Campos: ('Target', 'Neighbor', CosineSimilarity, frequencyNeighbor)
    val F : DataSet[(String,String,Double,Double)] =
    dsReducedThesaurus.join(dsFrequencies).where(1).equalTo(0){(dataset1, dataset2) => (dataset1._1, dataset1._2, dataset1._3, dataset2._2)}
      .sortPartition(3, Order.DESCENDING)

    //Obtem o contexto da(s) frase(s) de entrada
    //Contexto é (alvo-1, ALVO, alvo+1)
    val ContextInputs = InputSentences.flatMap{ (line,out: Collector[(String,String,String)]) => {
      if (line contains strComplexWord){
        val arLine = line.split(" ")
        val nTargetPos = arLine.indexOf(strComplexWord)

        var strTargetMinus1 = ""
        var strTarget       = strComplexWord
        var strTargetPlus1  = ""
        if (nTargetPos > 0) {
          if (nTargetPos == 0) {
            strTargetMinus1 = ""
            strTarget       = arLine(nTargetPos)
            strTargetPlus1  = arLine(nTargetPos+1)
          } else if (nTargetPos == arLine.length-1) {
            strTargetMinus1 = arLine(nTargetPos-1)
            strTarget       = arLine(nTargetPos)
            strTargetPlus1  = ""
          } else {
            strTargetMinus1 = arLine(nTargetPos - 1)
            strTarget = arLine(nTargetPos)
            strTargetPlus1 = arLine(nTargetPos + 1)
          }
        }
        out.collect((strTargetMinus1.toLowerCase, strTarget.toLowerCase, strTargetPlus1.toLowerCase))
      }
    }}

    //Filtra os trigrams que tem mesmo contexto que a frase de entrada,
    // e em seguida filtra os que tem a palavra w, de (w-1 w w+1), igual aos neighbors da palavra alvo
    val LM = dsTriGram.join(ContextInputs).where(0).equalTo(0){  (dataset1, dataset2) => dataset1 }
      .join(ContextInputs).where(2).equalTo(2){  (dataset1, dataset2) => dataset1 }
      .join(dsReducedThesaurus).where(1).equalTo(1){ (dataset1, dataset2) => dataset1 }


    //Nesta parte já possuímos F e LM, usaremos suas informaçoes para inferir os melhores vizinhos da palavra alvo
    // F  = ('Target",'Neighbor', cosine, frequency)
    // LM = (w-1 wNeighbor w+1, count)
    val Evaluation = F.leftOuterJoin(LM).where(1).equalTo(1) {
      // Bônus Para cada vez que a palavra aparece no contexto : similaridade * frequencia
      (dataset1, dataset2) =>
        if (dataset2 != null) {
          val nContextBonus = dataset1._3 + log(dataset2._4 + 1) //Bonus de contexto = similaridade +ln(nContextCount)
          val finalScore = (nContextBonus + log(dataset1._4)/10) //Score Final = Bonus contexto + ln(Frequency)/10
          (dataset1._1, dataset1._2, finalScore)
        } else {
          val finalScore = (dataset1._3  + log(dataset1._4)/10)  //Score Final = Similaridade + ln(Frequency)/10
          (dataset1._1, dataset1._2, finalScore)
        }
    }.groupBy(0)
      .sortGroup(2, Order.DESCENDING)
      .first(nSynonyms)

    if (bGenSteps) {
      F.writeAsText(strOutputFile + ".FrequencyList.txt", WriteMode.OVERWRITE)
      LM.writeAsText(strOutputFile + ".LM.txt", WriteMode.OVERWRITE)
      dsReducedThesaurus.writeAsText(strOutputFile + ".reducedThesaurus.txt", WriteMode.OVERWRITE)
      Evaluation.writeAsText(strOutputFile + ".FinalEvaluation2.txt", WriteMode.OVERWRITE)
    }

    Evaluation
 }

private def NGramsDictionary(env: ExecutionEnvironment, params: ParameterTool, input: DataSet[(String)]) : DataSet[String] = {
  //Initializations
  val nGram              = params.getInt("N", 2)
  val bGrouped           = params.has("group")
  val bRemoveSpecialChar = params.has("removeSpecialChars")

  val nGramDictionary =
    if (bGrouped) {
      input.flatMap(
        (x : String, out: Collector[(String, Int)]) => {
          if (bRemoveSpecialChar) {
            x.replaceAll("[~!@#$^%&*\\\\(\\\\)_+={}\\\\[\\\\]|;:\\\"'<,>.?`/\\\\\\\\-]", "")  //Remove caracteres especiais
              .replaceAll(" +", " ")                                                          //Remove espaços duplos
              .split(' ').sliding(nGram).foreach( p => out.collect((p.mkString(","),1)))
          } else {
            x.split(' ').sliding(nGram).foreach( p => out.collect((p.mkString(","),1)))
          }
        }
      ).groupBy(0)
       .reduceGroup(reducer = AdderGroupReduce[(String)])
       .map(x => (x._1+','+x._2.toString))
    } else {
      input.flatMap(
        (x : String, out: Collector[String]) => {
          if (bRemoveSpecialChar) {
            x.replaceAll("[~!@#$^%&*\\\\(\\\\)_+={}\\\\[\\\\]|;:\\\"'<,>.?`/\\\\\\\\-]", "")  //Remove caracteres especiais
              .replaceAll(" +", " ")                                                          //Remove espaços duplos
              .split(' ').sliding(nGram).foreach( p => out.collect(p.mkString(" ")))
          } else {
            x.split(' ').sliding(nGram).foreach( p => out.collect(p.mkString(" ")))
          }
        }
      )
    }

  nGramDictionary
}


/******************************************************
**  Custom Flink Iterations Functions
*****************************************************/
private def AdderGroupReduce[A] = new GroupReduceFunction[(A, Int), (A, Int)] {
override def reduce(values: java.lang.Iterable[(A, Int)], out: Collector[(A, Int)]): Unit = {
  var anyItem : A = null.asInstanceOf[A]
  var nSum = 0
  for ((key, value) <- values) {
    anyItem = key
    nSum += value
  }

  // Emit as Tuple of (key, sum)
  out.collect((anyItem, nSum))
}
}

private def TargetsLinksAndCounts = new GroupReduceFunction[(String,String,Int),(String, (scala.collection.mutable.Map[String,Int], Int))] {
override def reduce(values: java.lang.Iterable[(String, String, Int)], out: Collector[(String, (scala.collection.mutable.Map[String,Int], Int))]): Unit = {
  var dictLinks = scala.collection.mutable.Map[String,Int]()
  var nCount = 0
  var strTarget = ""

  for ((key0, key1, value) <- values){
    strTarget = key0
    nCount = nCount + value
    if (dictLinks.contains(key1)) {
      dictLinks(key1) = dictLinks(key1) + value
    } else {
      dictLinks += (key1 -> value)
    }
  }
  out.collect((strTarget, (dictLinks, nCount)))
}
}

private def ContextsLinksAndCounts = new GroupReduceFunction[(String,String,Int),(String, (scala.collection.mutable.Map[String,Int], Int))] {
override def reduce(values: java.lang.Iterable[(String, String, Int)], out: Collector[(String, (scala.collection.mutable.Map[String,Int], Int))]): Unit = {
  var dictLinks = scala.collection.mutable.Map[String,Int]()
  var nCount = 0
  var strTarget = ""

  for ((key0, key1, value) <- values){
    strTarget = key1
    nCount = nCount + value
    if (dictLinks.contains(key0)) {
      dictLinks(key0) = dictLinks(key0) + value
    } else {
      dictLinks += (key0 -> value)
    }
  }
  out.collect((strTarget, (dictLinks, nCount)))
}
}

private def TargetContextsGrouperWithZipIndex(bCompress : Boolean) = new GroupReduceFunction[(String, Int, String), (Int,(Any, (Int, Int, Any)))] {
  var nUniqueId = 0

  override def reduce(values: java.lang.Iterable[(String, Int, String)], out: Collector[(Int, (Any, (Int, Int, Any)))]): Unit = {
    var strTarget    = " "
    var nSum 	       = 0
    var nSumSquare   = 0
    var dictContexts = scala.collection.mutable.Map[String,Int]()

    for ((target, count, context) <- values){
      strTarget  = target
      nSum       += count
      nSumSquare += count*count
      dictContexts += (context -> count.toInt)
    }

    if (bCompress){
      val compressedDict = serializeMap(dictContexts.toMap)
      out.collect((nUniqueId, (strTarget, (nSum, nSumSquare, compressedDict))))
    } else {
      out.collect((nUniqueId, (strTarget, (nSum, nSumSquare, dictContexts.toMap))))
    }

    nUniqueId = nUniqueId + 1
  }
}

private def EntropyCalculator = new FlatMapFunction[(String, (scala.collection.mutable.Map[String,Int], Int)),(String,Int,Double)] {
override def flatMap(t: (String, (scala.collection.mutable.Map[String,Int], Int)), collector: Collector[(String,Int,Double)]): Unit = {
  var finalEntropy = 0.0
  val dictLinks = t._2._1
  val totalCount = t._2._2

  dictLinks.map{
    x => {
      val p : Double = x._2.toDouble / totalCount.toDouble
      finalEntropy = finalEntropy - (p * log(p))
    }
  }

  collector.collect((t._1, totalCount, finalEntropy))
}
}


class Profiler extends RichMapFunction[(String,String,Int,(Int, Double),(Int,Double)), Profile] {
private var nPairs = 0

override def open(config: Configuration): Unit = {
  val nBroadcastPairs = getRuntimeContext().getBroadcastVariable[Int]("broadcastPairs").asScala
  nPairs = nBroadcastPairs.head
}

def map(in: (String,String,Int,(Int, Double),(Int,Double))): Profile = {
  new Profile(in._1, in._2, in._3.toDouble, in._4._1.toDouble, in._5._1.toDouble,
             in._4._2.toDouble, in._5._2.toDouble, this.nPairs.toDouble)
}
}

private def OutputSim(nSimThresh : Double, nDistThresh : Double) = new FilterFunction[Similarity] {
def filter(value: Similarity) : Boolean = {
  if (value.target1 == value.target2){
    false
  } else {
    if (((nSimThresh  != -9999.0) && (value.cosine < nSimThresh)) ||
        ((nDistThresh != -9999.0) && ((value.lin > nDistThresh) && (value.l1 > nDistThresh) && (value.l1 > nDistThresh) && (value.jsd > nDistThresh)) )){
      false
    } else {
      true
    }
  }
}
}

private def SimilarityCalculator(strTarget1 : String, nSum1 : Int, nSquareSum1 : Int, dictContexts1 : Map[String,Int],
                                 strTarget2 : String, nSum2 : Int, nSquareSum2 : Int, dictContexts2 : Map[String,Int], bCalcDistance : Boolean) : Similarity = {

    //Inicializações
    var result : Similarity = new Similarity()
    var sumsum = 0.0

    for ((k1,v1) <- dictContexts1){
      if (dictContexts2.contains(k1)){
        val v2 = dictContexts2(k1)
        sumsum += v1 + v2
        result.cosine += v1 * v2
        if (bCalcDistance){
          var nDiffAbs = Math.abs(v1 - v2)
          result.l1     += nDiffAbs
          result.l2     += nDiffAbs * nDiffAbs
          result.askew1 += relativeEntropySmooth(v1, v2)
          result.askew2 += relativeEntropySmooth(v2, v1)
          result.jsd    += relativeEntropySmooth(v1, (v1+v2)/2) + relativeEntropySmooth(v2, (v1+v2)/2)
        }
      } else {
        if (bCalcDistance){
          result.askew1 += relativeEntropySmooth(v1, 0)
          result.jsd    += relativeEntropySmooth(v1, v1/2)
          result.l1     += v1
          result.l2     += v1 * v1
        }
      }
    }

    //Distance measures use the union of contexts and require this part
    if (bCalcDistance){
      for ((k2,v2) <- dictContexts2){
        if (!dictContexts1.contains(k2)){
          result.askew2 += relativeEntropySmooth( v2, 0 )
          result.jsd    += relativeEntropySmooth( v2, v2/2.0 )
          result.l1     += v2
          result.l2     += v2 * v2
        }
      }
      result.l2 = sqrt( result.l2 )
    }

    val dividendoSqrt = sqrt(nSquareSum1) * sqrt(nSquareSum2)
    if (dividendoSqrt != 0)
      result.cosine = result.cosine / dividendoSqrt

    val dividendoSum = nSum1 + nSum2
    if (dividendoSum != 0)
      result.lin = sumsum / dividendoSum

    val dividendoWJaccard = nSum1 + nSum2 - (sumsum/2.0)
    if (dividendoWJaccard != 0)
      result.wjaccard = (sumsum/2.0) / dividendoWJaccard

    result.randomic = random

    result.target1 = strTarget1
    result.target2 = strTarget2
    result
}

class Synonymer(strTarget : String) extends RichFlatMapFunction[(String), (String)] {
var arSynonyms : (String, ArrayBuffer[(String,Float)]) = ("", ArrayBuffer[(String,Float)]())

  override def open(config: Configuration): Unit = {
    val extractedSynonyms = getRuntimeContext().getBroadcastVariable[(String,ArrayBuffer[(String,Float)])]("extractedSynonyms").asScala
    arSynonyms = extractedSynonyms.head
  }

  def flatMap(in: String, out : Collector[String]) = {
    if (in contains strTarget) {
      out.collect(in) //Collect the original sentence
      for (strSynonym <- arSynonyms._2){
        out.collect( in.replaceAll("""\b"""+strTarget, strSynonym._1) )
      }
    }
  }
}


/******************************************************
**  Utils functions
*****************************************************/
  def relativeEntropySmooth(p1 : Double, p2: Double) : Double = {
    val ALPHA  = 0.99  //Smoothing factors for a-skewness measure
    val NALPHA = 0.01
    if (p1 == 0.0)
      0.0 //If p1=0, then product is 0. If p2=0, then smoothed
    else
      p1 * (log( p1 / ((ALPHA * p2) + (NALPHA * p1) )) )
  }

  def serializeMap(map : Map[String,Int]) : String = {
    var strOutput = ""
    for ((key,value) <- map){
      if (strOutput != ""){
        strOutput += ","
      }
      strOutput += key.trim+":"+value.toString
    }
    strOutput
  }

  def deserializeMap(strSerialized : String) : Map[String,Int] = {
    val pairs = strSerialized.split(":|,").grouped(2)
    val mapOutput = pairs.map { case Array(k, v) => k -> v.toInt }.toMap
    mapOutput
  }

//Não utilizado
  def decompress(compressed: Array[Byte]): String = {
    val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
    scala.io.Source.fromInputStream(inputStream).mkString
  }

//Não utilizado
  def compress(input: String): Array[Byte] = {
    val arInput = input.getBytes("UTF-8")
    val bos = new ByteArrayOutputStream(arInput.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(arInput)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }


/******************************************************
**  Data Types
*****************************************************/
class Profile(val target : String, val context : String, val targetContextCount : Double, val targetCount : Double,
            val contextCount : Double, val entropy_target : Double, val entropy_context : Double, val nPairs : Double){

private var cw1nw2  : Double = targetCount - targetContextCount
private var cnw1w2  : Double = contextCount - targetContextCount
private var cnw1nw2 : Double = nPairs - targetCount- contextCount + targetContextCount

private var ew1w2   : Double = expected(targetCount, contextCount, nPairs)
private var ew1nw2  : Double = expected(targetCount, nPairs - contextCount, nPairs)
private var enw1w2  : Double = expected(nPairs - targetCount, contextCount, nPairs)
private var enw1nw2 : Double = expected(nPairs - targetCount, nPairs - contextCount, nPairs)

private var condProb  : Double = funCondProb()
private var pmi       : Double = funPmi()
private var npmi      : Double = funNpmi()
private var lmi       : Double = funLmi()
private var tscore    : Double = funTscore()
private var zscore    : Double = funZscore()
private var dice      : Double = funDice()
private var chisquare : Double = funChisquare()
private var loglike   : Double = funLoglike()
private var affinity  : Double = funAffinity()

private def expected(cw1 : Double, cw2 : Double, n : Double) : Double =   (cw1 * cw2)/n
private def funCondProb()  : Double = targetContextCount / targetCount
private def funPmi()       : Double = math.log(targetContextCount) - math.log(ew1w2)
private def funNpmi()      : Double = pmi / ( math.log(nPairs) - math.log(targetContextCount) )
private def funLmi()       : Double = targetContextCount * pmi
private def funTscore()    : Double = (targetContextCount - ew1w2 ) / math.sqrt(targetContextCount)
private def funZscore()    : Double = (targetContextCount - ew1w2 ) / math.sqrt(ew1w2)
private def funDice()      : Double = (2 * targetContextCount) / (targetCount + contextCount)
private def funChisquare() : Double = {
  var r1 = 0.0; var r2 = 0.0; var r3 = 0.0; var r4 = 0.0

  if (ew1w2 != 0)   r1 = math.pow(targetContextCount - ew1w2, 2) / ew1w2
  if (ew1nw2 != 0)  r2 = math.pow(cw1nw2 - ew1nw2, 2) / ew1nw2
  if (enw1w2 != 0)  r3 = math.pow(cnw1w2 - enw1w2, 2) / enw1w2
  if (enw1nw2 != 0) r4 = math.pow(cnw1nw2 - enw1nw2, 2) / enw1nw2

  r1+r2+r3+r4
}

private def funLoglike() : Double = {
  var r1 = 0.0; var r2 = 0.0; var r3 = 0.0; var r4 = 0.0

  if (ew1w2 != 0)   r1 = PRODLOG(targetContextCount , targetContextCount / ew1w2)
  if (ew1nw2 != 0)  r2 = PRODLOG(cw1nw2             , cw1nw2  / ew1nw2  )
  if (enw1w2 != 0)  r3 = PRODLOG(cnw1w2             , cnw1w2  / enw1w2  )
  if (enw1nw2 != 0) r4 = PRODLOG(cnw1nw2            , cnw1nw2 / enw1nw2 )

  2 * (r1 + r2 + r3 + r4)
}

private def funAffinity(): Double = 0.5 * ((targetContextCount / targetCount) + (targetContextCount / contextCount))


//#Evita calcular log(0)
private def PRODLOG(a : Double, b: Double) : Double = {
  if ((a > 0) && (b > 0)){
    a * math.log(b)
  } else {
    0.0
  }
}

override def toString : String = {
  val formatter = new DecimalFormat("#.##########") //Limita em no máximo 10 casas decimais (economia de espaço)

  target +"\t"+ "0.0" +"\t"+ context +"\t"+ "0.0" +"\t"+
    formatter.format(targetContextCount) +"\t"+ formatter.format(targetCount)   +"\t"+ formatter.format(contextCount) +"\t"+
    formatter.format(condProb)           +"\t"+ formatter.format(pmi)           +"\t"+ formatter.format(npmi)         +"\t"+
    formatter.format(lmi)                +"\t"+ formatter.format(tscore)        +"\t"+ formatter.format(zscore)       +"\t"+
    formatter.format(dice)               +"\t"+ formatter.format(chisquare)     +"\t"+ formatter.format(loglike)      +"\t"+
    formatter.format(affinity)           +"\t"+ formatter.format(entropy_target)+"\t"+ formatter.format(entropy_context)
}
}

class Similarity(var target1  : String, var idTarget1 : Int,    var target2 : String, var idTarget2 : Int,
               var cosine   : Double, var wjaccard  : Double, var lin      : Double, var l1      : Double,
               var l2       : Double, var jsd       : Double, var randomic : Double, var askew1  : Double, var askew2 : Double){

  //Secondary Constructor
  def this(){
    this("",0,"",0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)
  }

  override def toString : String = {
    val formatter = new DecimalFormat("#.######") //Limita o arquivo de saída em 6 casas decimais (economia de espaço)

    target1                      +"\t"+ target2                  +"\t"+ formatter.format(cosine) +"\t"+ formatter.format(wjaccard) +"\t"+
      formatter.format(lin)      +"\t"+ formatter.format(l1)     +"\t"+ formatter.format(l2)     +"\t"+ formatter.format(jsd)      +"\t"+
      formatter.format(randomic) +"\t"+ formatter.format(askew1) +"\t"+ formatter.format(askew2)

  }


  def toStringWithEquivalents : String = {
    val formatter = new DecimalFormat("#.######") //Limita o arquivo de saída em 6 casas decimais (economia de espaço)

      target1                    +"\t"+ target2                  +"\t"+ formatter.format(cosine) +"\t"+ formatter.format(wjaccard) +"\t"+
      formatter.format(lin)      +"\t"+ formatter.format(l1)     +"\t"+ formatter.format(l2)     +"\t"+ formatter.format(jsd)      +"\t"+
      formatter.format(randomic) +"\t"+ formatter.format(askew1) +"\t"+ formatter.format(askew2) +"\n"+
      target2                    +"\t"+ target1                  +"\t"+ formatter.format(cosine) +"\t"+ formatter.format(wjaccard) +"\t"+
      formatter.format(lin)      +"\t"+ formatter.format(l1)     +"\t"+ formatter.format(l2)     +"\t"+ formatter.format(jsd)      +"\t"+
      formatter.format(randomic) +"\t"+ formatter.format(askew1) +"\t"+ formatter.format(askew2)
  }


}

}
