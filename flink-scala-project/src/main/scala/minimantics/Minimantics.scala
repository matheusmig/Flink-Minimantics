package minimantics

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import collection.JavaConversions._

/**
  * Created by mmignoni on 2017-04-18.
  */
object Minimantics {

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    //////////////////////////////////////////////////////////////////////
    // Custom Types
    //type FilterRawOutput =  DataSet[(String, String, int)]

    //////////////////////////////////////////////////////////////////////
    // Initializations
    val strInputFile = params.getRequired("i")
    val strOutputFile = params.getRequired("o")
    val nParallelism = params.getInt("P", 1)
    val nRetries = params.getInt("execution_retries", 0)

    //////////////////////////////////////////////////////////////////////
    // Environment Configuration
    val env = if ((!params.has("Addr")) || (!params.has("Port")) || (!params.has("Jar"))) {
      ExecutionEnvironment.getExecutionEnvironment
    } else {
      val strRemoteEnvironmentAddr = params.get("Addr")
      val intRemoteEnvironmentPort = params.getInt("Port", 6123)
      val strRemoteEnvironmentJar = params.get("Jar")
      ExecutionEnvironment.createRemoteEnvironment(strRemoteEnvironmentAddr, intRemoteEnvironmentPort, strRemoteEnvironmentJar)
    }

    env.setParallelism(nParallelism)
    env.getConfig.setGlobalJobParameters(params)     // make parameters available in the web interface

    //////////////////////////////////////////////////////////////////////
    // Read Inputs
    val data  = env.readTextFile(strInputFile)

    val output = FilterRaw(env, params, data)

    //////////////////////////////////////////////////////////////////////
    // Write Results
    output.writeAsText(strOutputFile, WriteMode.OVERWRITE)

    //////////////////////////////////////////////////////////////////////
    // Execute this crazy program
    env.execute("Scala Minimantics")
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
      input.map { str => str.split(" ")}
           .map { tup => ( tup(0), tup(1) , 1) }

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
      pairWords.join(targetsFiltered).where(0).equalTo(0)  { (dataset1, dataset2) => (dataset1._1, dataset1._2)}
               .join(contextsFiltered).where(0).equalTo(0) { (dataset1, dataset2) => (dataset1._1, dataset1._2)}


    if (bGenSteps)
      contextsFiltered.writeAsText(strOutputFile + ".filter.t"+nWordLengthThreshold.toString+".c"+nWordLengthThreshold.toString+".txt", WriteMode.OVERWRITE)

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

}
