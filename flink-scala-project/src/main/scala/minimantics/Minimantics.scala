package minimantics

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

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
    val strInputFile   = params.getRequired("i")
    val strOutputFile  = params.getRequired("o")
    val nParallelism   = params.getInt("P", 1)
    val nRetries       = params.getInt("execution_retries", 0)

    //////////////////////////////////////////////////////////////////////
    // Environment Configuration
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(nParallelism)
    env.getConfig.setGlobalJobParameters(params)     // make parameters available in the web interface

    //////////////////////////////////////////////////////////////////////
    // Read Inputs
    val data  = env.readTextFile(strInputFile)

    val output = FilterRaw(env, data)

    //////////////////////////////////////////////////////////////////////
    // Write Results
    output.writeAsText(strOutputFile, WriteMode.OVERWRITE)

    //////////////////////////////////////////////////////////////////////
    // Execute this crazy program
    env.execute("Scala Minimantics")
  }


  /////////////////////////////////////////////////////////////////////
  // Auxiliary Function
  private def FilterRaw(env: ExecutionEnvironment, input: DataSet[(String)]) : DataSet[(String, Int)] = {

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
               .reduceGroup ( new GroupReduceFunction[(String,Int), (String,Int)] {
                   override def reduce(values: Iterable[(String,Int)], out: Collector[(String,Int)]): Unit = {
                   val strTarget = values.head._1
                   val nTargetsum = values.reduce(_._2 + _._2)
                   out.collect((strTarget, nTargetsum))
                 }
               })
    targetsFiltered
  }


}
