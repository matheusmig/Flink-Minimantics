import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object Minimantics {

    def main(args: Array[String]) {

        val params: ParameterTool = ParameterTool.fromArgs(args)
        if (!params.has("i") && !params.has("o"){
          println("  Parameter Errors")
          println("  Usage: " +
            "--customer <path> --orders <path> --lineitem <path> --nation <path> --output <path>")
          return
        }

    
        //////////////////////////////////////////////////////////////////////
        // Custom Types
        type FilterRawOutput =  DataSet[(String, String, int)]

        //////////////////////////////////////////////////////////////////////
        // Initializations
        val strInputFile   = params.getRequired("i"); 
        val strOutputFile  = params.getRequired("o"); 
        val nParallelism   = params.getInt("P", 1);
        val nRetries       = params.getInt("execution_retries", 0);

        //////////////////////////////////////////////////////////////////////
        // Environment Configuration
        val env = ExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(nParallelism);
        // make parameters available in the web interface
        env.getConfig.setGlobalJobParameters(params)



        //////////////////////////////////////////////////////////////////////
        // Stage Selector
        if (params.has("stage")) {
            val stage  = params.get("stage", 2);

            if bStageSelector == 'FR'
               val output = FilterRaw(env, data, args);
            // else if 
            // ADD OTHER STAGES HERE

        } else {
            //execute all stages
        }

        //////////////////////////////////////////////////////////////////////
        // Write Results
        output.writeText(strOutputFile, WriteMode.OVERWRITE );

        env.execute("Scala WebLogAnalysis Example")

    }

    /////////////////////////////////////////////////////////////////////
    // Auxiliary Function
    private def FilterRaw(env: ExecutionEnvironment, input: DataSet[(String, String)]) : DataSet[(String, String, int)] = {
        val pairWords = input.map { lambda line: line.split(" ") }
                             .map { lambda tuple: (tuple[0].strip(), tuple[1].strip()) };

        if (params.has("steps")){
           pairWords.writeText(strOutputFile+".filterRawOutput.txt", WriteMode.OVERWRITE );  
        }

    }


}
