package it.polimi.genomics.binning

import java.nio.file.FileSystems

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.binning.tooling.DatasetGenerator
import it.polimi.genomics.compiler.Translator
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.binning.tooling._

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by andreagulino on 20/10/16.
  */
object Binning {

  private var xmlPath      = "/Users/andreagulino/Development/GMQL_GIT/GMQL/conf/binning_config.xml"
  private final val execute = true

  final val  logger = LoggerFactory.getLogger(this.getClass)

  var  output_queries:Seq[(String, Seq[(String, Long)])] =  Seq.empty
  var  output_couples:Seq[(String, Long)] =  Seq.empty


 // arg(0) path to xml configuration file
  def main(args: Array[String]): Unit = {



    val s = FileSystems.getDefault().getPath("")
    println(s.toAbsolutePath)

    val ln = args.length
    xmlPath  = if( ln > 0 )  args(0)  else xmlPath

    // CONFIGURATION

    // From XML to configuration objects
    val envConfig     = ConfigParser.getEnvironmentConfig(xmlPath)
    val queryConfigs  = ConfigParser.getQueriesConfigs(xmlPath)
    val binningConfig = ConfigParser.getBinningConfig(xmlPath)
    val dsConfigs     = Array( ConfigParser.getDatasetConfig(xmlPath, "REF"),
      ConfigParser.getDatasetConfig(xmlPath, "EXP"))



    // Environment variables check
    if( (envConfig.TYPE equals "CLUSTER") & (System.getenv("GMQL_EXEC") != "CLUSTER" ) ) {
      println("ERROR: Environment variable GMQL_EXEC must be set to CLUSTER")
      return
    }



    // Initialize FSTools
    var fs  = FSTools

    fs.init_file_system(envConfig)

    // DATASET GENERATION

    println("\nGenerating Datasets\n")

    val datasets: Seq[(String, String)] = for ( datasetConfig <- dsConfigs) yield {

      // Create dataset folder
      val dsPath = fs.createDataset(datasetConfig.NAME)

      var i = 0 // sample index

      for ( sampleConfig <- datasetConfig.samples ) {

        i += 1

        // Generate coordinates
        val coords    = DatasetGenerator.generateRegionData(sampleConfig)

        // Generate metadata
        val metadata = DatasetGenerator.generateMetaData(sampleConfig, i)

        // Add Scores
        val regions   =  DatasetGenerator.addScoreValue(coords, sampleConfig)

        // Store Dataset
        fs.storeSample(datasetConfig.NAME, i, regions, metadata)

      } // end for over samples

      (datasetConfig.NAME, dsPath)

    } // end for over datasets



    //  SPARK CONFIGURATION

    val conf = new SparkConf()
      .setAppName("GMQL V2 Spark")
      .setSparkHome("/usr/local/spark-1.6.2-bin-hadoop2.6/")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.sql.tungsten.enabled", "true")
    //.set("spark.eventLog.enabled", "true")
    //      .set("spark.eventLog.dir", fs.logPath)
    //      .set("spark.history.fs.logDirectory", fs.logPath)
    //       .set("spark.history.ui.port", "3333")

    var sc: SparkContext = null


    // EXECUTION

    // For each query
    for( queryConfig <- queryConfigs) {

      val query_spark = QueryBuilder.get(queryConfig, datasets(0)._2, datasets(1)._2, fs.getOutputPath).get


      //Log
      println("\n\n\n ## New Query ##")
      println(query_spark + "\n\n")

      // For each query -> For each bin size
      for (binSize <- binningConfig.binSizes) {


        // Log
        println("\n\n\n ### Testing Bin Size = " + binSize + "\n"+query_spark+"\n\n\n")


        if (execute) {
          try {
            sc = new SparkContext(conf)

            // Initialize a GMQL Server using the Spark implementation
            val server =
            new GmqlServer(new GMQLSparkExecutor(defaultBinSize = binSize, testingIOFormats = true, maxBinDistance = binSize, sc = sc),
              binning_size = Some(binSize))

            val translator = new Translator(server, fs.getOutputPath)
            val dd = translator.phase1(query_spark)

            // extract the output paths
            //            val outputs = dd.flatMap(x => x match {
            //              case d: MaterializeOperator =>
            //                Some(d.store_path)
            //              case _ => None
            //            })

            translator.phase2(dd)
            val spark_start: Long = System.currentTimeMillis


            // Run the server
            server.run()
            val spark_stop: Long = System.currentTimeMillis

            //  delete the content of the output folder
            fs.clean_outputs()

            val duration = spark_stop - spark_start
            println("\n\n\n\n------------------ DURATION : " + duration + "------------\n\n\n")

            output_couples = output_couples :+ (binSize.toString, duration)


          } catch {
            case e: Exception => e.printStackTrace();
              val err: Long = -1
              output_couples = output_couples :+ (binSize.toString, err)
          } finally {
            sc.stop()

          } //end actual execution

        } //end if(execute)

      } // end for over binSizes

      output_queries = output_queries :+ (queryConfig.getDescription, output_couples)
      // rest output couples
      output_couples = Seq.empty
    }


    // STORING RESULTS

    // Results to XML
    ResultBuilder.buildResult(output_queries, xmlPath )

    // Cleaning folders
    fs.cleanTestFolders

  } // end of Main

} // end of Binning