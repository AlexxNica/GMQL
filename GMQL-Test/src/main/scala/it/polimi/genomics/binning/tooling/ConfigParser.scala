package it.polimi.genomics.binning.tooling


import scala.xml.{Elem, Node, NodeSeq, XML}

/**
  * Created by andreagulino on 11/11/16.
  */


class QueryConfig(query: NodeSeq) {
  val DLE:Int = (query \\ "dle").text.trim.toInt
  var DGE:Option[Int] = None

  private val dge_text = (query \\ "dge").text.trim

  if( dge_text != "") {
    DGE = Some(dge_text.toInt)
  }

  def getDescription():String = {
    val dge = if(DGE.isDefined) ", DGE: "+DGE.get else ", NO DGE"
    return "DLE: "+ DLE + dge
  }

}

class BinningConfig(binning: NodeSeq) {

  var binSizes: Array[Int] = Array[Int]()

  private val discrete = (binning \\ "discrete").text.trim

  if( discrete equals "true" ) {

    // Read from the list of sizes
    val sizes:NodeSeq = (binning \\ "bin_sizes" \\ "size")
    binSizes = new Array[Int](sizes.size)

    for( sizeN <- 0 to sizes.size - 1 ) {
      binSizes(sizeN) = sizes(sizeN).text.trim.toInt
    }

  } else {

    val sampling = (binning \\ "sampling").text.trim.toInt
    val minSize  = (binning \\ "min_size").text.trim.toInt
    val maxSize  = (binning \\ "max_size").text.trim.toInt

    binSizes = binSizes :+ minSize

    for( num <- minSize to maxSize) {
      if( num%sampling == 0) {
        binSizes = binSizes :+ num
      }
    }

  }

}

class SampleConfig( sample: NodeSeq, chromLength: Int) {
  var REG_LENGTH_AVG: Long   = (sample \\ "reg_length_avg").text.trim.toInt
  var REG_LENGTH_STD: Float  = (sample \\ "reg_length_std").text.trim.toFloat
  var DIST_AVG: Long         = (sample \\ "dist_avg").text.trim.toInt
  var DIST_STD: Float        = (sample \\ "dist_std").text.trim.toFloat
  var NUM_REGIONS_MAX: Long  = (sample \\ "num_reg_max").text.trim.toInt
  var MIN_VAL: Float         = (sample \\ "min_val").text.trim.toFloat
  var MAX_VAL: Float         = (sample \\ "max_val").text.trim.toFloat
  var NUM_META: Long         = (sample \\ "num_meta").text.trim.toInt
  var CHROM_LENGTH: Long     = chromLength
}

class DatasetConfig( dataset:NodeSeq, name:String ) {

  val CHROM_LENGTH = (dataset \\ "chrom_length").text.trim.toInt
  val NAME         = name

  private val samples_nodes = (dataset \\ "samples" \\"sample")
  val samples:Array[SampleConfig] =  new Array[SampleConfig](samples_nodes.size)

  for( sampleN <- 0 to samples_nodes.size - 1 ) {
    samples(sampleN) = new SampleConfig(samples_nodes(sampleN), CHROM_LENGTH)
  }

}

class EnvConfig( environment: NodeSeq ) {
  val TYPE                = (environment  \\ "type").text.trim.capitalize
  val ROOT_LOCAL          = (environment  \\ "root_folder_local").text.trim
  //  val USERNAME          = (environment  \\ "username").text.trim
  //  val TEST_PATH         = (environment  \\ "test_folder").text.trim
  //  val RESULTS_PATH      = (environment  \\ "results_folder").text.trim
  val OUT_FOLDER_NAME     = (environment  \\ "out_folder_name").text.trim
  val DATA_FOLDER_NAME    = (environment  \\ "data_folder_name").text.trim
  val LOGS_FOLDER_NAME    = (environment  \\ "log_folder_name").text.trim
  val TEST_FOLDER_NAME    = (environment  \\ "test_folder_name").text.trim
  val RESULTS_FOLDER_NAME = (environment  \\ "results_folder_name").text.trim
}


object ConfigParser {

  def getDatasetConfig(xmlPath: String, datasetName: String): DatasetConfig = {

    val xml = XML.loadFile(xmlPath)
    val dataset:NodeSeq = (xml \\ "datasets" \\ "_" filter attributeValueEquals(datasetName))

    return new DatasetConfig(dataset, datasetName)

    //    val placeholder = (dataset \\ "placeholder").text //used to refer to the dataset within the query
    //    val dstype = (dataset \\ "type").text
    //    val names = dstype match {
    //      case "RNASEQ" => (dataset \\ "namevalues" \\ "value").map(n => n.text)
    //      case "BEDSCORE" => List.empty
    //
    //
    //        return null
    //
    //    }

  }

  def getBinningConfig(xmlPath:String): BinningConfig = {

    val xml = XML.loadFile(xmlPath)
    val binning = ( xml \\ "binning")

    return new BinningConfig(binning)

  }


  def getQueriesConfigs(xmlPath:String): Array[QueryConfig] = {

    val xml = XML.loadFile(xmlPath)
    val queries:NodeSeq = (xml \\ "queries" \\ "query")
    val queriesConfigs: Array[QueryConfig] = new Array[QueryConfig](queries.size)

    for( queryN <- 0 to queries.size - 1){
      queriesConfigs(queryN) = new QueryConfig(queries(queryN))
    }

    return queriesConfigs
  }

  def getEnvironmentConfig(xmlPath:String) : EnvConfig = {
    val xml = XML.loadFile(xmlPath)
    val env:NodeSeq = (xml \\ "environment")
    return new EnvConfig(env)
  }


  private def attributeValueEquals(value: String)(node: Node) = {
    node.attributes.exists(_.value.text == value)
  }

  def main(args: Array[String]): Unit = {
    val xmlPath = "/Users/andreagulino/Development/GMQL/conf/test_join1.xml"

    val ds = ConfigParser.getDatasetConfig(xmlPath, "REF")
    val qr = ConfigParser.getQueriesConfigs(xmlPath)
    val bn = ConfigParser.getBinningConfig(xmlPath)

    println("CHROM_LENGTH: "+ds.CHROM_LENGTH)

    println("\n")

    for ( sample:SampleConfig <- ds.samples) {
      println("# SAMPLE #")
      println("REG_LENGTH_AVG: "+sample.REG_LENGTH_AVG)
      println("REG_LENGTH_STD: "+sample.REG_LENGTH_STD)
      println("DIST_AVG: "+sample.DIST_AVG)
      println("DIST_STD: "+sample.DIST_STD)
      println("NUM_REGIONS_MAX: "+sample.NUM_REGIONS_MAX)
      println("MIN_VAL: "+sample.MIN_VAL)
      println("MAX_VAL: "+sample.MIN_VAL)
      println("NUM_META: "+sample.NUM_META)
    }

    println("\n")

    for( queryConfig <- qr) {
      println("DLE: "+queryConfig.DLE)
      println("DGE: "+queryConfig.DGE)
    }

    println("\n")

    val env = ConfigParser.getEnvironmentConfig(xmlPath)
    println("Environment Type: "+env.TYPE)

    println("\nBin Sizes to test:")

    for( binSize <- bn.binSizes){
      println(binSize)
    }

  }

}