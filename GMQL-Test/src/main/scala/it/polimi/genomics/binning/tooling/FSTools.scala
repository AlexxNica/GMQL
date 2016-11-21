package it.polimi.genomics.binning.tooling

import java.io.{File, PrintWriter}

import it.polimi.genomics.repository.util.Utilities
import org.apache.commons.io.FileUtils

/**
  * Created by andreagulino on 18/11/16.
  */
object FSTools {


  private var testPath_local:String    = null
  private var testPath_cluster:String  = null
  private var dataPath_local:String    = null
  private var dataPath_hdfs:String     = null
  private var outPath:String      = null
  private var logPath:String      = null
  var resultsPath:String  = null

  private var envConfig:EnvConfig = null

  private var utilities:Utilities = null
  private var HDFSRepoDir: String = null

  def init_file_system(envConfig: EnvConfig): Unit = {

    this.envConfig = envConfig

    testPath_local = envConfig.ROOT_LOCAL + envConfig.TEST_FOLDER_NAME +"/"
    println("Initializing  FS with local test directory: "+testPath_local)

    // Create a test folder on local fs (overwrite if exists) to store generated samples
    val dir : File = new File(testPath_local)
    if (dir.exists() && !dir.isDirectory) {
      dir.delete()
    }
    else if (dir.exists()) {
      FileUtils.deleteDirectory(dir)
    }
    dir.mkdir()

    // Create data folder inside local test folder
    dataPath_local = testPath_local+envConfig.DATA_FOLDER_NAME+"/"
    val out  : File  = new File(dataPath_local)
    out.mkdir()

    // Create the folder to store results (on local FS), do not overwrite if it exists
    resultsPath = envConfig.ROOT_LOCAL+envConfig.RESULTS_FOLDER_NAME+"/"
    val dir_res : File = new File(resultsPath)
    if (dir_res.exists() && !dir_res.isDirectory) {
      dir_res.delete()
    }
    else if (dir_res.exists() && dir_res.isDirectory) {
      println("Results folder already exists")
    } else {
      dir_res.mkdir()
    }

    // Create log folder inside results folder
    logPath        = resultsPath+envConfig.LOGS_FOLDER_NAME+"/"
    val log  : File  = new File(logPath)
    log.mkdir()

    // Check if LOCAL or CLUSTER
    if( envConfig.TYPE equals "CLUSTER" ) {

      utilities = new Utilities()

      HDFSRepoDir = utilities.HDFSRepoDir
      println("Using HDFS, root folder: " + HDFSRepoDir)

      testPath_cluster = envConfig.TEST_FOLDER_NAME+"/"
      outPath          = testPath_cluster+envConfig.OUT_FOLDER_NAME+"/"
      dataPath_hdfs    = testPath_cluster+envConfig.DATA_FOLDER_NAME+"/"

      // Delete any previously created test folder
      utilities.deleteDFSDir(testPath_cluster)

      // Create the required folder structure on HDFS
      utilities.createDirHDFS(testPath_cluster)
      utilities.createDirHDFS(testPath_cluster+envConfig.DATA_FOLDER_NAME)
      //utilities.createDirHDFS(testPath_cluster+envConfig.OUT_FOLDER_NAME)


    } else {

      outPath        = testPath_local+envConfig.OUT_FOLDER_NAME+"/"
      //val data : File  = new File(outPath)
      //data.mkdir()

    }


  }

  def createDataset(name:String): String = {

    (new File(dataPath_local+name)).mkdir()

    if( envConfig.TYPE == "CLUSTER" ) {

      println("Creating dataset in HDFS: "+getHDFSRootURI+dataPath_hdfs+name+"/")
      utilities.createDirHDFS(dataPath_hdfs+name+"/")
      return getHDFSRootURI+dataPath_hdfs+name+"/"
    } else {
      return dataPath_local+name+"/"
    }

  }

  private def getHDFSRootURI: String = {
    var pre  = utilities.gethdfsConfiguration().get("fs.defaultFS")
    val user = System.getenv("USER");

    if( (pre takeRight 1) != "/" ) {
       pre = pre + "/"
    }
    val uri = pre+"user/"+user+"/"
    return uri
  }

ls

  def clean_outputs():Unit = {

    if( envConfig.TYPE == "CLUSTER" ) {
      println("Deleting outpath")
      utilities.deleteDFSDir(outPath)
      //utilities.createDirHDFS(outPath)
    } else {
      val dir : File = new File(testPath_local+envConfig.OUT_FOLDER_NAME+"/")
      if (dir.exists() && !dir.isDirectory) {
        dir.delete()
      }
      else if (dir.exists()) {
        FileUtils.deleteDirectory(dir)
      }
    }

  }

  def getOutputPath : String  = {
    if( envConfig.TYPE equals "CLUSTER" ) {
      return getHDFSRootURI+outPath
    } else {
      return outPath
    }
  }

  def storeSample(dsName: String, sampleN: Int,  regions:List[String], meta:List[String]) : Unit = {
    val file_path = dataPath_local+dsName+"/file_"+sampleN+".bed"

    val file_region : File = new File(file_path)
    val file_meta   : File = new File(file_path + ".meta")

    val region_writer = new PrintWriter(file_region)
    val meta_writer = new PrintWriter(file_meta)

    regions.foreach(x=>region_writer.write(x+"\n"))
    meta.foreach(x=>meta_writer.write(x+"\n"))

    region_writer.close()
    meta_writer.close()

    // If execution set to cluster move to HDFS
    if( envConfig.TYPE == "CLUSTER" ) {
      println("Copying generated dataset "+dsName+" to: "+dataPath_hdfs+dsName)
      utilities.copyfiletoHDFS(file_path,           dataPath_hdfs+dsName+"/")
      utilities.copyfiletoHDFS(file_path + ".meta", dataPath_hdfs+dsName+"/")
    }

  }

  def cleanTestFolders : Unit = {
    if( envConfig.TYPE equals "CLUSTER") {
      println("Cleaning HDFS")
      utilities.deleteDFSDir(testPath_cluster)
    }

    println("Cleaning local fs test folder: "+testPath_local)
    val dir : File = new File(testPath_local)
    if (dir.exists() && !dir.isDirectory) {
      dir.delete()
    }
    else if (dir.exists()) {
      FileUtils.deleteDirectory(dir)
    }


  }

}
