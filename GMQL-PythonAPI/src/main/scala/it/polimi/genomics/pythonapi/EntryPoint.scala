package it.polimi.genomics.pythonapi

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import py4j.GatewayServer
/**
  * Created by Luca Nanni on 08/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * Main of the application. It instantiates a gateway server for Python
  * to access to the JVM (through py4j)
  * */
object EntryPoint {

  val logger  = LoggerFactory.getLogger(this.getClass)
  val properties = AppProperties
  def main(args: Array[String]): Unit = {

    /*
    * Setting up the Spark context
    * */
    val conf = new SparkConf()
      .setAppName(properties.applicationName)
      .setMaster(properties.master)
      .set("spark.serializer", properties.serializer)

    val sc = new SparkContext(conf)
    this.logger.info("Spark context initiated")

    val pythonManager = PythonManager
    pythonManager.setSparkContext(sc=sc)

    /*Synchronization with the python process*/
    val pw = new PrintWriter(new File("sync.txt"))
    pw.write("Spark context started")
    pw.close()

    val gatewayServer : GatewayServer = new GatewayServer()
    gatewayServer.start()
    this.logger.info("GatewayServer started")
  }
}