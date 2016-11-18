package it.polimi.genomics.binning.tooling

import java.util.Random

/**
  * Created by andreagulino on 18/11/16.
  */
object DatasetGenerator {


  def generateRegionData(config: SampleConfig):  List[String] = {

    var regions:List[String] = List.empty

    var stop = false
    var curRight:Long = 0
    var numRegions = 0

    // until you do not exceed the max number of regions or the region exceeds the chromosome
    while (!stop && numRegions < config.NUM_REGIONS_MAX) {

      val distance = this.round(this.normal(config.DIST_AVG, config.DIST_STD))
      val width    = this.round(this.normal(config.REG_LENGTH_AVG, config.REG_LENGTH_STD))
      val curLeft = curRight + distance
      curRight = curLeft + width

      if (curLeft >= config.CHROM_LENGTH || curRight >= config.CHROM_LENGTH) {
        stop = true
      } else {
        // assumption: all positive strand, all chromosome 1
        val row:String = "chr1\t"+curLeft+"\t"+curRight
        regions = regions ++ List(row)
        numRegions += 1
      }

    }

    return regions
  }

  def generateMetaData(config: SampleConfig, sampleNum:Int): List[String] =  {
    return List("antibody\tCTCF") ++ List("File_Number\t" + sampleNum)
  }

  def addScoreValue( coords: List[String] , config:SampleConfig): List[String] = {

    val min = config.MIN_VAL
    val max = config.MAX_VAL

    return coords.map {x => x+ "\t" + (scala.util.Random.nextDouble()*max + min)}
  }

  private def normal(mean: Long, std: Double): Double = {
    val  r = new Random()
    r.nextGaussian()*std+mean
  }

  private def round(n:Double): Long = {
    math.round(n)
  }

}
