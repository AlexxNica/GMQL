package it.polimi.genomics.binning.tooling

/**
  * Created by andreagulino on 25/10/16.
  */

object QueryBuilder {

  def createJoin (config:QueryConfig, refPath: String,
                  expPath: String, outPath: String): String = {
    val dle = config.DLE
    val dge = config.DGE

    val query = "A = SELECT (parser:BedScoreParser) "+refPath+";"+
                "S = SELECT(parser:BedScoreParser) "+expPath+";"+
                "J = JOIN(distance < "+dle+
                    (if(dge.isDefined) ",distance > "+dge.get else "") +
                                         ";output:left) A S;"+
                "MATERIALIZE J into "+outPath+";"

    return query
  }



  def get (queryConfig: QueryConfig, leftPath: String, rightPath: String, outPath: String): Option[String] = {

    val qtype = "JOIN"

    qtype match {
      case "JOIN" => return Some(createJoin(queryConfig, leftPath, rightPath, outPath))
    }

    return None

  }

  def main(args: Array[String]): Unit = {

    //print(r.get)
  }
}
