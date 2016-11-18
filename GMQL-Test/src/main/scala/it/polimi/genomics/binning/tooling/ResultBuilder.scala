package it.polimi.genomics.binning.tooling

import java.util.Calendar

import scala.xml.{Elem, Node, XML}

/**
  * Created by andreagulino on 14/11/16.
  */

object ResultBuilder {

  def buildResult(outputQueries: Seq[(String, Seq[(String, Long)])], xmlPath: String): Unit = {

    val xml = XML.loadFile(xmlPath)

    println("\n\n\n### Storing Results\n\n")

    var tests = <tests></tests>


    for ( q <- outputQueries ) {

      var curQuery =
        <query description={q._1}>
        </query>

      for (o <- q._2) {

        // x : numBins, y: time
        val curTest =
        <test>
          <x>
            {o._1.toLong}
          </x>
          <y>
            {o._2.toLong}
          </y>
        </test>

        curQuery = addChild(curQuery, curTest)

      }

      tests =  addChild(tests, curQuery)

    } // end for over queries

    val results = addChild(xml, tests)

    val now = Calendar.getInstance().getTime()
    scala.xml.XML.save(FSTools.resultsPath+now+".xml", results)


  }



  def addChild(n: Node, newChild: Node) = n match {
    case Elem(prefix, label, attribs, scope, child @ _*) =>
      Elem(prefix, label, attribs, scope, child ++ newChild : _*)
    case _ => error("Can only add children to elements!")

  }

}
