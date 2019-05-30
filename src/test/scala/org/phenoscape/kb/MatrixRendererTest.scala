package org.phenoscape.kb

object MatrixRendererTest extends App {

  val innerMap: Map[Any, Boolean] = Map("y1" -> true, "y2" -> false, "y3" -> true)

  val outerMap: Map[Any, Map[Any, Boolean]] = Map("y1" -> innerMap, "y2" -> innerMap, "y3" -> innerMap)

  val outputString = AnatomicalEntity.matrixRendererFromMapOfMaps(DependencyMatrix(outerMap))

  println(outputString)

}