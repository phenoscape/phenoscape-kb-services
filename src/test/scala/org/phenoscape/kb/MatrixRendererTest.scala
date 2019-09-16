package org.phenoscape.kb

import utest._


object MatrixRendererTest extends TestSuite {

  val tests = Tests {

    val innerMap1: Map[Any, Boolean] = Map("femur" -> true, "hindlimb" -> false, "forelimb" -> false, "hand" -> false, "face" -> false, "eye" -> false) //("y3" -> true, "y2" -> false, "y1" -> true)
    val innerMap2: Map[Any, Boolean] = Map("femur" -> true, "hindlimb" -> true, "forelimb" -> false, "hand" -> false, "face" -> false, "eye" -> false)
    val innerMap3: Map[Any, Boolean] = Map("femur" -> false, "hindlimb" -> false, "forelimb" -> true, "hand" -> false, "face" -> false, "eye" -> false)
    val innerMap4: Map[Any, Boolean] = Map("femur" -> false, "hindlimb" -> false, "forelimb" -> true, "hand" -> true, "face" -> false, "eye" -> false)
    val innerMap5: Map[Any, Boolean] = Map("femur" -> false, "hindlimb" -> false, "forelimb" -> false, "hand" -> false, "face" -> true, "eye" -> true)
    val innerMap6: Map[Any, Boolean] = Map("femur" -> false, "hindlimb" -> false, "forelimb" -> false, "hand" -> false, "face" -> false, "eye" -> true)


    val outerMap: Map[Any, Map[Any, Boolean]] = Map("eye" -> innerMap6, "femur" -> innerMap1, "hindlimb" -> innerMap2, "forelimb" -> innerMap3, "hand" -> innerMap4, "face" -> innerMap5) //("y2" -> innerMap, "y3" -> innerMap, "y1" -> innerMap)

    'testDep - {
      val outputString = AnatomicalEntity.matrixRendererFromMapOfMaps(DependencyMatrix(outerMap, List("femur", "hindlimb", "forelimb", "hand", "face", "eye")))
      val expectedString = ",femur,hindlimb,forelimb,hand,face,eye\nfemur,1,0,0,0,0,0\nhindlimb,1,1,0,0,0,0\nforelimb,0,0,1,0,0,0\nhand,0,0,1,1,0,0\nface,0,0,0,0,1,1\neye,0,0,0,0,0,1\n"

      assert(outputString.contentEquals(expectedString))
    }
  }
}

