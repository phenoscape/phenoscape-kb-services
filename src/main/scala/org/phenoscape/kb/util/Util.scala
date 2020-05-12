package org.phenoscape.kb.util

object Util {

  implicit class TraversableOps[A](self: Traversable[A]) {

    def groupMap[K, B](key: A => K)(f: A => B): Map[K, Set[B]] =
      self.groupBy(key).map { case (k, vs) => k -> vs.map(f).toSet }

  }

  implicit class MapOps[K, V](self: Map[K, V]) {

    /**
      * A strict version of mapValues (which in the collection API is lazy).
      *
      * @param f the function used to transform values of this map
      * @return a map which maps every key of this map to f(this(key))
      */
    def mapVals[W](f: V ⇒ W): Map[K, W] = self.map { case (k, v) => k -> f(v) }

  }

}
