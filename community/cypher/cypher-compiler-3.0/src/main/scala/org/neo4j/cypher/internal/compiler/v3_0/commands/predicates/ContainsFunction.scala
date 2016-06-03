package org.neo4j.cypher.internal.compiler.v3_0.commands.predicates

import org.neo4j.graphdb.{Node, Path, Relationship}

import scala.collection.mutable

/**
  * This is a state machine that handles checking for existence while materializing a list
  */
trait ContainsFunction {
  def contains(value: Any): (Option[Boolean], ContainsFunction)
}

class BuildUp(iterator: Iterator[Any], cachedSet: mutable.Set[Equivalent] = new mutable.HashSet[Equivalent])
  extends ContainsFunction {

  private var falseResult: Option[Boolean] = Some(false)

  override def contains(value: Any): (Option[Boolean], ContainsFunction) = {

    if (iterator.isEmpty && cachedSet.isEmpty)
      return (Some(false), AlwaysNotIn)

    if (value == null)
      return (None, this)

    Equivalent.tryBuild(value) match {
      case None =>
        val nextChecker = eagerlyLoadInput()
        nextChecker.contains(value)

      case Some(equivalent) =>
        if (cachedSet.contains(equivalent))
          (Some(true), this)
        else
          checkAndBuildUpCache(value)
    }
  }

  private def checkAndBuildUpCache(value: Any): (Option[Boolean], ContainsFunction) = {
    while (iterator.nonEmpty) {
      val nextValue = iterator.next()

      if (nextValue == null) {
        falseResult = None
      } else {
        Equivalent.tryBuild(nextValue) match {
          case None =>
            val fallBack = new SlowChecker(cachedSet.toVector.map(_.original) ++ iterator.toVector, falseResult)
            return fallBack.contains(value)
          case Some(next) =>
            cachedSet.add(next)
            val result = next == value
            if (result) return (Some(true), this)
        }
      }
    }

    (falseResult, new FastChecker(cachedSet, falseResult))
  }

  private def eagerlyLoadInput(): ContainsFunction = {
    while (iterator.nonEmpty) {
      val nextValue = iterator.next()

      if (nextValue == null) {
        falseResult = None
      } else {
        Equivalent.tryBuild(nextValue) match {
          case None =>
            val input = cachedSet.toVector.map(_.original) ++ iterator.toVector
            return new SlowChecker(input, falseResult)
          case Some(next) =>
            cachedSet.add(next)
        }
      }
    }

    new FastChecker(cachedSet, falseResult)
  }



  }

case object AlwaysNotIn extends ContainsFunction {
  override def contains(value: Any): (Option[Boolean], ContainsFunction) = (Some(false), this)
}

class SlowChecker(vector: Vector[Any], falseResult: Option[Boolean]) extends ContainsFunction {
  override def contains(value: Any): (Option[Boolean], ContainsFunction) =
    if (value == null) (None, this)
    else {
      val exists = vector.exists(x => Equals.areEqual(value, x)(null).get) // here we know that value and x are not null
      if (exists) (Some(true), this) else (falseResult, this)
    }

}

class FastChecker(cachedSet: mutable.Set[Equivalent], falseResult: Option[Boolean]) extends ContainsFunction {

  assert(cachedSet.nonEmpty, "If this is empty, we should use a AlwaysNotIn instead")

  override def contains(value: Any): (Option[Boolean], ContainsFunction) = {
    if (value == null)
      return (None, this)

    Equivalent.tryBuild(value) match {
      case Some(sameOther) =>
        val exists = cachedSet.contains(sameOther)
        val result = if (exists) Some(true) else falseResult
        (result, this)
      case None =>
        (falseResult, this)
    }
  }
}
