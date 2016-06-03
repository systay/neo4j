package org.neo4j.cypher.internal.compiler.v3_0.commands.predicates

import org.neo4j.graphdb.{Node, Path, Relationship}

class Equivalent(val eagerizedValue: Any, val original: Any) {

  override def equals(in: Any): Boolean = {
    val eagerOther = in match {
      case s: Equivalent => s.eagerizedValue
      case x => Equivalent.eager(x)
    }

    (eagerizedValue, eagerOther) match {
      case (null, null) => true
      //      case (n1: Number, n2: Number) => n1 == n2
      //        n1.doubleValue() == n2.doubleValue() ||
      //        (Math.rint(n1.doubleValue()).toLong == n2.longValue() &&
      //          Math.rint(n2.doubleValue()).toLong == n1.longValue())
      case (a, b) => a == b
      case _ => false
    }
  }


  private val hash: Int = hashCode(eagerizedValue)

  override def hashCode(): Int = hash

  private val EMPTY_LIST = 42

  private def hashCode(o: Any): Int = o match {
    case null => 0
    case n: Number => n.intValue()
    case n: Vector[_] =>
      val length = n.length
      if (length > 0)
        length * (hashCode(n.head) + hashCode(n.last))
      else
        EMPTY_LIST
    case x => x.hashCode()
  }
}

object Equivalent {
  def tryBuild(x: Any): Option[Equivalent] = try {
    Some(new Equivalent(eager(x), x))
  } catch {
    case _: ControlFlowException => None
  }

  private def eager(v: Any): Any = v match {
    case x: Number => x
    case a: String => a
    case n: Node => n
    case n: Relationship => n
    case p: Path => p
    case b: Boolean => b
    case null => null
    case a: Char => a.toString

    case a: Array[_] => a.toVector.map(eager)
    case a: TraversableOnce[_] => a.toVector.map(eager)
    case _ => throw new ControlFlowException
  }

}

class ControlFlowException extends RuntimeException
