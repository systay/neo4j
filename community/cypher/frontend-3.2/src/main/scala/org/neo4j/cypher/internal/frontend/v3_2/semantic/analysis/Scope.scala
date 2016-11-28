package org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_2.InvalidSemanticsException
import org.neo4j.cypher.internal.frontend.v3_2.ast.Variable

import scala.collection.mutable.ListBuffer

class Scope(val outer: Scope) {
  private val locals = new ListBuffer[Variable]

  def add(l: Variable): Binding = {
    if (locals.contains(l)) throw new InvalidSemanticsException(s"Variable ${l.name} already declared in this scope")
    locals += l
    Declaration
  }

  def enterScope() = new Scope(this)

  def exitScope(): Scope = {
    assert(outer != null, "Tried to exit the outermost scope")
    outer
  }

  def implicitDeclare(v: Variable) = {
    variableDefined(v) match {
      case None =>
        locals += v
        v.binding.value = Declaration

      case Some(other) =>
        v.binding.value = Bound(other)
    }
  }

  def variableDefined(v: Variable): Option[Variable] =
    (locals.find(_ == v), outer) match {
      case (e: Some[Variable], _) =>
        e
      case (None, null) =>
        None

      case _ =>
        outer.variableDefined(v)
    }

  // Equality and toString
  override def toString: String = locals.map(_.name).mkString(outer.toString + "[", ", ", "]")

  def canEqual(other: Any): Boolean = other.isInstanceOf[Scope]

  override def equals(other: Any): Boolean = other match {
    case that: Scope =>
      (that canEqual this) &&
        locals == that.locals &&
        outer == that.outer
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(locals, outer)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Scope {
  def empty = new Scope(null) {
    override def toString = "[]"
  }

  sealed trait ScopingContext

  object ScopingContext {

    case object Default extends ScopingContext

    case object Match extends ScopingContext

    case object Merge extends ScopingContext

    case object Create extends ScopingContext

    case object CreateUnique extends ScopingContext

    case object Expression extends ScopingContext

  }

}
