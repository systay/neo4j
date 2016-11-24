package org.neo4j.cypher.internal.frontend.v3_1.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_1.ast._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait Phase[ENVIRONMENT] {
  def enrich(node: ASTNode): Unit = enrich(node, initialValue)

  private def enrich(node: ASTNode, environment: ENVIRONMENT): ENVIRONMENT = {
    println(s"visited $node")

    val beforeEnv = before(node, environment)
    val afterChildren = node.myChildren.foldLeft(beforeEnv) {
      case (env, child) => enrich(child, env)
    }
    val afterEnvironment = after(node, afterChildren)

    remember(node, afterEnvironment)
    afterEnvironment
  }

  def initialValue: ENVIRONMENT

  protected def before(node: ASTNode, environment: ENVIRONMENT): ENVIRONMENT

  protected def after(node: ASTNode, environment: ENVIRONMENT): ENVIRONMENT

  protected def remember(node: ASTNode, environment: ENVIRONMENT)
}

object Scoping extends Phase[MyScope] {
  override def initialValue = MyScope.empty

  override protected def before(node: ASTNode, environment: MyScope) = node match {
    case _: SingleQuery =>
      environment.enterScope()
    case _ =>
      environment
  }

  override protected def after(node: ASTNode, environment: MyScope) = node match {
    case _: SingleQuery =>
      environment.exitScope()

    case _ =>
      node.myScope.value = environment
      environment
  }

  override protected def remember(node: ASTNode, environment: MyScope) =
    node.myScope.value = environment
}

class MyScope(val outer: MyScope) {
  val locals: ListBuffer[Variable] = new ListBuffer

  def add(l: Variable) = locals += l

  def varsInScope: mutable.Buffer[Variable] = outer.varsInScope.clone() ++= locals

  override def toString = locals.mkString(outer.toString + "[", ", ", "]")

  def canEqual(other: Any): Boolean = other.isInstanceOf[MyScope]

  override def equals(other: Any): Boolean = other match {
    case that: MyScope =>
      (that canEqual this) &&
        locals == that.locals &&
        outer == that.outer
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(locals, outer)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def enterScope() = new MyScope(this)

  def exitScope() = {
    assert(outer != null, "Tried to exit the outermost scope")
    outer
  }
}

object MyScope {
  def empty = new MyScope(null) {
    override def toString = "[]"

    override def varsInScope: mutable.Buffer[Variable] = new ListBuffer
  }
}
