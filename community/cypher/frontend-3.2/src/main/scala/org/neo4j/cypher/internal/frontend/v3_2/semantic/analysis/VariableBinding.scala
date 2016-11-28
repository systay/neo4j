package org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_2.InvalidSemanticsException
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis.Scope.ScopingContext

object VariableBinding extends Phase[Unit] {
  override def initialValue: Unit = {}

  override protected def before(node: ASTNode, environment: Unit): Unit = {

    val (scope, scopeContext) = node.myScope.value

    node match {
      case foreach: Foreach =>
        foreach.variable.binding.value = Declaration
        scope.add(foreach.variable)
        visit(foreach.expression, environment)

      case exp: Variable if scopeContext == ScopingContext.Match =>
        scope.implicitDeclare(exp)

      case exp: Variable if scopeContext == ScopingContext.Expression || scopeContext == ScopingContext.Default =>
        val variable = scope.variableDefined(exp).getOrElse(
          throw new InvalidSemanticsException(s"Variable `${exp.name}` not declared, at ${exp.position}")
        )
        exp.binding.value = Bound(variable)

      case _ =>
        environment
    }
  }

  override protected def after(node: ASTNode, env: Unit): Unit = {}
}

sealed trait Binding

case object Declaration extends Binding
case class Bound(to: Variable) extends Binding