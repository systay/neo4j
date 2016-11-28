package org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_2.InvalidSemanticsException
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis.Scope.ScopingContext
import org.neo4j.cypher.internal.frontend.v3_2.symbols.{TypeSpec, _}

object Typing extends Phase[Unit] {
  override def initialValue: Unit = {}

  override protected def before(node: ASTNode, environment: Unit): Unit = {}

  override protected def after(node: ASTNode, env: Unit): Unit = {

    val (scope, scopeContext) = node.myScope.value

    node match {
      case e: Expression if e.myType.hasValue =>
      // This expression is already typed. No need to do any more work

      case NodePattern(Some(variable: Variable), _, _) if scopeContext == ScopingContext.Match =>
        variable.binding.value match {
          case Declaration => variable.myType.value = CTNode.invariant
          case Bound(other) => variable.myType.value = checkTypes(other, CTNode.invariant)
        }

      case RelationshipPattern(Some(variable: Variable), _, _, _, _, _) if scopeContext == ScopingContext.Match =>
        variable.binding.value match {
          case Declaration => variable.myType.value = CTRelationship.invariant
          case Bound(other) => variable.myType.value = checkTypes(other, CTRelationship.invariant)
        }

      case e: SignedDecimalIntegerLiteral =>
        e.myType.value = CTInteger.covariant

      case e@Add(lhs, rhs) =>
        val myType = (lhs.myType.value, rhs.myType.value) match {
          case (CTInteger.covariant, CTInteger.covariant) => CTInteger.covariant
        }
        e.myType.value = myType

      case NodePattern(Some(variable), _, _) =>
        variable.myType.value = CTNode.invariant

      case _ =>
    }
  }

  private def checkTypes(v: Variable, expected: TypeSpec): TypeSpec = {
    val otherType = v.myType.value
    if (otherType != expected)
      throw new InvalidSemanticsException(s"Type mismatch: ${v.name} already defined with conflicting type $otherType (expected $expected)")
    otherType
  }
}
