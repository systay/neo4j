package org.neo4j.cypher.internal.frontend.v3_4.phases.semantics

import org.neo4j.cypher.internal.frontend.v3_4.ast.{Clause, Foreach, Statement, With}
import org.neo4j.cypher.internal.util.v3_4.ASTNode
import org.neo4j.cypher.internal.util.v3_4.attribution.Attribute
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, ScopeExpression}

object Scoping {
  def doIt(statement: Statement): (ReadScope, WriteScope) = {
    val readScope = new ReadScope
    val writeScope = new WriteScope

    def visitChildrenWith(e: ASTNode, s: Scope): Seq[(Any, Scope)] = e.children.toSeq.map(x => (x, s))

    val initScope = new Scope()
    statement.treeFold3(initScope) {
      case (w: With, scope) =>
        readScope.set(w.secretId, scope)
        val newScope = new Scope
        writeScope.set(w.secretId, newScope)
        visitChildrenWith(w, newScope)

      case (a: Foreach, scope) =>
        readScope.set(a.secretId, scope)
        readScope.set(a.expression.secretId, scope)
        val newScope = scope.createInnerScope()
        writeScope.set(a.secretId, newScope)
        // Children will get different scopes
        Seq(
          (a.variable, newScope),
          (a.expression, scope),
          (a.updates, newScope)
        )

      case (a: ScopeExpression, scope) =>
        val newScope = scope.createInnerScope()
        writeScope.set(a.secretId, newScope)
        visitChildrenWith(a, newScope)

      case (e: Expression, scope) =>
        readScope.set(e.secretId, scope)
        visitChildrenWith(e, scope)

      case (a: Clause, scope) =>
        readScope.set(a.secretId, scope)
        writeScope.set(a.secretId, scope)
        visitChildrenWith(a, scope)
    }

    (readScope, writeScope)
  }
}

object Scope {
  var id = 0
}

class Scope(var innerScopes: Seq[Scope] = Seq.empty) {
  val myId = {
    val x = Scope.id
    Scope.id += 1
    x
  }

  override def toString = s"Scope($myId ${innerScopes.mkString(", ")})"

  def createInnerScope(): Scope = {
    val newScope = new Scope()
    innerScopes = innerScopes :+ newScope
    newScope
  }
}

class ReadScope extends Attribute[Scope]

class WriteScope extends Attribute[Scope]