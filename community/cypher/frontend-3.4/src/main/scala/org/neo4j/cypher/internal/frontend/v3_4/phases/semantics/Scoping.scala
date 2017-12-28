/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.phases.semantics

import org.neo4j.cypher.internal.frontend.v3_4.ast.{Clause, Foreach, Statement, With}
import org.neo4j.cypher.internal.util.v3_4.ASTNode
import org.neo4j.cypher.internal.util.v3_4.attribution.Attribute
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, ScopeExpression, Variable}

import scala.annotation.tailrec

/**
  * Scopes in Cypher are a little different from scopes in a language such as Java or Scala.
  *
  * In Java, you have scopes that can contain other scopes inside of them. These scopes exist in Cypher
  * as well, but then you also have weird behaviours around WITH. These are modeled by WITH reading from one scope
  * and writing to another.
  *
  * Clauses can read from one and write to another scope, but expressions only read from scopes
  */
object Scoping {
  def doIt(statement: Statement): (ReadScope, WriteScope) = {
    val readScope = new ReadScope
    val writeScope = new WriteScope

    def visitChildrenWith(e: ASTNode, s: Scope): Seq[(Any, Scope)] = e.children.toSeq.map(x => (x, s))

    val initScope = new Scope()
    statement.treeAccumulateTopDown(initScope) {
      case (w: With, scope) =>
        readScope.set(w.secretId, scope)
        val newScope = new Scope()
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

class Scope(parent: Option[Scope] = None, var locals: Set[Variable] = Set.empty) {
  // Just here to make debugging easier
  val myId: Int = {
    val x = Scope.id
    Scope.id += 1
    x
  }

  override def toString = s"Scope($myId parent: $parent)"

  def createInnerScope(): Scope = {
    new Scope(Some(this))
  }

  @tailrec
  final def getVariable(name: String): Option[Variable] = {
    val local: Option[Variable] = locals.collectFirst {
      case v if v.name == name => v
    }

    (local, parent) match {
      case (x: Some[_], _) => x
      case (None, None) => None
      case (None, Some(p)) => p.getVariable(name)
    }
  }
}

class ReadScope extends Attribute[Scope]

class WriteScope extends Attribute[Scope]