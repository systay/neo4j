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

import org.neo4j.cypher.internal.frontend.v3_4.ast.{Foreach, Statement}
import org.neo4j.cypher.internal.util.v3_4.{ASTNode, InternalException}
import org.neo4j.cypher.internal.util.v3_4.attribution.{Attribute, Id}
import org.neo4j.cypher.internal.v3_4.expressions.Variable

/**
  * After this phase, every Variable instance in the AST tree will be annotated with information about
  * whether a variable is declared or bound to something earlier
  */
object VariableBinding {
  def doIt(statement: Statement, readScope: ReadScope, writeScope: WriteScope): VariableBinding = {
    val bindings = new VariableBinding

    statement.treeVisitTopDown {
      case v: Variable if !bindings.contains(v.secretId) =>
        val scope = readScope.get(v.secretId)
        val variableDeclaration: Option[Variable] = scope.getVariable(v.name)
        variableDeclaration match {
          case None =>
            bindings.set(v.secretId, Declaration(v.secretId))
            scope.locals += v
          case Some(other) =>
            bindings.set(v.secretId, Resolution(other.secretId))
        }

      case f: Foreach =>
        val scope = writeScope.get(f.secretId)
        bindings.set(f.variable.secretId, Declaration(f.variable.secretId))
        scope.locals += f.variable

    }

    bindings
  }
}

sealed trait VariableUse
case class Resolution(id: Id) extends VariableUse
case class Declaration(id: Id) extends VariableUse

class VariableBinding extends Attribute[VariableUse]

// This is the Attribute[VariableUse] in a form that is easy to consume by the type algorithm
class Bindings(ast: ASTNode, val variableBindings: VariableBinding) {
  def declarationOf(v: Variable): Option[Variable] = {
    val id =
      variableBindings.get(v.secretId) match {
      case _:Declaration => throw new InternalException("this is a declaration")
      case Resolution(x) => x
    }

    ast.treeFind[Variable] {
      case x: Variable => x.secretId == id
    }
  }
}