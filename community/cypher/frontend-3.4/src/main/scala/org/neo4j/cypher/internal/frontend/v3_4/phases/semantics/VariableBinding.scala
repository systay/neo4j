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
            bindings.set(v.secretId, Declared(v.secretId))
            scope.locals += v
          case Some(other) =>
            bindings.set(v.secretId, Referenced(other.secretId))
        }

      case f: Foreach =>
        val scope = writeScope.get(f.secretId)
        bindings.set(f.variable.secretId, Declared(f.variable.secretId))
        scope.locals += f.variable

    }

    bindings
  }
}

sealed trait VariableUse

case class Referenced(id: Id) extends VariableUse

case class Declared(id: Id) extends VariableUse

class VariableBinding extends Attribute[VariableUse]