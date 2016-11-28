/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis

import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.semantic.analysis.Scope.ScopingContext

/*
This phase is concerned with two things - declaring variables and creating scopes that said variables exist in

The Scope keeps information about the scope tree, and the ScopingContext is used by the Variable objects to know
if they should declare or reference existing variables.
 */
object Scoping extends Phase[(Scope, ScopingContext)] {

  override def initialValue: (Scope, ScopingContext) = (Scope.empty, ScopingContext.Default)

  override protected def before(node: ASTNode, environment: (Scope, ScopingContext)): (Scope, ScopingContext) = {
    println(node)
    val (currentScope, scopingContext) = environment

    node match {
      case _: SingleQuery =>
        (currentScope.enterScope(), scopingContext)

      case foreach: Foreach =>
        visit(foreach.expression, (currentScope, ScopingContext.Expression))

        val foreachScope = currentScope.enterScope()
        visit(foreach.variable, (foreachScope, ScopingContext.Expression))
        (foreachScope, scopingContext)

      case _: Match =>
        (currentScope, ScopingContext.Match)

      case _: Return =>
        (currentScope, ScopingContext.Expression)

      case _ =>
        environment
    }
  }

  override protected def after(node: ASTNode, env: (Scope, ScopingContext)): (Scope, ScopingContext) = {
    node.myScope.value = env
    env
  }
}
