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
package org.neo4j.cypher.internal.frontend.v3_1.ast

import org.neo4j.cypher.internal.frontend.v3_1.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_1.{InputPosition, SemanticError, _}

object FunctionInvocation {
  def apply(name: FunctionName, argument: Expression)(position: InputPosition): FunctionInvocation = {
    val f1 = FunctionInvocation(name, distinct = false, IndexedSeq(argument))
    f1.position.update(position)
    f1
  }
  def apply(left: Expression, name: FunctionName, right: Expression): FunctionInvocation = {
    val f1 = FunctionInvocation(name, distinct = false, IndexedSeq(left, right))
    f1.position.update(name.position())
    f1
  }

  def apply(expression: Expression, name: FunctionName): FunctionInvocation ={
    val f1 = FunctionInvocation(name, distinct = false, IndexedSeq(expression))
    f1.position.update(name.position())
    f1
  }
}

case class FunctionInvocation(functionName: FunctionName, distinct: Boolean, args: IndexedSeq[Expression])
                              extends Expression {
  val name = functionName.name
  val function: Option[Function] = Function.lookup.get(name.toLowerCase)

  def semanticCheck(ctx: SemanticContext) = function match {
    case None    => SemanticError(s"Unknown function '$name'", position)
    case Some(f) => f.semanticCheckHook(ctx, this)
  }
}

case class FunctionName(name: String) extends SymbolicName {
  override def equals(x: Any): Boolean = x match {
    case FunctionName(other) => other.toLowerCase == name.toLowerCase
    case _ => false
  }
  override def hashCode = name.toLowerCase.hashCode
}
