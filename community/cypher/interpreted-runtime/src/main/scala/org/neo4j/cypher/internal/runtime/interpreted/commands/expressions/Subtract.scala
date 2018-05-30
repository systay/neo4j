/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherMath
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue

case class Subtract(a: Expression, b: Expression) extends Arithmetics(a, b) {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    CypherMath.subtract(a(ctx, state), b(ctx, state))

  def calc(a: NumberValue, b: NumberValue) = a.minus(b)

  def rewrite(f: (Expression) => Expression) = f(Subtract(a.rewrite(f), b.rewrite(f)))

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}
