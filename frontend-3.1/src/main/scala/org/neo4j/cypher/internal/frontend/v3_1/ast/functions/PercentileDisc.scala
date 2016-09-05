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
package org.neo4j.cypher.internal.frontend.v3_1.ast.functions

import org.neo4j.cypher.internal.frontend.v3_1.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_1.ast.{AggregatingFunction, SimpleTypedFunction, UserFunctionInvocation}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._

case object PercentileDisc extends AggregatingFunction with SimpleTypedFunction {
  def name = "percentileDisc"

  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTInteger),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def semanticCheck(ctx: SemanticContext, invocation: UserFunctionInvocation) =
    super.semanticCheck(ctx, invocation) ifOkChain  checkPercentileRange(invocation.args(1))
}
