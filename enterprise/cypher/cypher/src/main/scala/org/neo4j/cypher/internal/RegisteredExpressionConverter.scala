/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.{NodeProperty, expressions => commandExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.RegisterAllocationFailed
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverters, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{ProjectedPath, Expression => CommandExpression}
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticTable, ast}

class RegisteredExpressionConverter(semanticTable: SemanticTable) extends ExpressionConverters {
  override def toCommandExpression(expression: ast.Expression, self: ExpressionConverters): CommandExpression = expression match {
    case NodeProperty(offset, propertyKeyName) if propertyKeyName.id(semanticTable).nonEmpty =>
      commandExpressions.NodeProperty(offset, propertyKeyName.id(semanticTable))
    case x => CommunityExpressionConverters.toCommandExpression(expression, self)
  }

  override def toCommandProjectedPath(e: ast.PathExpression): ProjectedPath =
    throw new RegisterAllocationFailed("Paths are not supported with register allocation yet")
}
