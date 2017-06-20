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

import org.neo4j.cypher.internal.compatibility.v3_3.ast
import org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.{expressions => commandExpressions}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.RegisterAllocationFailed
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{CommunityExpressionConverters, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{ProjectedPath, Expression => CommandExpression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.{UnresolvedLabel, UnresolvedRelType}
import org.neo4j.cypher.internal.compiler.v3_3.ast.NestedPlanExpression
import org.neo4j.cypher.internal.compiler.v3_3.spi.TokenContext
import org.neo4j.cypher.internal.frontend.v3_3.ast.functions.Exists
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ASTExpression}
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticTable, ast => frontEndAst}

class RegisteredExpressionConverter(semanticTable: SemanticTable, resolver: TokenContext) extends ExpressionConverters {
  override def toCommandExpression(expression: ASTExpression, self: ExpressionConverters): CommandExpression = expression match {
    case ast.NodeProperty(offset, propertyKeyName) if propertyKeyName.id(semanticTable).nonEmpty =>
      commandExpressions.NodeProperty(offset, propertyKeyName.id(semanticTable))

    case ast.IdFromRegister(offset) =>
      commandExpressions.IdFromRegister(offset)

    case ast.NodeFromRegister(offset) =>
      commandExpressions.NodeFromRegister(offset)

    case ast.GetDegree(offset, relType, dir) =>
      val maybeToken = relType map { relType =>
        UnresolvedRelType(relType.name).resolve(resolver)
      }

      commandExpressions.GetDegree(offset, maybeToken, dir)

    case ast.HasLabels(offset, labelName) =>
      val labelToken = UnresolvedLabel(labelName.name).resolve(resolver)
      commandExpressions.HasLabels(offset, labelToken)

    case f: frontEndAst.FunctionInvocation if f.function == Exists =>
      throw new RegisterAllocationFailed(s"${f.function} not supported with register allocation yet")

    case _: frontEndAst.IterablePredicateExpression |
         _: frontEndAst.AndedPropertyInequalities |
         _: NestedPlanExpression
    => throw new RegisterAllocationFailed(s"$expression not supported with register allocation yet")

    case x => CommunityExpressionConverters.toCommandExpression(expression, self)
  }

  override def toCommandProjectedPath(e: frontEndAst.PathExpression): ProjectedPath =
    throw new RegisterAllocationFailed("Paths are not supported with register allocation yet")
}
