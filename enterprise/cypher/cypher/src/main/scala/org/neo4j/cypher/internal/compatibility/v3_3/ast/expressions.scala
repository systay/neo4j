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
package org.neo4j.cypher.internal.compatibility.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.ast.{PropertyKeyName, RelTypeName, Expression => ASTExpression}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticDirection}

case class NodeProperty(offset: Int, propertyKeyName: PropertyKeyName)(val position: InputPosition)
  extends ASTExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class RelationshipProperty(offset: Int, propertyKeyName: PropertyKeyName)(val position: InputPosition)
  extends ASTExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class IdFromRegister(offset: Int)(val position: InputPosition) extends ASTExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class NodeFromRegister(offset: Int)(val position: InputPosition) extends ASTExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}

case class GetDegree(offset: Int, relType: Option[RelTypeName], dir: SemanticDirection)(val position: InputPosition)
  extends ASTExpression {
  override def semanticCheck(ctx: ASTExpression.SemanticContext): SemanticCheck = SemanticCheckResult.success
}

