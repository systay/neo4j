/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_0.ast

import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression.{SemanticContext, _}
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.cypher.internal.frontend.v3_0.{InputPosition, _}

case class TreeProjection(name: Identifier, items: Seq[TreeProjectionElement])(val position: InputPosition)
  extends Expression with SimpleTyping {
  protected def possibleTypes = CTMap

  override def semanticCheck(ctx: SemanticContext) =
    items.semanticCheck(ctx) chain
      name.ensureDefined() chain
      super.semanticCheck(ctx)
}

sealed trait TreeProjectionElement extends SemanticCheckableWithContext

case class KeyValuePair(key: PropertyKeyName, exp: Expression)(val position: InputPosition) extends TreeProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = exp.semanticCheck(ctx)
}

case class VariableElement(id: Identifier)(val position: InputPosition) extends TreeProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = id.semanticCheck(ctx)
}

case class DotElement(id: Identifier)(val position: InputPosition) extends TreeProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
}