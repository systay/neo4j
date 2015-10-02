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
package org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.{Rewriter, topDown}

case object desugarTreeProjection extends Rewriter {
  def apply(that: AnyRef): AnyRef = topDown(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case e@TreeProjection(id, items) =>
      val mapExpressionItems = items.map {
        case KeyValuePair(k, v) => k -> v

        case DotElement(property@Identifier(name)) =>
          val k = PropertyKeyName(name)(property.position)
          k -> Property(id, k)(property.position)

        case VariableElement(id@Identifier(name)) =>
          PropertyKeyName(name)(id.position) -> id

      }

      MapExpression(mapExpressionItems)(e.position)
  }
}
