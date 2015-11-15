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
import org.neo4j.cypher.internal.frontend.v3_0._

object inliningContextCreator extends (ast.Statement => InliningContext) {

  def apply(input: ast.Statement): InliningContext = {
    input.treeFold(InliningContext()) {
      // We cannot inline expressions in a DISTINCT with clause, projecting the result of the expression
      // would change the result of the distinctification
      case withClause: With if !withClause.distinct =>
        (context, children) =>
          children(context.enterQueryPart(aliasedReturnItems(withClause.returnItems.items)))

      // When just passing an variable through a WITH, do not count the variable as used. This case shortcuts the
      // tree folding so the variables are not tracked.
      case AliasedReturnItem(Variable(n1), alias@Variable(n2)) if n1 == n2 =>
        (context, children) =>
          context

      case variable: Variable =>
        (context, children) =>
          children(context.trackUsageOfVariable(variable))

      // When an variable is used in ORDER BY, it should never be inlined
      case sortItem: SortItem =>
        (context, children) =>
          children(context.spoilVariable(sortItem.expression.asInstanceOf[Variable]))

      // Do not inline pattern variables, unless they are clean aliases of previous variables
      case NodePattern(Some(variable), _, _) =>
        (context, children) =>
          if (context.isAliasedVarible(variable))
            children(context)
          else
            children(context.spoilVariable(variable))

      case RelationshipPattern(Some(variable), _, _, _, _, _) =>
        (context, children) =>
          if (context.isAliasedVarible(variable))
            children(context)
          else
            children(context.spoilVariable(variable))
    }
  }

  private def aliasedReturnItems(items: Seq[ReturnItem]): Map[Variable, Expression] =
    items.collect { case AliasedReturnItem(expr, ident) => ident -> expr }.toMap
}
