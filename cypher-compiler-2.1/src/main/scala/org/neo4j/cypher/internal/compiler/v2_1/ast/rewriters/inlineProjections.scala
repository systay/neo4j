/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException

object inlineProjections extends (Statement => Statement) {

  def apply(input: Statement): Statement = {
    val context = inliningContextCreator(input)

    val removePatternPartNames = TypedRewriter[Pattern](bottomUp(namedPatternPartRemover))
    val inliner = TypedRewriter[ASTNode](context.identifierRewriter)
    val inlineReturnItems = inlineReturnItemsFactory(inliner.narrowed(_))

    val inliningRewriter = Rewriter.lift {
      case withClause @ With(false, returnItems @ ListedReturnItems(items), orderBy, None, None, None) =>
        val pos = returnItems.position
        val filteredItems = items.collect {
          case item@AliasedReturnItem(expr, ident) if !context.projections.contains(ident) || containsAggregate(expr) =>
            item
        }

        val newReturnItems =
          if (filteredItems.isEmpty) ReturnAll()(pos)
          else inlineReturnItems(returnItems.copy(items = filteredItems)(pos))

        withClause.copy(
          returnItems = newReturnItems,
          orderBy = orderBy.map(inliner.narrowed(_))
        )(withClause.position)

      case returnClause @ Return(_, returnItems: ListedReturnItems, orderBy, skip, limit) =>
        returnClause.copy(
          returnItems = inlineReturnItems(returnItems),
          orderBy = orderBy.map(inliner.narrowed(_)),
          skip = skip.map(inliner.narrowed(_)),
          limit = limit.map(inliner.narrowed(_))
        )(returnClause.position)

      case m @ Match(_, mPattern, mHints, mOptWhere) =>
        val newOptWhere = mOptWhere.map(inliner.narrowed(_))
        val newHints = mHints.map(inliner.narrowed(_))
        // no need to inline in patterns since all expressions have been moved to WHERE prior to
        // calling inlineProjections
        val newPattern = removePatternPartNames(mPattern)
        m.copy(pattern = newPattern, hints = newHints, where = newOptWhere)(m.position)

      case _: UpdateClause  =>
        throw new CantHandleQueryException

      case clause: Clause =>
        inliner.narrowed(clause)
    }

    input.rewrite(topDown(inliningRewriter)).asInstanceOf[Statement]
  }

  private def inlineReturnItemsFactory(inlineExpressions: Expression => Expression) =
    (returnItems: ListedReturnItems) =>
      returnItems.copy(
        items = returnItems.items.collect {
          case item: AliasedReturnItem =>
            item.copy( expression = inlineExpressions(item.expression))(item.position)
        }
      )(returnItems.position)
}



