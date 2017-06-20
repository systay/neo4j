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
package org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime

import org.neo4j.cypher.internal.compatibility.v3_3.ast.{IdFromRegister, NodeFromRegister, NodeProperty, RelationshipProperty}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.{ast => registerExpressions}
import org.neo4j.cypher.internal.compiler.v3_3.ast.NestedPlanExpression
import org.neo4j.cypher.internal.frontend.v3_3.ast.functions.Id
import org.neo4j.cypher.internal.frontend.v3_3.ast.{FunctionInvocation, Property, Variable, Expression => ASTExpression}
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, SemanticTable, topDown, ast => frontEndAst}

class RegisterExpressionRewriter(semanticTable: SemanticTable, registerAllocations: RegisterAllocations)
  extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val instance = topDown(Rewriter.lift {
    case p@Property(Variable(ident), propertyKey) if semanticTable.containsNode(ident) =>
      NodeProperty(registerAllocations.getLongOffsetFor(ident), propertyKey)(p.position)

    case p@Property(Variable(ident), propertyKey) if semanticTable.containsRelationship(ident) =>
      RelationshipProperty(registerAllocations.getLongOffsetFor(ident), propertyKey)(p.position)

    case Property(Variable(_), _) =>
      throw new RegisterAllocationFailed("still haven't figured this one out")

    case x@FunctionInvocation(namespace, functionName, _, args) if functionName.name == Id.name =>
      longOffsetFor(args, registerAllocations) getOrElse x

    case v@Variable(ident) if semanticTable.containsNode(ident) =>
      NodeFromRegister(registerAllocations.getLongOffsetFor(ident))(v.position)

    case v@frontEndAst.GetDegree(Variable(key), relType, dir) if semanticTable.containsNode(key) =>
      registerExpressions.GetDegree(registerAllocations.getLongOffsetFor(key), relType, dir)(v.position)

    case v@frontEndAst.HasLabels(Variable(key), label :: nil) if semanticTable.containsNode(key) =>
      registerExpressions.HasLabels(registerAllocations.getLongOffsetFor(key), label)(v.position)

    case f: frontEndAst.FunctionInvocation if f.function == frontEndAst.functions.Exists =>
      throw new RegisterAllocationFailed(s"${f.function} not supported with register allocation yet")

    case _: frontEndAst.IterablePredicateExpression |
         _: frontEndAst.AndedPropertyInequalities |
         _: NestedPlanExpression
    =>
      throw new RegisterAllocationFailed(s"An expression was not supported with register allocation yet")

    case v: Variable =>
      throw new RegisterAllocationFailed(s"An expression was not supported with register allocation yet")

  })

  private def longOffsetFor(args: IndexedSeq[ASTExpression], registerAllocations: RegisterAllocations): Option[ASTExpression] =
    if (args.size == 1) {
      args.head match {
        case v@Variable(key) =>
          registerAllocations.slots.get(key) match {
            case Some(LongSlot(offset)) => Some(IdFromRegister(offset)(v.position))
            case _ => None
          }

        case _ => None
      }
    } else None
}
