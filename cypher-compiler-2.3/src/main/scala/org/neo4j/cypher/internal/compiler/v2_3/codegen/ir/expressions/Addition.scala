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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{CodeGenContext, MethodStructure}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

case class Addition(lhs: CodeGenExpression, rhs: CodeGenExpression) extends CodeGenExpression with BinaryOperator {

  override protected def generator[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = structure.add

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  val validTypes = Seq(CTString, CTFloat, CTInteger, CTCollection(CTAny))

  override def cypherType(implicit context: CodeGenContext) = (lhs.cypherType, rhs.cypherType) match {
    // Strings
    case (CTString, CTString) => CTString

    // Collections
    case (CollectionType(left), CollectionType(right)) => CollectionType(left leastUpperBound right)
    case (CollectionType(innerType), singleElement) => CollectionType(innerType leastUpperBound singleElement)
    case (singleElement, CollectionType(innerType)) => CollectionType(innerType leastUpperBound singleElement)

    // Numbers
    case (CTInteger, CTInteger) => CTInteger
    case (Number(_), Number(_)) => CTFloat

    // Runtime we'll figure it out
    case _ => CTAny
  }

  object Number {
    def unapply(x: CypherType): Option[CypherType] = if (CTNumber.isAssignableFrom(x)) Some(x) else None
  }
}
