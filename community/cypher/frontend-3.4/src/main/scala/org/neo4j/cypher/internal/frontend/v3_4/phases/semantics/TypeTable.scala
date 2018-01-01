/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.phases.semantics

import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Types.NewCypherType
import org.neo4j.cypher.internal.frontend.v3_4.phases.semantics.Typing.TypeAccessor
import org.neo4j.cypher.internal.util.v3_4.attribution.Attribute
import org.neo4j.cypher.internal.v3_4.expressions.Expression

class TypeTable extends Attribute[TypeConstraint] with TypeAccessor {
  def get(e: Expression): Set[NewCypherType] =
    get(e.secretId).possibleTypes

  def set(e: Expression, t: TypeConstraint): Unit = set(e.secretId, t)
}

class TypeExpectations extends Attribute[Set[NewCypherType]] {
  def set(e: Expression, types: NewCypherType*): Unit = set(e.secretId, types.toSet)
  def set(e: Expression, types: Set[NewCypherType]): Unit = set(e.secretId, types)
}

sealed trait TypeConstraint {
  def possibleTypes: Set[NewCypherType]

  def possibleErrors: Set[String]
}

// Set of types that this expression could have. This is used when we have at least one valid
// type - we can't fail here, and need to check types at runtime to make sure we are OK
case class ValidTypeConstraint(possibleTypes: Set[NewCypherType]) extends TypeConstraint {
  override def toString: String = ":" + possibleTypes.mkString(" | ")

  override def possibleErrors = Set.empty[String]
}

// No valid types found for this expression. We want to finish typing of the rest of the tree before failing,
// so we store the errors here, but use the fallback types as the type for the expression when asked
case class InvalidTypeConstraint(fallbackTypes: Set[NewCypherType], errors: Set[String]) extends TypeConstraint {
  override def possibleTypes: Set[NewCypherType] = fallbackTypes

  override def possibleErrors: Set[String] = errors
}
