/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.values

/**
 * This singleton value represents unbound entities inside expressions or predicates.
 *
 * It currently only may occur due to patterns containing optional relationships which may introduce
 * unbound identifiers.  It mainly serves to differentiate this situation from plain null values.
 *
 * Semantics:
 *
 * [X] You can't compare it, two unbound are always unequal
 * [X] You can't return it. It will always be mapped to null prior to returning any value.
 * [ ] UnboundValue equals null
 * [ ] Figure out the type. Candidate: UnboundType() subtype of nothing. Alternative: Just use AnyType()
 * [ ] You cannot ask for labels from an unbound value.
 * [ ] Unbound values are invalid arguments to expressions that compute a result value
 * [ ] You cannot get properties from an unbound value.
 * [ ] You cannot remove properties from an unbound value.
 * [ ] You cannot set properties to an unbound value.
 */
case object UnboundValue {
  def is(v: Any): Boolean = v == this

  override def toString = "UNBOUND_VALUE"
}
