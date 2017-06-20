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

import org.neo4j.cypher.internal.frontend.v3_3.InternalException

object RegisterAllocations {
  def empty = new RegisterAllocations(Map.empty, 0, 0)
}

class RegisterAllocations(var slots: Map[String, Slot], var numberOfLongs: Int = 0, var numberOfReferences: Int = 0) {

  private def checkNotAlreadyTaken(key: String) =
    if (slots.contains(key))
      throw new InternalException("Tried overwriting already taken variable name")

  def newLong(name: String): Unit = {
    checkNotAlreadyTaken(name)
    val slot = LongSlot(numberOfLongs)
    slots = slots + (name -> slot)
    numberOfLongs = numberOfLongs + 1
  }

  def newReference(name: String): Unit = {
    checkNotAlreadyTaken(name)
    val slot = RefSlot(numberOfReferences)
    slots = slots + (name -> slot)
    numberOfReferences = numberOfReferences + 1
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[RegisterAllocations]

  override def equals(other: Any): Boolean = other match {
    case that: RegisterAllocations =>
      (that canEqual this) &&
        slots == that.slots &&
        numberOfLongs == that.numberOfLongs &&
        numberOfReferences == that.numberOfReferences
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(slots, numberOfLongs, numberOfReferences)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"RegisterAllocations(slots=$slots, longs=$numberOfLongs, objs=$numberOfReferences)"

  def getLongOffsetFor(name: String): Int = slots.get(name) match {
    case Some(s:LongSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no long slot for `$name`. It was a $s")
    case _ => throw new InternalException("Uh oh... There was no slot for `$name`")
  }

  def getReferenceOffsetFor(name: String): Int = slots.get(name) match {
    case Some(s: RefSlot) => s.offset
    case Some(s) => throw new InternalException(s"Uh oh... There was no reference slot for `$name`. It was a $s")
    case _ => throw new InternalException("Uh oh... There was no slot for `$name`")
  }
}