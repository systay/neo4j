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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized._

//class NodeHashJoinBuild elxtends MidOperator {
//
//  /*
//  The hash table is keyed by an array of longs representing the node ids, and the values is a 2-element array of ints, with the
//   */
//
//
//
//  override def operate(data: Morsel, context: QueryContext, state: QueryState): ReturnType = {
//
//    UnitType
//  }
//}

class TLHashMap(data: Morsel) {

  val loadFactor: Double = 0.8
  val tableSize: Int = (data.validRows / loadFactor).toInt
  val table = new Array[Entry](tableSize)

  class Entry(hash: Int, offset: Int, var next: Entry)

  def add(node: Long, row: Int): Unit = {
    val hash = node.hashCode()
    val offset = hash % tableSize
    val el = new Entry(hash, row, null)
    var current = table(offset)
    if (current == null)
      table(offset) = el
    else {
      while(current.next == null) {
        current = current.next
      }
      current.next = el
    }
  }
}