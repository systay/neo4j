/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes.matching

import org.neo4j.graphdb.{Path, Node, Relationship}
import java.lang.IllegalArgumentException

abstract class GraphRelationship {
  def getOtherNode(node:Node): Node
}

case class SingleGraphRelationship(rel: Relationship) extends GraphRelationship {
  def getOtherNode(node: Node): Node = rel.getOtherNode(node)

  override def canEqual(that: Any) = that.isInstanceOf[SingleGraphRelationship] || that.isInstanceOf[Relationship]

  override def equals(obj: Any) = obj == this || obj == rel
}

case class VariableLengthGraphRelationship(path: Path) extends GraphRelationship {
  def getOtherNode(node: Node): Node = {
    if (path.startNode() == node) path.endNode()
    else if (path.endNode() == node) path.startNode()
    else throw new IllegalArgumentException("Node is not start nor end of path.")
  }

  override def canEqual(that: Any) = that.isInstanceOf[VariableLengthGraphRelationship] || that.isInstanceOf[Path]

  override def equals(obj: Any) = obj == this || obj == path
}
