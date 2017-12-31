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

object Types {

  sealed trait NewCypherType

  case class ListType(inner: Set[NewCypherType]) extends NewCypherType {
    override def toString: String = s"List[${inner.mkString(",")}]"

    /**
      * Instead of grabbing the inner field directly, use this method,
      * that will expand List[?] to any valid inner type
      * @return
      */
    def elementTypes: Set[NewCypherType] =
      if (inner.size == 1 && inner.head == ?)
        ???
      else
        inner
  }

  case class MapType(possibleTypes: Set[NewCypherType]) extends NewCypherType {
    override def toString: String = s"Map[${possibleTypes.mkString(",")}]"
  }

  val ANY: Set[NewCypherType] = Set(
    ListType.ListOfUnknown,
    MapType.MapOfUnknown,
    IntegerType,
    StringType,
    FloatType,
    BoolType,
    NodeType,
    RelationshipType,
    PointType,
    GeometryType,
    PathType,
    GraphRefType
  )

  case object IntegerType extends NewCypherType
  case object StringType extends NewCypherType
  case object FloatType extends NewCypherType
  case object BoolType extends NewCypherType
  case object NodeType extends NewCypherType
  case object RelationshipType extends NewCypherType
  case object PointType extends NewCypherType
  case object GeometryType extends NewCypherType
  case object PathType extends NewCypherType
  case object GraphRefType extends NewCypherType

  // Used to model both literal null-values and the fact that
  // a lot of property access can return null
  case object NullType extends NewCypherType

  // This special type is used to stop type possibility explosion -
  // a list of any type could be a list of lists, or a list of list of list.
  // Instead, we model these using List[
  case object ? extends NewCypherType

  object ListType {
    val ListOfUnknown = ListType(?)

    def apply(t: NewCypherType*): ListType = ListType(t.toSet)
  }

  object MapType {
    val MapOfUnknown = MapType(?)

    def apply(t: NewCypherType*): MapType = MapType(t.toSet)
  }

}
