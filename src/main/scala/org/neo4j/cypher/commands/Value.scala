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
package org.neo4j.cypher.commands

import scala.collection.JavaConverters._
import org.neo4j.graphdb._
import java.lang.String
import org.neo4j.cypher._

abstract class Value extends (Map[String, Any] => Any) {
  def identifier: Identifier

  def checkAvailable(symbols: SymbolTable)

  def dependsOn: Set[String]
}

case class Literal(v: Any) extends Value {
  def apply(m: Map[String, Any]) = v

  def identifier: Identifier = LiteralIdentifier(v.toString)

  def checkAvailable(symbols: SymbolTable) {}

  def dependsOn: Set[String] = Set()

  override def toString() = if (v.isInstanceOf[String]) "\"" + v + "\"" else v.toString
}

abstract case class FunctionValue(functionName: String, arguments: Value*) extends Value {

  def identifier: Identifier = ValueIdentifier(functionName + "(" + arguments.map(_.identifier.name).mkString(",") + ")");

  def checkAvailable(symbols: SymbolTable) {
    arguments.foreach(_.checkAvailable(symbols))
  }

  def dependsOn: Set[String] = arguments.flatMap(_.dependsOn).toSet
}


case class NullablePropertyValue(subEntity: String, subProperty: String) extends PropertyValue(subEntity, subProperty) {
  protected override def handleNotFound(propertyContainer: PropertyContainer, x: NotFoundException): Any = null
}

case class PropertyValue(entity: String, property: String) extends Value {
  protected def handleNotFound(propertyContainer: PropertyContainer, x: NotFoundException): Any = throw new SyntaxException("%s.%s does not exist on %s".format(entity, property, propertyContainer), x)

  def apply(m: Map[String, Any]): Any = {
    m(entity).asInstanceOf[PropertyContainer] match {
      case null => null
      case propertyContainer => try {
        propertyContainer.getProperty(property)
      } catch {
        case x: NotFoundException => handleNotFound(propertyContainer, x)
      }
    }
  }

  def identifier: Identifier = PropertyIdentifier(entity, property)

  def checkAvailable(symbols: SymbolTable) {
    symbols.assertHas(PropertyContainerIdentifier(entity))
  }

  def dependsOn: Set[String] = Set(entity)

  override def toString(): String = entity + "." + property
}

case class RelationshipTypeValue(relationship: Value) extends FunctionValue("TYPE", relationship) {
  def apply(m: Map[String, Any]): Any = relationship(m).asInstanceOf[Relationship].getType.name()

  override def checkAvailable(symbols: SymbolTable) {
    symbols.assertHas(RelationshipIdentifier(relationship.identifier.name))
  }

  override def toString() = "type(" + relationship + ")"
}

case class ArrayLengthValue(inner: Value) extends FunctionValue("LENGTH", inner) {
  def apply(m: Map[String, Any]): Any = inner(m) match {
    case path: Path => path.length()
    case iter:Traversable[_] => iter.toList.length
    case s:String => s.length()
    case x => throw new IterableRequiredException(inner)
  }
}


case class IdValue(inner: Value) extends FunctionValue("ID", inner) {
  def apply(m: Map[String, Any]): Any = inner(m) match {
    case node: Node => node.getId
    case rel: Relationship => rel.getId
    case x => throw new SyntaxException("Expected " + inner.identifier.name + " to be a node or relationship.")
  }
}

case class PathNodesValue(path: Value) extends FunctionValue("NODES", path) {
  def apply(m: Map[String, Any]): Any = path(m) match {
    case p: Path => p.nodes().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }
}

case class Extract(iterable:Value, id:String, expression:Value) extends Value {
  def apply(m: Map[String, Any]): Any = iterable(m) match {
    case x:Iterable[Any] => x.map( iterValue => {
      val innerMap = m + (id -> iterValue)
      expression(innerMap)
    })
    case _ => throw new IterableRequiredException(iterable)
  }

  def identifier: Identifier = ArrayIdentifier("extract(" + id + " in " + iterable.identifier.name + " : " + expression.identifier.name + ")")

  def checkAvailable(symbols: SymbolTable) {
    iterable.checkAvailable(symbols)
  }

  def dependsOn: Set[String] = (iterable.dependsOn ++ expression.dependsOn) - id
}

case class PathRelationshipsValue(path: Value) extends FunctionValue("RELATIONSHIPS", path) {
  def apply(m: Map[String, Any]): Any = path(m) match {
    case p: Path => p.relationships().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }
}

case class EntityValue(entityName: String) extends Value {
  def apply(m: Map[String, Any]): Any = m.getOrElse(entityName, throw new NotFoundException)

  def identifier: Identifier = Identifier(entityName)

  def checkAvailable(symbols: SymbolTable) {
    symbols.assertHas(Identifier(entityName))
  }

  def dependsOn: Set[String] = Set(entityName)

  override def toString(): String = entityName
}

case class ParameterValue(parameterName: String) extends Value {
  def apply(m: Map[String, Any]): Any = m.getOrElse(parameterName, throw new ParameterNotFoundException("Expected a parameter named " + parameterName))

  def identifier: Identifier = Identifier(parameterName)

  def checkAvailable(symbols: SymbolTable) {}

  def dependsOn: Set[String] = Set(parameterName)
  override def toString(): String = "{" + parameterName + "}"
}