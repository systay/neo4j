/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import scala.collection.JavaConverters._
import java.lang.String
import org.neo4j.cypher._
import internal.symbols._
import org.neo4j.graphdb.{Path, Relationship, NotFoundException, PropertyContainer, Node}
import collection.Seq

abstract class Expression extends (Map[String, Any] => Any) {
  def identifier: Identifier

  def declareDependencies(extectedType: AnyType): Seq[Identifier]

  def dependencies(extectedType: AnyType): Seq[Identifier] = {
    val myType = identifier.typ
    if (!extectedType.isAssignableFrom(myType))
      throw new SyntaxException(identifier.name + " expected to be of type " + extectedType + " but it is of type " + identifier.typ)
    declareDependencies(extectedType)
  }

  def rewrite(f: Expression => Expression): Expression
}

case class Add(a: Expression, b: Expression) extends Expression {
  def identifier = Identifier(a.identifier.name + " + " + b.identifier.name, ScalarType())

  def apply(m: Map[String, Any]) = {
    val aVal = a(m)
    val bVal = b(m)

    (aVal, bVal) match {
      case (x: Number, y: Number) => x.doubleValue() + y.doubleValue()
      case (x: String, y: String) => x + y
      case _ => throw new CypherTypeException("Don't know how to add `" + aVal.toString + "` and `" + bVal.toString + "`")
    }

  }

  def declareDependencies(extectedType: AnyType) = a.declareDependencies(extectedType) ++ b.declareDependencies(extectedType)

  def rewrite(f: (Expression) => Expression) = f(Add(a.rewrite(f), b.rewrite(f)))
}

case class Subtract(a: Expression, b: Expression) extends Expression {
  def identifier = Identifier(a.identifier.name + " + " + b.identifier.name, NumberType())

  def apply(m: Map[String, Any]) = {
    val aVal = a(m)
    val bVal = b(m)

    (aVal, bVal) match {
      case (x: Number, y: Number) => x.doubleValue() - y.doubleValue()
      case _ => throw new CypherTypeException("Don't know how to subtract `" + bVal.toString + "` from `" + aVal.toString + "`")
    }

  }

  def declareDependencies(extectedType: AnyType) = a.declareDependencies(extectedType) ++ b.declareDependencies(extectedType)

  def rewrite(f: (Expression) => Expression) = f(Subtract(a.rewrite(f), b.rewrite(f)))
}

case class Modulo(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "%"

  def verb = "modulo"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = a.doubleValue() % b.doubleValue()

  def rewrite(f: (Expression) => Expression) = f(Modulo(a.rewrite(f), b.rewrite(f)))
}

case class Pow(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "^"

  def verb = "power"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = math.pow(a.doubleValue(), b.doubleValue())

  def rewrite(f: (Expression) => Expression) = f(Pow(a.rewrite(f), b.rewrite(f)))
}

case class Multiply(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "*"

  def verb = "multiply"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = a.doubleValue() * b.doubleValue()

  def rewrite(f: (Expression) => Expression) = f(Multiply(a.rewrite(f), b.rewrite(f)))
}

case class Divide(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "/"

  def verb = "divide"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = a.doubleValue() / b.doubleValue()

  def rewrite(f: (Expression) => Expression) = f(Divide(a.rewrite(f), b.rewrite(f)))
}

abstract class Arithmetics(a: Expression, b: Expression) extends Expression {
  def identifier = Identifier("%s %s %s".format(a.identifier.name, operand, b.identifier.name), ScalarType())

  def operand: String

  def throwTypeError(bVal: Any, aVal: Any): Nothing = {
    throw new CypherTypeException("Don't know how to subtract `" + bVal.toString + "` from `" + aVal.toString + "`")
  }

  def apply(m: Map[String, Any]) = {
    val aVal = a(m)
    val bVal = b(m)

    (aVal, bVal) match {
      case (x: Number, y: Number) => numberWithNumber(x, y)
      case (x: String, y: String) => stringWithString(x, y)
      case _ => throwTypeError(bVal, aVal)
    }

  }

  def verb: String

  def stringWithString(a: String, b: String): String

  def numberWithNumber(a: Number, b: Number): Number

  def declareDependencies(extectedType: AnyType) = a.declareDependencies(extectedType) ++ b.declareDependencies(extectedType)
}

case class Literal(v: Any) extends Expression {
  def apply(m: Map[String, Any]) = v

  def identifier = Identifier(v.toString, AnyType.fromJava(v))

  override def toString() = if (v.isInstanceOf[String]) "\"" + v + "\"" else v.toString

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq()

  def rewrite(f: (Expression) => Expression) = f(this)
}

abstract class CastableExpression extends Expression {
  override def dependencies(extectedType: AnyType): Seq[Identifier] = declareDependencies(extectedType)
}

case class Nullable(expression: Expression) extends Expression {
  def identifier = Identifier(expression.identifier.name + "?", expression.identifier.typ)

  def apply(m: Map[String, Any]) = try {
    expression.apply(m)
  } catch {
    case x: EntityNotFoundException => null
  }

  def declareDependencies(extectedType: AnyType) = expression.dependencies(extectedType)

  override def dependencies(extectedType: AnyType) = expression.dependencies(extectedType)

  def rewrite(f: (Expression) => Expression) = f(Nullable(expression.rewrite(f)))
}

case class Property(entity: String, property: String) extends CastableExpression {
  def apply(m: Map[String, Any]): Any = {
    m(entity).asInstanceOf[PropertyContainer] match {
      case null => null
      case propertyContainer => try {
        propertyContainer.getProperty(property)
      } catch {
        case x: NotFoundException => throw new EntityNotFoundException("The property '%s' does not exist on %s".format(property, propertyContainer), x)
      }
    }
  }

  def identifier: Identifier = Identifier(entity + "." + property, ScalarType())

  override def toString(): String = entity + "." + property

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq(Identifier(entity, MapType()))

  def rewrite(f: (Expression) => Expression) = f(this)
}

case class RelationshipTypeFunction(relationship: Expression) extends Expression {
  def apply(m: Map[String, Any]): Any = relationship(m).asInstanceOf[Relationship].getType.name()

  def identifier = Identifier("TYPE(" + relationship.identifier.name + ")", StringType())

  override def toString() = "type(" + relationship + ")"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = relationship.dependencies(RelationshipType())

  def rewrite(f: (Expression) => Expression) = f(RelationshipTypeFunction(relationship.rewrite(f)))
}

case class CoalesceFunction(expressions: Expression*) extends Expression {
  def apply(m: Map[String, Any]): Any = expressions.map(expression => expression(m)).find(value => value != null) match {
    case None => null
    case Some(x) => x
  }

  def innerExpectedType: Option[AnyType] = null

  def argumentsString: String = expressions.map(_.identifier.name).mkString(",")

  //TODO: Find out the closest matching return type
  def identifier = Identifier("COALESCE(" + argumentsString + ")", AnyType())

  override def toString() = "coalesce(" + argumentsString + ")"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = expressions.flatMap(_.dependencies(AnyType()))

  def rewrite(f: (Expression) => Expression) = f(CoalesceFunction(expressions.map(e => e.rewrite(f)): _*))
}

case class LengthFunction(inner: Expression) extends Expression {
  def apply(m: Map[String, Any]): Any = inner(m) match {
    case path: Path => path.length()
    case iter: Traversable[_] => iter.toList.length
    case s: String => s.length()
    case x => throw new IterableRequiredException(inner)
  }

  def identifier = Identifier("LENGTH(" + inner.identifier.name + ")", IntegerType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = {
    val seq = inner.dependencies(AnyIterableType()).toList
    seq
  }

  def rewrite(f: (Expression) => Expression) = f(LengthFunction(inner.rewrite(f)))
}

case class IdFunction(inner: Expression) extends Expression {
  def apply(m: Map[String, Any]): Any = inner(m) match {
    case node: Node => node.getId
    case rel: Relationship => rel.getId
  }

  def identifier = Identifier("ID(" + inner.identifier.name + ")", LongType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = inner.dependencies(MapType())

  def rewrite(f: (Expression) => Expression) = f(IdFunction(inner.rewrite(f)))
}

case class NodesFunction(path: Expression) extends Expression {
  def apply(m: Map[String, Any]): Any = path(m) match {
    case p: Path => p.nodes().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }

  def identifier = Identifier("NODES(" + path.identifier.name + ")", new IterableType(NodeType()))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = path.dependencies(PathType())

  def rewrite(f: (Expression) => Expression) = f(NodesFunction(path.rewrite(f)))
}

case class ExtractFunction(iterable: Expression, id: String, expression: Expression) extends Expression {
  def apply(m: Map[String, Any]): Any = iterable(m) match {
    case x: Iterable[Any] => x.map(iterValue => {
      val innerMap = m + (id -> iterValue)
      expression(innerMap)
    }).toList
    case _ => throw new IterableRequiredException(iterable)
  }

  def identifier = Identifier("extract(" + id + " in " + iterable.identifier.name + " : " + expression.identifier.name + ")", new IterableType(expression.identifier.typ))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] =
  // Extract depends on everything that the iterable and the expression depends on, except
  // the new identifier inserted into the expression context, named with id
    iterable.dependencies(AnyIterableType()) ++ expression.dependencies(AnyType()).filterNot(_.name == id)

  def rewrite(f: (Expression) => Expression) = f(ExtractFunction(iterable.rewrite(f), id, expression.rewrite(f)))
}

case class RelationshipFunction(path: Expression) extends Expression {
  def apply(m: Map[String, Any]): Any = path(m) match {
    case p: Path => p.relationships().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }

  def identifier = Identifier("RELATIONSHIPS(" + path.identifier.name + ")", new IterableType(RelationshipType()))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = path.dependencies(PathType())

  def rewrite(f: (Expression) => Expression) = f(RelationshipFunction(path.rewrite(f)))
}

case class Entity(entityName: String) extends CastableExpression {
  def apply(m: Map[String, Any]): Any = m.getOrElse(entityName, throw new NotFoundException)

  def identifier: Identifier = Identifier(entityName, AnyType())

  override def toString(): String = entityName

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq(Identifier(entityName, extectedType))

  def rewrite(f: (Expression) => Expression) = f(this)
}

case class Parameter(parameterName: String) extends CastableExpression {
  def apply(m: Map[String, Any]): Any = m.getOrElse("-=PARAMETER=-" + parameterName + "-=PARAMETER=-", throw new ParameterNotFoundException("Expected a parameter named " + parameterName)) match {
    case ParameterValue(x) => x
    case _ => throw new ParameterNotFoundException("Expected a parameter named " + parameterName)
  }

  def identifier: Identifier = Identifier(parameterName, AnyType())

  override def toString(): String = "{" + parameterName + "}"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq()

  def rewrite(f: (Expression) => Expression) = f(this)
}

case class ParameterValue(value: Any)