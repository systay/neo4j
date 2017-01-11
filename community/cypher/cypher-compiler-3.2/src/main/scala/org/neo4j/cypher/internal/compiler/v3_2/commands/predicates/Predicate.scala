/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.commands.predicates

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{Expression, Literal}
import org.neo4j.cypher.internal.compiler.v3_2.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v3_2.helpers.{CastSupport, IsList, IsMap, ListSupport}
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_2.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_2.helpers.NonEmptyList
import org.neo4j.graphdb._

import scala.util.{Failure, Success, Try}

abstract class Predicate extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState) = isMatch(ctx).orNull
  def isTrue(m: ExecutionContext)(implicit state: QueryState): Boolean = isMatch(m).getOrElse(false)
  def andWith(other: Predicate): Predicate = Ands(this, other)
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean]

  def andWith(preds: Predicate*): Predicate =
    if (preds.isEmpty) this else preds.fold(this)(_ andWith _)
}

object Predicate {
  def fromSeq(in: Seq[Predicate]) = in.reduceOption(_ andWith _).getOrElse(True())
}

abstract class CompositeBooleanPredicate extends Predicate {

  def predicates: NonEmptyList[Predicate]

  def shouldExitWhen: Boolean

  /**
   * This algorithm handles the case where we combine multiple AND or multiple OR groups (CNF or DNF).
   * As well as performing shortcut evaluation so that a false (for AND) or true (for OR) will exit the
   * evaluation without performing any further predicate evaluations (including those that could throw
   * exceptions). Any exception thrown is held until the end (or until the exit state) so that it is
   * superceded by exit predicates (false for AND and true for OR).
   */
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    predicates.foldLeft[Try[Option[Boolean]]](Success(Some(!shouldExitWhen))) { (previousValue, predicate) =>
      previousValue match {
        // if a previous evaluation was true (false) the OR (AND) result is determined
        case Success(Some(result)) if result == shouldExitWhen => previousValue
        case _ =>
          Try(predicate.isMatch(m)) match {
            // If we get the exit case (false for AND and true for OR) ignore any error cases
            case Success(Some(result)) if result == shouldExitWhen => Success(Some(shouldExitWhen))
            // Handle null only for non error cases
            case Success(None) if previousValue.isSuccess => Success(None)
            // errors or non-exit cases propagate as normal
            case Failure(e) if previousValue.isSuccess => Failure(e)
            case _ => previousValue
          }
      }
    } match {
      case Failure(e) => throw e
      case Success(option) => option
    }
  }
}

case class Not(a: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = a.isMatch(m) match {
    case Some(x) => Some(!x)
    case None    => None
  }
  override def toString: String = "NOT(" + a + ")"
}

case class Xor(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = (a.isMatch(m), b.isMatch(m)) match {
    case (None, _)          => None
    case (_, None)          => None
    case (Some(l), Some(r)) => Some(l ^ r)
  }

  override def toString: String = "(" + a + " XOR " + b + ")"
}

case class IsNull(expression: Expression) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = expression(m) match {
    case null => Some(true)
    case _    => Some(false)
  }

  override def toString: String = expression + " IS NULL"
}

case class True() extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = Some(true)
  override def toString: String = "true"
}

case class PropertyExists(variable: Expression, propertyKey: KeyToken) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = variable(m) match {
    case pc: Node         => Some(propertyKey.getOptId(state.query).exists(state.query.nodeOps.hasProperty(pc.getId, _)))
    case pc: Relationship => Some(propertyKey.getOptId(state.query).exists(state.query.relationshipOps.hasProperty(pc.getId, _)))
    case IsMap(map)       => Some(map(state.query).get(propertyKey.name).orNull != null)
    case null             => None
    case _                => throw new CypherTypeException("Expected " + variable + " to be a property container.")
  }

  override def toString: String = s"hasProp($variable.${propertyKey.name})"
}

trait StringOperator {
  self: Predicate =>
  override def isMatch(m: ExecutionContext)(implicit state: QueryState) = (lhs(m), rhs(m)) match {
    case (l: String, r: String) => Some(compare(l,r))
    case (_, _) => None
  }

  def lhs: Expression
  def rhs: Expression
  def compare(a: String, b: String): Boolean
}

case class StartsWith(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: String, b: String) = a.startsWith(b)
}

case class EndsWith(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: String, b: String) = a.endsWith(b)
}

case class Contains(lhs: Expression, rhs: Expression) extends Predicate with StringOperator {
  override def compare(a: String, b: String) = a.contains(b)
}

case class LiteralRegularExpression(lhsExpr: Expression, regexExpr: Literal)(implicit converter: String => String = identity) extends Predicate {
  lazy val pattern = converter(regexExpr.v.asInstanceOf[String]).r.pattern

  def isMatch(m: ExecutionContext)(implicit state: QueryState) =
    lhsExpr(m) match {
      case s: String => Some(pattern.matcher(s).matches())
      case _ => None
    }

  override def toString = s"$lhsExpr =~ $regexExpr"
}

case class RegularExpression(lhsExpr: Expression, regexExpr: Expression)(implicit converter: String => String = identity) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = (lhsExpr(m), regexExpr(m)) match {
    case (null, _) =>
      None
    case (_, null) =>
      None
    case (lhs, rhs)  =>
      val rhsAsRegexString = converter(CastSupport.castOrFail[String](rhs))
      if (!lhs.isInstanceOf[String])
        None
      else
        Some(rhsAsRegexString.r.pattern.matcher(lhs.asInstanceOf[String]).matches())
  }

  override def toString: String = lhsExpr.toString() + " ~= /" + regexExpr.toString() + "/"
}

case class NonEmpty(collection: Expression) extends Predicate with ListSupport {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    collection(m) match {
      case IsList(x) => Some(x.nonEmpty)
      case null            => None
      case x               => throw new CypherTypeException("Expected a collection, got `%s`".format(x))
    }
  }

  override def toString: String = "nonEmpty(" + collection.toString() + ")"
}

case class HasLabel(entity: Expression, label: KeyToken) extends Predicate {

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = entity(m) match {

    case null =>
      None

    case value =>
      val node           = CastSupport.castOrFail[Node](value)
      val nodeId         = node.getId
      val queryCtx       = state.query

      label.getOptId(state.query) match {
        case None =>
          Some(false)
        case Some(labelId) =>
          Some(queryCtx.isLabelSetOnNode(labelId, nodeId))
      }
  }

  override def toString = s"$entity:${label.name}"
}

case class CoercedPredicate(inner:Expression) extends Predicate with ListSupport {
  def isMatch(m: ExecutionContext)(implicit state: QueryState) = inner(m) match {
    case x: Boolean   => Some(x)
    case null         => None
    case IsList(coll) => Some(coll.nonEmpty)
    case x            => throw new CypherTypeException(s"Don't know how to treat that as a predicate: $x")
  }

  override def toString = inner.toString
}
