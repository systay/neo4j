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
package org.neo4j.cypher.internal.parser.v1_8

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.{True, Entity, Expression}

trait ParserPattern extends Base {

  def usePattern[T](translator: AbstractPattern => Maybe[T], acceptable: Seq[T] => Boolean): Parser[Seq[T]] = Parser {
    case in =>
      usePattern(translator)(in) match {
        case Success(patterns, rest) =>
          if (acceptable(patterns))
            Success(patterns, rest)
          else
            Failure("", rest)
        case Failure(msg, rest) => Failure(msg, rest)
        case Error(msg, rest) => Error(msg, rest)
      }
  }

  def usePattern[T](translator: AbstractPattern => Maybe[T]): Parser[Seq[T]] = Parser {
    case in => translate(in, translator, pattern(in))
  }

  def usePath[T](translator: AbstractPattern => Maybe[T]):Parser[Seq[T]] = Parser {
    case in => translate(in, translator, path(in))
  }

  private def translate[T](in: Input, translator: (AbstractPattern) => Maybe[T], pattern1: ParseResult[Seq[AbstractPattern]]): ParseResult[Seq[T]] with Product with Serializable = {
    pattern1 match {
      case Success(abstractPattern, rest) =>
        val concretePattern = abstractPattern.map(p => translator(p))

        concretePattern.find(!_.success) match {
          case Some(No(msg)) => Failure(msg, rest)
          case None => Success(concretePattern.map(_.value), rest)
        }

      case Failure(msg, rest) => Failure(msg, rest)
      case Error(msg, rest) => Error(msg, rest)
    }
  }

  private def pattern: Parser[Seq[AbstractPattern]] = commaList(patternBit) ^^ (patterns => patterns.flatten)

  private def patternBit: Parser[Seq[AbstractPattern]] =
    pathAssignment |
      path |
      singleNode

  private def singleNode: ParserPattern.this.type#Parser[Seq[ParsedEntity]] = {
    node ^^ (n => Seq(n))
  }

  private def pathAssignment: Parser[Seq[AbstractPattern]] = optParens(identity) ~ "=" ~ optParens(path) ^^ {
    case pathName ~ "=" ~ Seq(p: ParsedShortestPath) => Seq(p.rename(pathName))
    case pathName ~ "=" ~ patterns => Seq(ParsedNamedPath(pathName, patterns))
  }

  private def node: Parser[ParsedEntity] =
    parens(nodeFromExpression) |  // whatever expression, but inside parenthesis
      singleNodeEqualsMap | // x = {}
      nodeIdentifier |    // x
      nodeInParenthesis | // (x {})
      failure("expected an expression that is a node")


  private def singleNodeEqualsMap = identity ~ "=" ~ properties ^^ {
    case name ~ "=" ~ map => ParsedEntity(Entity(name), map, True())
  }

  private def nodeInParenthesis = parens(opt(identity) ~ props) ^^ {
    case id ~ props => ParsedEntity(Entity(namer.name(id)), props, True())
  }

  private def nodeFromExpression = Parser {
    case in => expression(in) match {
      case Success(exp, rest) => Success(ParsedEntity(exp, Map[String, Expression](), True()), rest)
      case x: Error => x
      case Failure(msg, rest) => failure("expected an expression that is a node", rest)
    }
  }

  private def nodeIdentifier = identity ^^ {
    case name => ParsedEntity(Entity(name), Map[String, Expression](), True())
  }

  private def path: Parser[List[AbstractPattern]] = relationship | shortestPath

  private def relationship: Parser[List[AbstractPattern]] = {
    node ~ rep1(tail) ^^ {
      case head ~ tails =>
        var start = head
        val links = tails.map {
          case Tail(dir, relName, relProps, end, None, types, optional) =>
            val (l, r, d) = turnStartAndEndCorrect(dir, end, start)
            val t = ParsedRelation(namer.name(relName), relProps, l, r, types, d, optional, True())
            start = end
            t
          case Tail(dir, relName, relProps, end, Some((min, max)), types, optional) =>
            val (l, r, d) = turnStartAndEndCorrect(dir, end, start)
            val t = ParsedVarLengthRelation(namer.name(None), relProps, l, r, types, d, optional, True(), min, max, relName)
            start = end
            t
        }

        List(links: _*)
    }
  }

  private def patternForShortestPath: Parser[AbstractPattern] = onlyOne("expected single path segment", relationship)

  private def shortestPath: Parser[List[AbstractPattern]] = (ignoreCase("shortestPath") | ignoreCase("allShortestPaths")) ~ parens(patternForShortestPath) ^^ {
    case algo ~ relInfo =>
      val single = algo match {
        case "shortestpath" => true
        case "allshortestpaths" => false
      }

      val PatternWithEnds(start, end, typez, dir, optional, maxDepth, relIterator, predicate) = relInfo

      List(ParsedShortestPath(name = namer.name(None),
        props = Map(),
        start = start,
        end = end,
        typ = typez,
        dir = dir,
        optional = optional,
        predicate = predicate,
        maxDepth = maxDepth,
        single = single,
        relIterator = relIterator))
  }

  // It's easier on everything if all relationships are either outgoing or both, but never incoming.
  // So we turn all patterns around, facing the same way
  private def turnStartAndEndCorrect(dir: Direction, end: ParsedEntity, start: ParsedEntity): (ParsedEntity, ParsedEntity, Direction) = {
    dir match {
      case Direction.INCOMING => (end, start, Direction.OUTGOING)
      case Direction.OUTGOING => (start, end, Direction.OUTGOING)
      case Direction.BOTH => (start.expression, end.expression) match {
        case (Entity(a), Entity(b)) if a < b => (start, end, Direction.BOTH)
        case (Entity(a), Entity(b)) if a >= b => (end, start, Direction.BOTH)
        case _ => (start, end, Direction.BOTH)
      }
    }
  }

  private def tailWithRelData: Parser[Tail] = opt("<") ~ "-" ~ "[" ~ opt(identity) ~ opt("?") ~ opt(":" ~> rep1sep(identity, "|")) ~ variable_length ~ props ~ "]" ~ "-" ~ opt(">") ~ node ^^ {
    case l ~ "-" ~ "[" ~ rel ~ optional ~ typez ~ varLength ~ properties ~ "]" ~ "-" ~ r ~ end => Tail(direction(l, r), rel, properties, end, varLength, typez.toSeq.flatten.distinct, optional.isDefined)
  } | linkErrorMessages

  private def linkErrorMessages: Parser[Tail] =
    opt("<") ~> "-" ~> "[" ~> opt(identity) ~> opt("?") ~> opt(":" ~> rep1sep(identity, "|")) ~> variable_length ~> props ~> "]" ~> failure("expected -") |
      opt("<") ~> "-" ~> "[" ~> opt(identity) ~> opt("?") ~> opt(":" ~> rep1sep(identity, "|")) ~> variable_length ~> props ~> failure("unclosed bracket") |
      opt("<") ~> "-" ~> "[" ~> failure("expected relationship information") |
      opt("<") ~> "-" ~> failure("expected [ or -")

  private def variable_length = opt("*" ~ opt(wholeNumber) ~ opt("..") ~ opt(wholeNumber)) ^^ {
    case None => None
    case Some("*" ~ None ~ None ~ None) => Some(None, None)
    case Some("*" ~ min ~ None ~ None) => Some((min.map(_.toInt), min.map(_.toInt)))
    case Some("*" ~ min ~ _ ~ max) => Some((min.map(_.toInt), max.map(_.toInt)))
  }

  private def tailWithNoRelData = opt("<") ~ "--" ~ opt(">") ~ node ^^ {
    case l ~ "--" ~ r ~ end => Tail(direction(l, r), None, Map(), end, None, Seq(), optional = false)
  }

  private def tail: Parser[Tail] = tailWithRelData | tailWithNoRelData

  private def props = opt(properties) ^^ {
    case None => Map[String, Expression]()
    case Some(x) => x
  }

  private def properties =
    expression ^^ (x => Map[String, Expression]("*" -> x)) |
      "{" ~> repsep(propertyAssignment, ",") <~ "}" ^^ (_.toMap)


  private def direction(l: Option[String], r: Option[String]): Direction = (l, r) match {
    case (None, Some(_)) => Direction.OUTGOING
    case (Some(_), None) => Direction.INCOMING
    case _ => Direction.BOTH

  }

  private def propertyAssignment: Parser[(String, Expression)] = identity ~ ":" ~ expression ^^ {
    case id ~ ":" ~ exp => (id, exp)
  }

  def expression: Parser[Expression]

  private case class Tail(dir: Direction,
                          relName: Option[String],
                          relProps: Map[String, Expression],
                          end: ParsedEntity,
                          varLength: Option[(Option[Int], Option[Int])],
                          types: Seq[String],
                          optional: Boolean)

  abstract sealed class Maybe[+T] {
    def value: T

    def success: Boolean
  }

  case class Yes[T](value: T) extends Maybe[T] {
    def success = true
  }

  case class No(msg: String) extends Maybe[Nothing] {
    def value = throw new Exception()

    def success = false
  }

}
