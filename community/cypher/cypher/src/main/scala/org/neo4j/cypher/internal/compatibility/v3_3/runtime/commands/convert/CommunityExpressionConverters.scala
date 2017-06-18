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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.ProjectedPath._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{InequalitySeekRangeExpression, ProjectedPath, Expression => CommandExpression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.UnresolvedRelType
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{PathExtractorExpression, predicates, expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.compiler.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.functions._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.DesugaredMapProjection
import org.neo4j.cypher.internal.frontend.v3_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection, ast}

object CommunityExpressionConverters extends ExpressionConverters {

  import PatternConverters._

  override def toCommandExpression(expression: ast.Expression, self: ExpressionConverters): CommandExpression =
    expression match {
      case e: ast.Null => commandexpressions.Null()
      case e: ast.True => predicates.True()
      case e: ast.False => predicates.Not(predicates.True())
      case e: ast.Literal => commandexpressions.Literal(e.value)
      case e: ast.Variable => variable(e)
      case e: ast.Or => predicates.Or(self.toCommandPredicate(e.lhs), self.toCommandPredicate(e.rhs))
      case e: ast.Xor => predicates.Xor(self.toCommandPredicate(e.lhs), self.toCommandPredicate(e.rhs))
      case e: ast.And => predicates.And(self.toCommandPredicate(e.lhs), self.toCommandPredicate(e.rhs))
      case e: ast.Ands => predicates.Ands(NonEmptyList.from(e.exprs.map(self.toCommandPredicate)))
      case e: ast.Ors => predicates.Ors(NonEmptyList.from(e.exprs.map(self.toCommandPredicate)))
      case e: ast.Not => predicates.Not(self.toCommandPredicate(e.rhs))
      case e: ast.Equals => predicates.Equals(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.NotEquals => predicates.Not(predicates.Equals(toCommandExpression(e.lhs), toCommandExpression(e.rhs)))
      case e: ast.RegexMatch => regexMatch(e)
      case e: ast.In => in(e)
      case e: ast.StartsWith => predicates.StartsWith(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.EndsWith => predicates.EndsWith(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.CoerceTo => commandexpressions.CoerceTo(toCommandExpression(e.expr), e.typ)
      case e: ast.Contains => predicates.Contains(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.IsNull => predicates.IsNull(toCommandExpression(e.lhs))
      case e: ast.IsNotNull => predicates.Not(predicates.IsNull(toCommandExpression(e.lhs)))
      case e: ast.InequalityExpression => inequalityExpression(e, self)
      case e: ast.Add => commandexpressions.Add(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.UnaryAdd => toCommandExpression(e.rhs)
      case e: ast.Subtract => commandexpressions.Subtract(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.UnarySubtract => commandexpressions.Subtract(commandexpressions.Literal(0), toCommandExpression(e.rhs))
      case e: ast.Multiply => commandexpressions.Multiply(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.Divide => commandexpressions.Divide(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.Modulo => commandexpressions.Modulo(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.Pow => commandexpressions.Pow(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
      case e: ast.FunctionInvocation => toCommandExpression(e.function, e, self)
      case e: ast.CountStar => commandexpressions.CountStar()
      case e: ast.Property => toCommandProperty(e)
      case e: ast.Parameter => toCommandParameter(e)
      case e: ast.CaseExpression => caseExpression(e, self)
      case e: ast.PatternExpression =>
        val legacyPatterns = e.pattern.asLegacyPatterns
        commands.PathExpression(legacyPatterns, predicates.True(), PathExtractorExpression(legacyPatterns), false)
      case e: ast.PatternComprehension => commands.PathExpression(e.pattern.asLegacyPatterns, toCommandPredicate(e.predicate, self), toCommandExpression(e.projection), true)
      case e: ast.ShortestPathExpression => commandexpressions.ShortestPathExpression(e.pattern.asLegacyPatterns(None).head)
      case e: ast.HasLabels => hasLabels(e)
      case e: ast.ListLiteral => commandexpressions.ListLiteral(toCommandExpression(e.expressions): _*)
      case e: ast.MapExpression => commandexpressions.LiteralMap(mapItems(e.items))
      case e: ast.ListSlice => commandexpressions.ListSlice(toCommandExpression(e.list), toCommandExpression(e.from), toCommandExpression(e.to))
      case e: ast.ContainerIndex => commandexpressions.ContainerIndex(toCommandExpression(e.expr), toCommandExpression(e.idx))
      case e: ast.FilterExpression => commandexpressions.FilterFunction(toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(self.toCommandPredicate).getOrElse(predicates.True()))
      case e: ast.ExtractExpression => commandexpressions.ExtractFunction(toCommandExpression(e.expression), e.variable.name, toCommandExpression(e.scope.extractExpression.get))
      case e: ast.ListComprehension => listComprehension(e, self)
      case e: ast.AllIterablePredicate => commands.AllInList(toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(self.toCommandPredicate).getOrElse(predicates.True()))
      case e: ast.AnyIterablePredicate => commands.AnyInList(toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(self.toCommandPredicate).getOrElse(predicates.True()))
      case e: ast.NoneIterablePredicate => commands.NoneInList(toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(self.toCommandPredicate).getOrElse(predicates.True()))
      case e: ast.SingleIterablePredicate => commands.SingleInList(toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(self.toCommandPredicate).getOrElse(predicates.True()))
      case e: ast.ReduceExpression => commandexpressions.ReduceFunction(toCommandExpression(e.list), e.variable.name, toCommandExpression(e.expression), e.accumulator.name, toCommandExpression(e.init))
      case e: ast.PathExpression => toCommandProjectedPath(e)
      case e: pipes.NestedPipeExpression => commandexpressions.NestedPipeExpression(e.pipe, toCommandExpression(e.projection))
      case e: ast.GetDegree => getDegree(e)
      case e: PrefixSeekRangeWrapper => commandexpressions.PrefixSeekRangeExpression(e.range.map(toCommandExpression))
      case e: InequalitySeekRangeWrapper => InequalitySeekRangeExpression(e.range.mapBounds(toCommandExpression))
      case e: ast.AndedPropertyInequalities =>
        val inequalities = e.inequalities.map(inequalityExpression(_, self))
        predicates.AndedPropertyComparablePredicates(variable(e.variable), toCommandProperty(e.property), inequalities)
      case e: DesugaredMapProjection => commandexpressions.DesugaredMapProjection(e.name.name, e.includeAllProps, mapProjectionItems(e.items))
      case e: ResolvedFunctionInvocation =>
        val callArgumentCommands = e.callArguments.map(Some(_)).zipAll(e.fcnSignature.get.inputSignature.map(_.default.map(_.value)), None, None).map {
          case (given, default) => given.map(toCommandExpression).getOrElse(commandexpressions.Literal(default.get))
        }
        val signature = e.fcnSignature.get
        if (signature.isAggregate) commandexpressions.AggregationFunctionInvocation(signature, callArgumentCommands)
        else commandexpressions.FunctionInvocation(signature, callArgumentCommands)
      case e: ast.MapProjection => throw new InternalException("should have been rewritten away")
      case e: NestedPlanExpression => commandexpressions.NestedPlanExpression(e.plan)
      case _ =>
        throw new InternalException(s"Unknown expression type during transformation (${expression.getClass})")
    }

  def toCommandParameter(e: ast.Parameter) =
    commandexpressions.ParameterExpression(e.name)

  override def toCommandProjectedPath(e: ast.PathExpression): ProjectedPath = {
    def project(pathStep: PathStep): Projector = pathStep match {

      case NodePathStep(Variable(node), next) =>
        singleNodeProjector(node, project(next))

      case SingleRelationshipPathStep(Variable(rel), SemanticDirection.INCOMING, next) =>
        singleIncomingRelationshipProjector(rel, project(next))

      case SingleRelationshipPathStep(Variable(rel), SemanticDirection.OUTGOING, next) =>
        singleOutgoingRelationshipProjector(rel, project(next))

      case SingleRelationshipPathStep(Variable(rel), SemanticDirection.BOTH, next) =>
        singleUndirectedRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Variable(rel), SemanticDirection.INCOMING, next) =>
        multiIncomingRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Variable(rel), SemanticDirection.OUTGOING, next) =>
        multiOutgoingRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Variable(rel), SemanticDirection.BOTH, next) =>
        multiUndirectedRelationshipProjector(rel, project(next))

      case NilPathStep =>
        nilProjector
    }

    val projector = project(e.step)
    val dependencies = e.step.dependencies.map(_.name)

    ProjectedPath(dependencies, projector)
  }

  private def toCommandExpression(expression: ast.Function, invocation: ast.FunctionInvocation, self: ExpressionConverters): CommandExpression =
    expression match {
      case Abs => commandexpressions.AbsFunction(toCommandExpression(invocation.arguments.head))
      case Acos => commandexpressions.AcosFunction(toCommandExpression(invocation.arguments.head))
      case Asin => commandexpressions.AsinFunction(toCommandExpression(invocation.arguments.head))
      case Atan => commandexpressions.AtanFunction(toCommandExpression(invocation.arguments.head))
      case Atan2 =>
        commandexpressions.Atan2Function(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1)))
      case Avg =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Avg(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Ceil => commandexpressions.CeilFunction(toCommandExpression(invocation.arguments.head))
      case Coalesce => commandexpressions.CoalesceFunction(toCommandExpression(invocation.arguments): _*)
      case Collect =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Collect(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Cos => commandexpressions.CosFunction(toCommandExpression(invocation.arguments.head))
      case Cot => commandexpressions.CotFunction(toCommandExpression(invocation.arguments.head))
      case Count =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Count(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Degrees => commandexpressions.DegreesFunction(toCommandExpression(invocation.arguments.head))
      case E => commandexpressions.EFunction()
      case EndNode => commandexpressions.RelationshipEndPoints(toCommandExpression(invocation.arguments.head), start = false)
      case Exists =>
        invocation.arguments.head match {
          case property: ast.Property =>
            commands.predicates.PropertyExists(toCommandExpression(property.map), PropertyKey(property.propertyKey.name))
          case expression: ast.PatternExpression =>
            self.toCommandPredicate(expression)
          case expression: pipes.NestedPipeExpression =>
            self.toCommandPredicate(expression)
          case e: ast.ContainerIndex =>
            commandexpressions.ContainerIndex(toCommandExpression(e.expr), toCommandExpression(e.idx))
          case e: NestedPlanExpression =>
            commands.expressions.NestedPlanExpression(e.plan)
        }
      case Exp => commandexpressions.ExpFunction(toCommandExpression(invocation.arguments.head))
      case Floor => commandexpressions.FloorFunction(toCommandExpression(invocation.arguments.head))
      case Haversin => commandexpressions.HaversinFunction(toCommandExpression(invocation.arguments.head))
      case Head =>
        commandexpressions.ContainerIndex(
          toCommandExpression(invocation.arguments.head),
          commandexpressions.Literal(0)
        )
      case Id => commandexpressions.IdFunction(toCommandExpression(invocation.arguments.head))
      case Keys => commandexpressions.KeysFunction(toCommandExpression(invocation.arguments.head))
      case Labels => commandexpressions.LabelsFunction(toCommandExpression(invocation.arguments.head))
      case Last =>
        commandexpressions.ContainerIndex(
          toCommandExpression(invocation.arguments.head),
          commandexpressions.Literal(-1)
        )
      case Left =>
        commandexpressions.LeftFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1))
        )
      case Length => commandexpressions.LengthFunction(toCommandExpression(invocation.arguments.head))
      case Log => commandexpressions.LogFunction(toCommandExpression(invocation.arguments.head))
      case Log10 => commandexpressions.Log10Function(toCommandExpression(invocation.arguments.head))
      case LTrim => commandexpressions.LTrimFunction(toCommandExpression(invocation.arguments.head))
      case Max =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Max(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Min =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Min(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Nodes => commandexpressions.NodesFunction(toCommandExpression(invocation.arguments.head))
      case PercentileCont =>
        val firstArg = toCommandExpression(invocation.arguments.head)
        val secondArg = toCommandExpression(invocation.arguments(1))

        val command = commandexpressions.PercentileCont(firstArg, secondArg)
        if (invocation.distinct)
          commandexpressions.Distinct(command, firstArg)
        else
          command
      case PercentileDisc =>
        val firstArg = toCommandExpression(invocation.arguments.head)
        val secondArg = toCommandExpression(invocation.arguments(1))

        val command = commandexpressions.PercentileDisc(firstArg, secondArg)
        if (invocation.distinct)
          commandexpressions.Distinct(command, firstArg)
        else
          command
      case Pi => commandexpressions.PiFunction()
      case Distance =>
        val firstArg = toCommandExpression(invocation.arguments.head)
        val secondArg = toCommandExpression(invocation.arguments(1))
        commandexpressions.DistanceFunction(firstArg, secondArg)
      case Point => commandexpressions.PointFunction(toCommandExpression(invocation.arguments.head))
      case Radians => commandexpressions.RadiansFunction(toCommandExpression(invocation.arguments.head))
      case Rand => commandexpressions.RandFunction()
      case functions.Range =>
        commandexpressions.RangeFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1)),
          toCommandExpression(invocation.arguments.lift(2)).getOrElse(commandexpressions.Literal(1))
        )
      case Relationships => commandexpressions.RelationshipFunction(toCommandExpression(invocation.arguments.head))
      case Replace =>
        commandexpressions.ReplaceFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1)),
          toCommandExpression(invocation.arguments(2))
        )
      case Reverse => commandexpressions.ReverseFunction(toCommandExpression(invocation.arguments.head))
      case Right =>
        commandexpressions.RightFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1))
        )
      case Round => commandexpressions.RoundFunction(toCommandExpression(invocation.arguments.head))
      case RTrim => commandexpressions.RTrimFunction(toCommandExpression(invocation.arguments.head))
      case Sign => commandexpressions.SignFunction(toCommandExpression(invocation.arguments.head))
      case Sin => commandexpressions.SinFunction(toCommandExpression(invocation.arguments.head))
      case Size => commandexpressions.SizeFunction(toCommandExpression(invocation.arguments.head))
      case Split =>
        commandexpressions.SplitFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1))
        )
      case Sqrt => commandexpressions.SqrtFunction(toCommandExpression(invocation.arguments.head))
      case StartNode => commandexpressions.RelationshipEndPoints(toCommandExpression(invocation.arguments.head), start = true)
      case StdDev =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Stdev(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case StdDevP =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.StdevP(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Substring =>
        commandexpressions.SubstringFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1)),
          toCommandExpression(invocation.arguments.lift(2))
        )
      case Sum =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Sum(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Tail =>
        commandexpressions.ListSlice(
          toCommandExpression(invocation.arguments.head),
          Some(commandexpressions.Literal(1)),
          None
        )
      case Tan => commandexpressions.TanFunction(toCommandExpression(invocation.arguments.head))
      case Timestamp => commandexpressions.TimestampFunction()
      case ToBoolean => commandexpressions.ToBooleanFunction(toCommandExpression(invocation.arguments.head))
      case ToFloat => commandexpressions.ToFloatFunction(toCommandExpression(invocation.arguments.head))
      case ToInteger => commandexpressions.ToIntegerFunction(toCommandExpression(invocation.arguments.head))
      case ToLower => commandexpressions.ToLowerFunction(toCommandExpression(invocation.arguments.head))
      case ToString => commandexpressions.ToStringFunction(toCommandExpression(invocation.arguments.head))
      case ToUpper => commandexpressions.ToUpperFunction(toCommandExpression(invocation.arguments.head))
      case Properties => commandexpressions.PropertiesFunction(toCommandExpression(invocation.arguments.head))
      case Trim => commandexpressions.TrimFunction(toCommandExpression(invocation.arguments.head))
      case Type => commandexpressions.RelationshipTypeFunction(toCommandExpression(invocation.arguments.head))
    }

  private def toCommandPredicate(e: Option[ast.Expression], self: ExpressionConverters): Predicate =
    e.map(self.toCommandPredicate).getOrElse(predicates.True())

  private def toCommandProperty(e: ast.Property): commandexpressions.Property =
    commandexpressions.Property(toCommandExpression(e.map), PropertyKey(e.propertyKey.name))

  private def toCommandExpression(expression: Option[ast.Expression]): Option[CommandExpression] =
    expression.map(toCommandExpression)

  private def toCommandExpression(expressions: Seq[ast.Expression]): Seq[CommandExpression] =
    expressions.map(toCommandExpression)

  private def variable(e: ast.Variable) = commands.expressions.Variable(e.name)

  private def inequalityExpression(original: ast.InequalityExpression, self: ExpressionConverters): predicates.ComparablePredicate = original match {
    case e: ast.LessThan =>
      val lexp = self.toCommandExpression(e.lhs)
      val rexp = self.toCommandExpression(e.rhs)
      predicates.LessThan(lexp, rexp)
    case e: ast.LessThanOrEqual => predicates.LessThanOrEqual(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.GreaterThan => predicates.GreaterThan(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.GreaterThanOrEqual => predicates.GreaterThanOrEqual(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
  }

  private def getDegree(original: ast.GetDegree) = {
    val typ = original.relType.map(relType => UnresolvedRelType(relType.name))
    commandexpressions.GetDegree(toCommandExpression(original.node), typ, original.dir)
  }

  private def regexMatch(e: ast.RegexMatch) = toCommandExpression(e.rhs) match {
    case literal: commandexpressions.Literal =>
      predicates.LiteralRegularExpression(toCommandExpression(e.lhs), literal)
    case command =>
      predicates.RegularExpression(toCommandExpression(e.lhs), command)
  }

  private def in(e: ast.In) = e.rhs match {
    case value: Parameter =>
      predicates.ConstantCachedIn(toCommandExpression(e.lhs), toCommandExpression(value))

    case value@ListLiteral(expressions) if expressions.isEmpty =>
      predicates.Not(predicates.True())

    case value@ListLiteral(expressions) if expressions.forall(_.isInstanceOf[Literal]) =>
      predicates.ConstantCachedIn(toCommandExpression(e.lhs), toCommandExpression(value))

    case _ =>
      predicates.DynamicCachedIn(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
  }

  private def caseExpression(e: ast.CaseExpression, self: ExpressionConverters) = e.expression match {
    case Some(innerExpression) =>
      val legacyAlternatives = e.alternatives.map { a => (toCommandExpression(a._1), toCommandExpression(a._2)) }
      commandexpressions.SimpleCase(toCommandExpression(innerExpression), legacyAlternatives, toCommandExpression(e.default))
    case None =>
      val predicateAlternatives = e.alternatives.map { a => (self.toCommandPredicate(a._1), toCommandExpression(a._2)) }
      commandexpressions.GenericCase(predicateAlternatives, toCommandExpression(e.default))
  }

  private def hasLabels(e: ast.HasLabels) = e.labels.map {
    l => predicates.HasLabel(toCommandExpression(e.expression), commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)): Predicate
  } reduceLeft {
    predicates.And(_, _)
  }

  private def mapItems(items: Seq[(PropertyKeyName, Expression)]): Map[String, CommandExpression] =
    items.map {
      case (id, ex) => id.name -> toCommandExpression(ex)
    }.toMap

  private def mapProjectionItems(items: Seq[LiteralEntry]): Map[String, CommandExpression] =
    items.map {
      case LiteralEntry(id, ex) => id.name -> toCommandExpression(ex)
    }.toMap

  private def listComprehension(e: ast.ListComprehension, self: ExpressionConverters): CommandExpression = {
    val filter = e.innerPredicate match {
      case Some(_: ast.True) | None =>
        toCommandExpression(e.expression)
      case Some(inner) =>
        commandexpressions.FilterFunction(toCommandExpression(e.expression), e.variable.name, self.toCommandPredicate(inner))
    }
    e.extractExpression match {
      case Some(extractExpression) =>
        commandexpressions.ExtractFunction(filter, e.variable.name, toCommandExpression(extractExpression))
      case None =>
        filter
    }
  }
}
