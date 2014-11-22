/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.execution

import java.util.Date

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.OtherConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.commands.StatementConverters
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Expression, Identifier, NodeStartItem, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{AggregationExpression, Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{EntityProducerFactory, True, Predicate => CommandPredicate}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders.prepare.KeyTokenResolver
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{PipeInfo, PlanFingerprint}
import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityInput
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{CantHandleQueryException, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{InstrumentedGraphStatistics, PlanContext}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable
import org.neo4j.cypher.internal.helpers.Eagerly
import org.neo4j.graphdb.Relationship

case class PipeExecutionBuilderContext(cardinality: Metrics.CardinalityModel, semanticTable: SemanticTable)

class PipeExecutionPlanBuilder(monitors: Monitors) {

  val entityProducerFactory = new EntityProducerFactory
  val resolver = new KeyTokenResolver

  def build(plan: LogicalPlan)(implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeInfo = {
    val updating = false


    def pipeWithCleanRowSpec(plan: LogicalPlan, input: QueryGraphCardinalityInput) = buildPipe(plan, Some(RowSpec.from(plan)), input)

    def buildPipe(plan: LogicalPlan, rowSpec: Option[RowSpec], input: QueryGraphCardinalityInput): Pipe = {
      implicit val monitor = monitors.newMonitor[PipeMonitor]()
      implicit val c = context.cardinality

      def getOrCreateSpec(p: LogicalPlan) = rowSpec.getOrElse(RowSpec.from(p))

      val result: Pipe with RonjaPipe = plan match {
        case Projection(left, expressions) =>
          ProjectionNewPipe(buildPipe(left, Some(RowSpec.from(left)), input), Eagerly.immutableMapValues(expressions, buildExpression))()

        case ProjectEndpoints(left, rel, start, end, directed, length) =>
          ProjectEndpointsPipe(buildPipe(left, Some(RowSpec.from(left)), input), rel.name, start.name, end.name, directed, length.isSimple)()

        case sr @ SingleRow() =>
          SingleRowPipe()

        case sr @ Argument(ids) =>
          ArgumentPipe(new SymbolTable(sr.typeInfo))()

        case AllNodesScan(IdName(id), _) =>
          val rowSpec = getOrCreateSpec(plan)
          AllNodesScanPipe(id, rowSpec, rowSpec.indexToNode(id))()

        case NodeByLabelScan(IdName(id), label, _) =>
          val rowSpec = getOrCreateSpec(plan)
          NodeByLabelScanPipe(id, label, rowSpec, rowSpec.indexToNode(id))()

        case NodeByIdSeek(IdName(id), nodeIdExpr, _) =>
          val rowSpec = getOrCreateSpec(plan)
          NodeByIdSeekPipe(id, nodeIdExpr.asEntityByIdRhs, rowSpec, rowSpec.indexToNode(id))()

        case DirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode), _) =>
          DirectedRelationshipByIdSeekPipe(id, relIdExpr.asEntityByIdRhs, toNode, fromNode)()

        case UndirectedRelationshipByIdSeek(IdName(id), relIdExpr, IdName(fromNode), IdName(toNode), _) =>
          UndirectedRelationshipByIdSeekPipe(id, relIdExpr.asEntityByIdRhs, toNode, fromNode)()

        case NodeIndexSeek(IdName(id), label, propertyKey, valueExpr, _) =>
          NodeIndexSeekPipe(id, label, propertyKey, valueExpr.map(buildExpression), unique = false)()

        case NodeIndexUniqueSeek(IdName(id), label, propertyKey, valueExpr, _) =>
          NodeIndexSeekPipe(id, label, propertyKey, valueExpr.map(buildExpression), unique = true)()

        case Selection(predicates, left) =>
          FilterPipe(buildPipe(left, Some(getOrCreateSpec(left)), input), predicates.map(buildPredicate).reduce(_ ++ _))()

        case CartesianProduct(left, right) =>
          CartesianProductPipe(buildPipe(left, Some(RowSpec.from(left)), input), buildPipe(right, Some(RowSpec.from(right)), input))()

        case Expand(left, IdName(fromName), dir, projectedDir, types: Seq[RelTypeName], IdName(toName), IdName(relName), SimplePatternLength, _) =>
          implicit val table: SemanticTable = context.semanticTable
          val rowSpec = getOrCreateSpec(plan)
          val from = rowSpec.indexToNode(fromName)
          val rel = rowSpec.indexToRel(relName)
          val to = rowSpec.indexToNode(toName)
          if (types.exists(_.id == None))
            ExpandPipeForStringTypes(buildPipe(left, Some(rowSpec), input), from, fromName, rel, to, relName, toName, dir, types.map(_.name))()
          else {
            ExpandPipeForIntTypes(buildPipe(left, Some(rowSpec), input), from, fromName, rel, to, relName, toName, dir, types.flatMap(_.id).map(_.id))()
          }

        case Expand(left, IdName(fromName), dir, projectedDir, types, IdName(toName), IdName(relName), VarPatternLength(min, max), predicates) =>
          val (keys, exprs) = predicates.unzip
          val commands = exprs.map(buildPredicate)
          val rowSpec = Some(getOrCreateSpec(plan))
          val predicate = (context: ExecutionContext, state: QueryState, rel: Relationship) => {
            keys.zip(commands).forall { case (identifier: Identifier, expr: CommandPredicate) =>
              context(identifier.name) = rel
              val result = expr.isTrue(context)(state)
              context.remove(identifier.name)
              result
            }
          }

          implicit val table: SemanticTable = context.semanticTable

          if (types.exists(_.id == None))
            VarLengthExpandPipeForStringTypes(buildPipe(left, rowSpec, input), fromName, relName, toName, dir, projectedDir, types.map(_.name), min, max, predicate)()
          else
            VarLengthExpandPipeForIntTypes(buildPipe(left, rowSpec, input), fromName, relName, toName, dir, projectedDir, types.flatMap(_.id).map(_.id), min, max, predicate)()

        case OptionalExpand(left, IdName(fromName), dir, types, IdName(toName), IdName(relName), SimplePatternLength, predicates) =>
          val predicate = predicates.map(buildPredicate).reduceOption(_ ++ _).getOrElse(True())
          OptionalExpandPipe(buildPipe(left, Some(getOrCreateSpec(left)), input), fromName, relName, toName, dir, types.map(_.name), predicate)()

        case NodeHashJoin(nodes, left, right) =>
          val rightPipe = buildPipe(right, Some(RowSpec.from(right)), input)
          val leftPipe: Pipe = buildPipe(left, Some(RowSpec.from(left)), input)
          NodeHashJoinPipe(nodes.map(_.name), leftPipe, rightPipe)()

        case OuterHashJoin(nodes, left, right) =>
          val nullableIdentifiers = (right.availableSymbols -- left.availableSymbols).map(_.name)
          val rightPipe = buildPipe(right, Some(RowSpec.from(right)), input)
          val leftPipe: Pipe = buildPipe(left, Some(RowSpec.from(left)), input)
          NodeOuterHashJoinPipe(nodes.map(_.name), leftPipe, rightPipe, nullableIdentifiers)()

        case Optional(inner) =>
          OptionalPipe(inner.availableSymbols.map(_.name), buildPipe(inner, Some(getOrCreateSpec(inner)), input))()

        case Apply(outer, inner) =>
          ApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(outer, input.recurse(outer)))()

        case SemiApply(outer, inner) =>
          SemiApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(inner, input.recurse(outer)), negated = false)()

        case AntiSemiApply(outer, inner) =>
          SemiApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(outer, input.recurse(outer)), negated = true)()

        case LetSemiApply(outer, inner, idName) =>
          LetSemiApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(outer, input.recurse(outer)), idName.name, negated = false)()

        case LetAntiSemiApply(outer, inner, idName) =>
          LetSemiApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(outer, input.recurse(outer)), idName.name, negated = true)()

        case apply@SelectOrSemiApply(outer, inner, predicate) =>
          SelectOrSemiApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(outer, input.recurse(outer)), buildPredicate(predicate), negated = false)()

        case apply@SelectOrAntiSemiApply(outer, inner, predicate) =>
          SelectOrSemiApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(outer, input.recurse(outer)), buildPredicate(predicate), negated = true)()

        case apply@LetSelectOrSemiApply(outer, inner, idName, predicate) =>
          LetSelectOrSemiApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(outer, input.recurse(outer)), idName.name, buildPredicate(predicate), negated = false)()

        case apply@LetSelectOrAntiSemiApply(outer, inner, idName, predicate) =>
          LetSelectOrSemiApplyPipe(pipeWithCleanRowSpec(outer, input), pipeWithCleanRowSpec(outer, input.recurse(outer)), idName.name, buildPredicate(predicate), negated = true)()

        case Sort(left, sortItems) =>
          SortPipe(buildPipe(left, Some(getOrCreateSpec(left)), input), sortItems)()

        case Skip(lhs, count) =>
          SkipPipe(buildPipe(lhs, Some(getOrCreateSpec(lhs)), input), buildExpression(count))()

        case Limit(lhs, count) =>
          LimitPipe(buildPipe(lhs, Some(getOrCreateSpec(lhs)), input), buildExpression(count))()

        case SortedLimit(lhs, exp, sortItems) =>
          TopPipe(buildPipe(lhs, Some(getOrCreateSpec(lhs)), input), sortItems.map(_.asCommandSortItem).toList, exp.asCommandExpression)()

        // TODO: Maybe we shouldn't encode distinct as an empty aggregation.
        case Aggregation(Projection(source, expressions), groupingExpressions, aggregatingExpressions)
          if aggregatingExpressions.isEmpty && expressions == groupingExpressions =>
          DistinctPipe(buildPipe(source, Some(getOrCreateSpec(source)), input), groupingExpressions.mapValues(_.asCommandExpression))()

        case Aggregation(source, groupingExpressions, aggregatingExpressions) if aggregatingExpressions.isEmpty =>
          DistinctPipe(buildPipe(source, Some(getOrCreateSpec(source)), input), groupingExpressions.mapValues(_.asCommandExpression))()

        case Aggregation(source, groupingExpressions, aggregatingExpressions) =>
          EagerAggregationPipe(
            buildPipe(source, Some(getOrCreateSpec(source)), input),
            Eagerly.immutableMapValues[String, ast.Expression, commands.expressions.Expression](groupingExpressions, x => x.asCommandExpression),
            Eagerly.immutableMapValues[String, ast.Expression, AggregationExpression](aggregatingExpressions, x => x.asCommandExpression.asInstanceOf[AggregationExpression])
          )()

        case FindShortestPaths(source, shortestPath) =>
          val legacyShortestPaths = shortestPath.expr.asLegacyPatterns(shortestPath.name.map(_.name))
          val legacyShortestPath = legacyShortestPaths.head
          new ShortestPathPipe(buildPipe(source, Some(getOrCreateSpec(source)), input), legacyShortestPath)()

        case Union(lhs, rhs) =>
          NewUnionPipe(pipeWithCleanRowSpec(lhs, input), pipeWithCleanRowSpec(rhs, input))()

        case UnwindCollection(lhs, identifier, collection) =>
          UnwindPipe(buildPipe(lhs, Some(getOrCreateSpec(lhs)), input), collection.asCommandExpression, identifier.name)()

        case LegacyIndexSeek(id, hint: NodeStartItem, _) =>
          val source = new SingleRowPipe()
          val ep = entityProducerFactory.nodeStartItems((planContext, StatementConverters.StartItemConverter(hint).asCommandStartItem))
          NodeStartPipe(source, id.name, ep)()

        case x =>
          throw new CantHandleQueryException(x.toString)
      }

      val cardinality = context.cardinality(plan, input)
      result.withEstimatedCardinality(cardinality.amount.toLong)
    }

    object buildPipeExpressions extends Rewriter {
      val instance = Rewriter.lift {
        case ast.NestedPlanExpression(patternPlan, pattern) =>
          val pos = pattern.position
          val pipe = buildPipe(patternPlan, None, QueryGraphCardinalityInput.empty)
          val step = projectNamedPaths.patternPartPathExpression(ast.EveryPath(pattern.pattern.element))
          val result = ast.NestedPipeExpression(pipe, ast.PathExpression(step)(pos))(pos)
          result
      }

      def apply(that: AnyRef): AnyRef = bottomUp(instance).apply(that)
    }

    def buildExpression(expr: ast.Expression): CommandExpression = {
      val rewrittenExpr = expr.endoRewrite(buildPipeExpressions)

      rewrittenExpr.asCommandExpression.rewrite(resolver.resolveExpressions(_, planContext))
    }

    def buildPredicate(expr: ast.Expression): CommandPredicate = {
      val rewrittenExpr: Expression = expr.endoRewrite(buildPipeExpressions)

      rewrittenExpr.asCommandPredicate.rewrite(resolver.resolveExpressions(_, planContext)).asInstanceOf[CommandPredicate]
    }


    val topLevelPipe = buildPipe(plan, None, QueryGraphCardinalityInput.empty)

    val fingerprint = planContext.statistics match {
      case igs: InstrumentedGraphStatistics =>
        Some(PlanFingerprint(new Date(), planContext.getLastCommittedTransactionId, igs.snapshot.freeze))
      case _ =>
        None
    }
    PipeInfo(topLevelPipe, updating, None, fingerprint, Ronja)
  }
}
