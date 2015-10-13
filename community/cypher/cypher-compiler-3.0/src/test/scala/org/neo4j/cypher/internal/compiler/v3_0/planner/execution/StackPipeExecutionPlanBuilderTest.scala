package org.neo4j.cypher.internal.compiler.v3_0.planner.execution

import org.neo4j.cypher.internal.compiler.v3_0.Monitors
import org.neo4j.cypher.internal.compiler.v3_0.pipes.{RonjaPipe, FakePipe, Pipe}
import org.neo4j.cypher.internal.compiler.v3_0.planner.execution.StackPipeExecutionPlanBuilderTest
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{IdName, LogicalLeafPlan, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_0.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.helpers.FakeClock

class StackPipeExecutionPlanBuilderTest extends CypherFunSuite {

  abstract class FakePlan extends LogicalPlan {
    override def lhs: Option[LogicalPlan] = None

    override def solved = ???

    override def availableSymbols = ???

    override def rhs: Option[LogicalPlan] = None

    override def mapExpressions(f: (Set[IdName], Expression) => Expression) = ???

    override def strictness = ???
  }

  abstract class FakeRonjaPipe extends FakePipe(Iterator.empty) with RonjaPipe {
    override def planDescriptionWithoutCardinality = ???

    override def withEstimatedCardinality(estimated: Double) = ???

    override def estimatedCardinality = ???
  }


  case class LeafPlan(name: String) extends FakePlan

  case class OneChildPlan(name: String, child: LogicalPlan) extends FakePlan {
    override def lhs = Some(child)
  }

  case class TwoChildPlan(name: String, l: LogicalPlan, r: LogicalPlan) extends FakePlan {
    override def rhs = Some(l)

    override def lhs = Some(r)
  }

  case class LeafPipe(name: String) extends FakeRonjaPipe

  case class OneChildPipe(name: String, src: Pipe) extends FakeRonjaPipe

  case class TwoChildPipe(name: String, l: Pipe, r: Pipe) extends FakeRonjaPipe


  val factory = new PipeBuilderFactory {
    override def apply(monitors: Monitors, recurse: LogicalPlan => Pipe)
                      (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder = new PipeBuilder {
      def build(plan: LogicalPlan) = plan match {
        case LeafPlan(n) => LeafPipe(n)
      }

      def build(plan: LogicalPlan, source: Pipe) = plan match {
        case OneChildPlan(name, _) => OneChildPipe(name, source)
      }

      def build(plan: LogicalPlan, lhs: Pipe, rhs: Pipe) = plan match {
        case TwoChildPlan(name, _, _) => TwoChildPipe(name, lhs, rhs)
      }
    }
  }

  val apa = new StackPipeExecutionPlanBuilder(new FakeClock, mock[Monitors], factory)


  test("apa") {
    /*
      a
      |
      b
     / \
    c   d*/


    val plan =
      OneChildPlan("a",
        TwoChildPlan("b",
          LeafPlan("c"),
          LeafPlan("d")
        )
      )

    val expectedPipe =
      OneChildPipe("a",
        TwoChildPipe("b",
          LeafPipe("c"),
          LeafPipe("d")
        )
      )

    val result = apa.build(plan)


    result should equal(expectedPipe)
  }
}
