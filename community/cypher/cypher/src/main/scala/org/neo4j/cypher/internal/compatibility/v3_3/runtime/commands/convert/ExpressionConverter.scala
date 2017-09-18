package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{ProjectedPath, Expression => CommandExpression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.SeekArgs
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.frontend.v3_3.ast
import org.neo4j.cypher.internal.frontend.v3_3.ast.PathExpression

trait ExpressionConverter {

  def toCommandExpression(expression: ast.Expression): CommandExpression

  def toCommandPredicate(in: ast.Expression): Predicate

  def toCommandPredicate(e: Option[ast.Expression]): Predicate

  def toCommandSeekArgs(seek: SeekableArgs): SeekArgs

  def toCommandProjectedPath(e: PathExpression): ProjectedPath
}
