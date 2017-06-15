package org.neo4j.cypher.internal.enterprise_interpreted_runtime.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState

case class NodeFromRegister(offset: Int) extends Expression{
  override def rewrite(f: (Expression) => Expression): Expression = f(this)

  override def arguments: Seq[Expression] = Seq.empty

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val nodeId = ctx.getLong(offset)
    state.query.nodeOps.getById(nodeId)
  }

  override def symbolTableDependencies: Set[String] = Set.empty
}
