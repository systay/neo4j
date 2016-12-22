package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.AST_REWRITE

object ExtractParameters extends Phase {
  override def phase = AST_REWRITE

  override def description: String = ???

  override def process(from: CompilationState, context: Context): CompilationState = ???

  override def postConditions: Set[Condition] = ???
}
