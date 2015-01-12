package org.neo4j.cypher.internal.compiler.v2_2.helpers

import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable

object SemanticTableHelper {
  implicit class RichSemanticTable(table: SemanticTable) {
    def transplantResolutionOnto(target: SemanticTable) =
      target.copy(
        resolvedLabelIds = table.resolvedLabelIds,
        resolvedPropertyKeyNames = table.resolvedPropertyKeyNames,
        resolvedRelTypeNames = table.resolvedRelTypeNames
      )
  }
}
