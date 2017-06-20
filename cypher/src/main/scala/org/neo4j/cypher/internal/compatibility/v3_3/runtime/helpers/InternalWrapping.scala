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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers

import org.neo4j.cypher.internal.frontend
import org.neo4j.cypher.internal.frontend.v3_3.notification.{DeprecatedPlannerNotification, InternalNotification, PlannerUnsupportedNotification, RuntimeUnsupportedNotification, _}
import org.neo4j.graphdb
import org.neo4j.graphdb.impl.notification.{NotificationCode, NotificationDetail}
import scala.collection.JavaConverters._

object InternalWrapping {

  def asKernelNotification(notification: InternalNotification) = notification match {
    case DeprecatedStartNotification(pos, message) =>
      NotificationCode.START_DEPRECATED.notification(pos.asInputPosition, NotificationDetail.Factory.startDeprecated(message))
    case CartesianProductNotification(pos, variables) =>
      NotificationCode.CARTESIAN_PRODUCT.notification(pos.asInputPosition, NotificationDetail.Factory.cartesianProduct(variables.asJava))
    case LengthOnNonPathNotification(pos) =>
      NotificationCode.LENGTH_ON_NON_PATH.notification(pos.asInputPosition)
    case PlannerUnsupportedNotification =>
      NotificationCode.PLANNER_UNSUPPORTED.notification(graphdb.InputPosition.empty)
    case RuntimeUnsupportedNotification =>
      NotificationCode.RUNTIME_UNSUPPORTED.notification(graphdb.InputPosition.empty)
    case IndexHintUnfulfillableNotification(label, propertyKeys) =>
      NotificationCode.INDEX_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.index(label, propertyKeys: _*))
    case JoinHintUnfulfillableNotification(variables) =>
      NotificationCode.JOIN_HINT_UNFULFILLABLE.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case JoinHintUnsupportedNotification(variables) =>
      NotificationCode.JOIN_HINT_UNSUPPORTED.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.joinKey(variables.asJava))
    case IndexLookupUnfulfillableNotification(labels) =>
      NotificationCode.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(graphdb.InputPosition.empty, NotificationDetail.Factory.indexSeekOrScan(labels.asJava))
    case EagerLoadCsvNotification =>
      NotificationCode.EAGER_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case LargeLabelWithLoadCsvNotification =>
      NotificationCode.LARGE_LABEL_LOAD_CSV.notification(graphdb.InputPosition.empty)
    case MissingLabelNotification(pos, label) =>
      NotificationCode.MISSING_LABEL.notification(pos.asInputPosition, NotificationDetail.Factory.label(label))
    case MissingRelTypeNotification(pos, relType) =>
      NotificationCode.MISSING_REL_TYPE.notification(pos.asInputPosition, NotificationDetail.Factory.relationshipType(relType))
    case MissingPropertyNameNotification(pos, name) =>
      NotificationCode.MISSING_PROPERTY_NAME.notification(pos.asInputPosition, NotificationDetail.Factory.propertyName(name))
    case UnboundedShortestPathNotification(pos) =>
      NotificationCode.UNBOUNDED_SHORTEST_PATH.notification(pos.asInputPosition)
    case ExhaustiveShortestPathForbiddenNotification(pos) =>
      NotificationCode.EXHAUSTIVE_SHORTEST_PATH.notification(pos.asInputPosition)
    case DeprecatedFunctionNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_FUNCTION.notification(pos.asInputPosition, NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedProcedureNotification(pos, oldName, newName) =>
      NotificationCode.DEPRECATED_PROCEDURE.notification(pos.asInputPosition, NotificationDetail.Factory.deprecatedName(oldName, newName))
    case DeprecatedFieldNotification(pos, procedure, field) =>
      NotificationCode.DEPRECATED_PROCEDURE_RETURN_FIELD.notification(pos.asInputPosition, NotificationDetail.Factory.deprecatedField(procedure, field))
    case DeprecatedVarLengthBindingNotification(pos, variable) =>
      NotificationCode.DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP.notification(pos.asInputPosition, NotificationDetail.Factory.bindingVarLengthRelationship(variable))
    case DeprecatedRelTypeSeparatorNotification(pos) =>
      NotificationCode.DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(pos.asInputPosition)
    case DeprecatedPlannerNotification =>
      NotificationCode.DEPRECATED_PLANNER.notification(graphdb.InputPosition.empty)
    case ProcedureWarningNotification(pos, name, warning) =>
      NotificationCode.PROCEDURE_WARNING.notification(pos.asInputPosition, NotificationDetail.Factory.procedureWarning(name, warning))
  }

  private implicit class ConvertibleCompilerInputPosition(pos: frontend.v3_3.InputPosition) {
    def asInputPosition = new graphdb.InputPosition(pos.offset, pos.line, pos.column)
  }
}
