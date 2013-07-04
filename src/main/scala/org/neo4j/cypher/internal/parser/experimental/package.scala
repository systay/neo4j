/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser

package object experimental {
  type SemanticCheck = SemanticState => SemanticCheckResult

  // Allows joining of two (SemanticState => SemanticCheckResult) funcs together (using then)
  implicit def chainableSemanticCheck(check: SemanticCheck) = ChainableSemanticCheck(check)
  // Allows joining of a (SemanticState => SemanticCheckResult) func to a (SemanticState => Either[SemanticError, SemanticState]) func
  implicit def chainableSemanticEitherFunc(func: SemanticState => Either[SemanticError, SemanticState]) = ChainableSemanticCheck(func)
  // Allows joining of a (SemanticState => SemanticCheckResult) func to a (SemanticState => Option[SemanticError]) func
  implicit def chainableSemanticOptionFunc(func: SemanticState => Option[SemanticError]) = ChainableSemanticCheck(func)

  // Allows using a (SemanticState => Either[SemanticError, SemanticState]) func, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticEitherFunc(func: SemanticState => Either[SemanticError, SemanticState]) : SemanticCheck = state => {
    func(state).fold(error => SemanticCheckResult.error(state, error), s => SemanticCheckResult.success(s))
  }
  // Allows using a (SemanticState => Seq[SemanticError]) func, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrorsFunc(func: SemanticState => Seq[SemanticError]) : SemanticCheck = state => {
    SemanticCheckResult(state, func(state))
  }
  // Allows using a (SemanticState => Option[SemanticError]) func, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrorFunc(func: SemanticState => Option[SemanticError]) : SemanticCheck = state => {
    SemanticCheckResult.error(state, func(state))
  }

  // Allows using a sequence of SemanticError, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrors(errors: Seq[SemanticError]) : SemanticCheck = SemanticCheckResult(_, errors)
  // Allows using a single SemanticError, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticError(error: SemanticError) : SemanticCheck = SemanticCheckResult.error(_, error)
  // Allows using an optional SemanticError, where a (SemanticState => SemanticCheckResult) func is expected
  implicit def liftSemanticErrorOption(error: Option[SemanticError]) : SemanticCheck = SemanticCheckResult.error(_, error)

  // Allows starting with a sequence of SemanticError, and joining to a (SemanticState => SemanticCheckResult) func (using then)
  implicit def liftSemanticErrorsAndChain(errors: Seq[SemanticError]) : ChainableSemanticCheck = liftSemanticErrors(errors)
  // Allows starting with a single SemanticError, and joining to a (SemanticState => SemanticCheckResult) func (using then)
  implicit def liftSemanticErrorAndChain(error: SemanticError) : ChainableSemanticCheck = liftSemanticError(error)
  // Allows starting with an optional SemanticError, and joining to a (SemanticState => SemanticCheckResult) func (using then)
  implicit def liftSemanticErrorOptionAndChain(error: Option[SemanticError]) : ChainableSemanticCheck = liftSemanticErrorOption(error)

  // Allows calling semanticCheck on an optional SemanticCheckable object
  implicit def semanticCheckableOption[A <: SemanticCheckable](option: Option[A]) = SemanticCheckableOption(option)
  // Allows calling semanticCheck on a traversable sequence of SemanticCheckable objects
  implicit def semanticCheckableTraversableOnce[A <: SemanticCheckable](traversable: TraversableOnce[A]) = SemanticCheckableTraversableOnce(traversable)
}
