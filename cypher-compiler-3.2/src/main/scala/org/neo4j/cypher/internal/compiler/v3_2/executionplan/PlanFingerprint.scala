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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_2.spi.{GraphStatistics, GraphStatisticsSnapshot}
import java.text.SimpleDateFormat
import java.util.Date

case class PlanFingerprint(creationTimeMillis: Long, lastCheckTimeMillis: Long, txId: Long, snapshot: GraphStatisticsSnapshot)

class PlanFingerprintReference(clock: Clock, divergence: StatsDivergenceCalculator,
                               private var fingerprint: Option[PlanFingerprint]) {

  def isStale(lastCommittedTxId: () => Long, statistics: GraphStatistics): Boolean = {
    fingerprint.fold({PlanFingerprint.log2("NO  - No fingerprint"); false}) { f =>
      lazy val currentTimeMillis = clock.millis()
      lazy val currentTxId = lastCommittedTxId()

      PlanFingerprint.log2(s"Creation:${f.creationTimeMillis} TTL:${divergence.initialMillis} Current:$currentTimeMillis currentTx:$currentTxId")

      if (divergence.tooSoon(currentTimeMillis, f.lastCheckTimeMillis)) {
        PlanFingerprint.log2("NO  - Shorter than minimal TTL")
        return false
      }
      if(!(check(currentTxId != f.txId,
        () => { fingerprint = Some(f.copy(lastCheckTimeMillis = currentTimeMillis)) }))) {
        PlanFingerprint.log2(s"NO  - Same txID. Setting LastCheckTime to $currentTimeMillis")
        return false
      }
      if(!(check(f.snapshot.diverges(f.snapshot.recompute(statistics), divergence.decay(currentTimeMillis - f.creationTimeMillis)),
        () => { fingerprint = Some(f.copy(lastCheckTimeMillis = currentTimeMillis, txId = currentTxId)) }))) {
        PlanFingerprint.log2(s"NO  - Statistics don't diverge. Setting LastCheckTime to $currentTimeMillis. Setting txID to $currentTxId")
        return false
      }
      PlanFingerprint.log2("YES")
      true
    }
  }

  private def check(test: => Boolean, ifFalse: () => Unit ) = if (test) { true } else { ifFalse() ; false }
}

trait StatsDivergenceCalculator {
  val initialThreshold: Double
  val initialMillis: Long

  def tooSoon(currentTimeMillis: Long, lastCheckTimeMillis: Long) = currentTimeMillis - initialMillis < lastCheckTimeMillis

  def decay(millisSincePreviousReplan: Long): Double
}

case class StatsDivergenceInverseDecayCalculator(initialThreshold: Double, targetThreshold: Double, initialMillis: Long, targetMillis: Long) extends StatsDivergenceCalculator {
  val decayFactor: Double = (initialThreshold / targetThreshold - 1.0) / (targetMillis - initialMillis)

  def decay(millisSincePreviousReplan: Long): Double = {
    // Note that this equation has a possible singularity for very steep decays, when millisSincePreviousReplan < initialMillis
    // However, that will never happen because of the 'tooSoon' test above
    initialThreshold / (1.0 + decayFactor * (millisSincePreviousReplan - initialMillis))
  }
}

case class StatsDivergenceExponentialDecayCalculator(initialThreshold: Double, targetThreshold: Double, initialMillis: Long, targetMillis: Long) extends StatsDivergenceCalculator {
  val decayFactor: Double = (Math.log(initialThreshold) - Math.log(targetThreshold)) / (targetMillis - initialMillis)

  def decay(millisSincePreviousReplan: Long): Double = {
    val exponent = -1.0 * decayFactor * (millisSincePreviousReplan - initialMillis)
    initialThreshold * Math.pow(Math.E, exponent)
  }
}

case class StatsDivergenceNoDecayCalculator(initialThreshold: Double, initialMillis: Long) extends StatsDivergenceCalculator {
  def decay(millisSinceThreshold: Long): Double = {
    initialThreshold
  }
}

object PlanFingerprint {
  val inverse = "inverse"
  val exponential = "exponential"
  val none = "none"
  val similarityTolerance = 0.0001

  def divergenceCalculatorFor(name: String, initialThreshold: Double, targetThreshold: Double, initialMillis: Long, targetMillis: Long): StatsDivergenceCalculator = {
    if (targetThreshold <= similarityTolerance || initialThreshold - targetThreshold <= similarityTolerance || targetMillis <= initialMillis) {
      // Input values that disable the threshold decay algorithm
      StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis)
    } else {
      // Input is valid, select decay algorithm, the GraphDatabaseSettings will limit the possible values
      name.toLowerCase match {
        case "none" => StatsDivergenceNoDecayCalculator(initialThreshold, initialMillis)
        case "exponential" => StatsDivergenceExponentialDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
        case _ => StatsDivergenceInverseDecayCalculator(initialThreshold, targetThreshold, initialMillis, targetMillis)
      }
    }
  }

  def divergenceNoDecayCalculator(threshold: Double, ttl: Long) =
    StatsDivergenceNoDecayCalculator(threshold, ttl)

  def apply(creationTimeMillis: Long, txId: Long, snapshot: GraphStatisticsSnapshot): PlanFingerprint =
    PlanFingerprint(creationTimeMillis, creationTimeMillis, txId, snapshot)

  //TODO: Remove logging
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //Or whatever format fits best your needs.

  def log3(msg: String): Unit = {
    log2(s"\t$msg")
  }

  def log2(msg: String): Unit = {
    log(s"\t$msg")
  }

  def log(msg: String): Unit = {
    val date = sdf.format(new Date)
    println(s"$date\t$msg")
  }
}