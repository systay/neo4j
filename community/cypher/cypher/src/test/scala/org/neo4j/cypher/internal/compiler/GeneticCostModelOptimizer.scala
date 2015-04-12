package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compatibility.WrappedMonitors2_3
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_3.executionplan._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CachedFunction
import org.neo4j.cypher.internal.compiler.v2_3.parser.{CypherParser, ParserMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CostBasedExecutablePlanBuilder, CostBasedPipeBuilderFactory, PlanningMonitor, UnionQuery}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.internal.{LRUCache, ProfileMode}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.helpers.Clock
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.util.Random

class GeneticCostModelOptimizer extends CypherFunSuite with GraphIcing {

  val MUTATION_CHANCE = .05
  val r = new Random()

  def maybeMutate(in: CostPerRow) = if (r.nextDouble() < MUTATION_CHANCE)
    CostPerRow(in.cost * r.nextDouble() * 2)
  else
    in

  def maybeMutate(in: Multiplier) = if (r.nextDouble() < MUTATION_CHANCE)
    Multiplier(in.coefficient * r.nextDouble() * 2)
  else
    in

  def either[T](a: T, b: T) = if (r.nextBoolean()) a else b

  case class Genome(CPU_BOUND: CostPerRow = 0.1,
                    FAST_STORE: CostPerRow = 1.0,
                    SLOW_STORE: CostPerRow = 10.0,
                    PROBE_BUILD_COST: CostPerRow = 1.0,
                    PROBE_SEARCH_COST: CostPerRow = .5,
                    EAGERNESS_MULTIPLIER: Multiplier = 3.0) {
    def clone(count: Int): Seq[Genome] = for (i <- 0 to count) yield {
      new Genome(maybeMutate(CPU_BOUND),
        maybeMutate(FAST_STORE),
        maybeMutate(SLOW_STORE),
        maybeMutate(PROBE_BUILD_COST),
        maybeMutate(PROBE_SEARCH_COST),
        maybeMutate(EAGERNESS_MULTIPLIER)
      )
    }

    def mateWith(other: Genome) =
      new Genome(
        maybeMutate(either(CPU_BOUND, other.CPU_BOUND)),
        maybeMutate(either(FAST_STORE, other.FAST_STORE)),
        maybeMutate(either(SLOW_STORE, other.SLOW_STORE)),
        maybeMutate(either(PROBE_BUILD_COST, other.PROBE_BUILD_COST)),
        maybeMutate(either(PROBE_SEARCH_COST, other.PROBE_SEARCH_COST)),
        maybeMutate(either(EAGERNESS_MULTIPLIER, other.EAGERNESS_MULTIPLIER))
      )
  }

  class GeneticMetricsFactory(genome: Genome) extends MetricsFactory {
    def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
      new StatisticsBackedCardinalityModel(queryGraphCardinalityModel)

    def newCostModel() = CardinalityCostModel(genome.CPU_BOUND, genome.FAST_STORE, genome.SLOW_STORE, genome.PROBE_BUILD_COST, genome.PROBE_SEARCH_COST, genome.EAGERNESS_MULTIPLIER)

    def newQueryGraphCardinalityModel(statistics: GraphStatistics) =
      QueryGraphCardinalityModel.default(statistics)
  }

  private def testFitness(db: GraphDatabaseAPI, params: Map[FileName, Map[ParamKey, Any]], queries: Seq[(ParamKey, ParamKey)])(genome: Genome): Long = {
    val compiler = ronjaCompiler(genome)(db)

    def testQuery(query: String, fileName: FileName) = {

      val (plan: ExecutionPlan, parameters) = db.withTx {
        tx =>
          val planContext = new TransactionBoundPlanContext(db.statement, db)
          compiler.planQuery(query, planContext, devNullLogger)
      }

      val dbHits = db.withTx {
        tx =>
          val queryContext = new TransactionBoundQueryContext(db, tx, true, db.statement)
          val result = plan.run(queryContext, db.statement, ProfileMode, parameters ++ params(fileName))
          result.size
          result.executionPlanDescription().totalDbHits.get
      }

      dbHits
    }
    val map = queries.map {
      case (query, fileName) => testQuery(query, fileName)
    }.toList
    val total = map.sum
//    println(s"$total -> $genome")
    total
  }

  private def nextGeneration(parents: Seq[Genome]): Seq[Genome] = {
    val clones = (for (ignored <- 1 to 5;
                       l <- parents;
                       r <- parents if l != r) yield l.mateWith(r)).distinct

    val size = 50

    def cloneUntil100(in: Seq[Genome]): Seq[Genome] = if (in.size >= size)
      in.take(size)
    else
      cloneUntil100(in.flatMap((x: Genome) => x.clone(2))).distinct

    cloneUntil100(clones ++ parents)
  }

  type FileName = String
  type ParamKey = String
  type ParamValue = String

  def dparamsFiles(queries: Seq[(String, String)]): Map[FileName, List[Map[ParamKey, Any]]] = {
    val fileNames: Seq[FileName] =
      queries.map(_._2)

    val fileNamesAndFileContent =
      fileNames.map { f => if (f == "NONE") f -> List.empty
      else {
        val lines: Iterator[String] = scala.io.Source.fromFile(f).getLines()
        val next = lines.next()
        val keys: Array[ParamKey] = next.split('|')
        val result: List[Map[ParamKey, Any]] = lines.map(l => {
          val paramKeyToValue: Map[ParamKey, Any] = (keys zip l.split('|')).map {
            case (k, v) if k.endsWith(":Long") => k.replace(":Long", "") -> v.toLong
            case (k, v) => k -> v
          }.toMap
          paramKeyToValue
        }).toList

        f -> result
      }
      }.toMap

    fileNamesAndFileContent
  }

  // Pick a random param from each file
  private def getRandomParams(queries: Seq[(String, String)]): Map[FileName, Map[ParamKey, Any]] =
    dparamsFiles(queries).map {
      case (k, params: List[Map[ParamKey, Any]]) =>
        val newV = if (params.isEmpty) Map.empty[FileName, Map[ParamKey, Any]]
        else
          params.apply(r.nextInt(params.length))

        k -> newV
    }


  def knownGoodStartingPoints = nextGeneration(Seq(
    Genome(CostPerRow(6.926715054967385), CostPerRow(1.70978920816183), CostPerRow(0.002659392740993439), CostPerRow(0.04327718042941619), CostPerRow(0.015126098267194911), Multiplier(0.0010908515356362696)),
    Genome(CostPerRow(35.43058709767039), CostPerRow(8.367770039335008), CostPerRow(16.77700834487859), CostPerRow(0.37452158958995563), CostPerRow(0.12642338891414875), Multiplier(0.21835379109619427)))
  )

  def randomStartingPoints: Seq[Genome] = for (i <- 1 to 30) yield
  new Genome(
    CPU_BOUND = CostPerRow(r.nextDouble() * 20),
    FAST_STORE = CostPerRow(r.nextDouble() * 20),
    SLOW_STORE = CostPerRow(r.nextDouble() * 20),
    PROBE_BUILD_COST = CostPerRow(r.nextDouble() * 20),
    PROBE_SEARCH_COST = CostPerRow(r.nextDouble() * 20),
    EAGERNESS_MULTIPLIER = Multiplier(r.nextDouble() * 20)
  )


  test("apa") {

    var population = randomStartingPoints

    // Pick a random benchmark to test
    val benchmarkDb = benchmarks.keys.toSeq.apply(r.nextInt(benchmarks.size))
    val queries = benchmarks(benchmarkDb)
    println("testing with " + benchmarkDb)
    implicit val db =
      new GraphDatabaseFactory().
        newEmbeddedDatabase(benchmarkDb).
        asInstanceOf[GraphDatabaseAPI]
    try {
      while (true) {
        val params = getRandomParams(queries)
        val fitnessF: Genome => Long = CachedFunction(testFitness(db, params, queries))
        val results = population.par.map(g => g -> fitnessF(g)).toList
        val min = results.map(_._2).min
        val max = results.map(_._2).max
        val avg = results.map(_._2).sum / results.size
        val winners = results.sortBy(_._2).take(5)
        println(
          s"""-=-=- Generational report -=-=-
             |Population size: ${population.size}
              |Best: $min
              |Worst: $max
              |Avg: $avg
              |
              |Winners:
              |${winners.mkString("\n")}
           """.stripMargin)
        population = nextGeneration(winners.map(_._1)).distinct
      }
    }
    finally
      db.shutdown()
  }

  var lastSeenLogicalPlan: LogicalPlan = null

  private def ronjaCompiler(genome: Genome)(implicit graph: GraphDatabaseService): CypherCompiler = {
    val metricsFactoryInput = new GeneticMetricsFactory(genome)
    val monitorTag = "CompilerComparison"
    val rewriterSequencer = RewriterStepSequencer.newPlain _
    val parser = new CypherParser(mock[ParserMonitor[Statement]])
    val checker = new SemanticChecker(mock[SemanticCheckMonitor])
    val rewriter = new ASTRewriter(rewriterSequencer, mock[AstRewritingMonitor])
    val metricsFactory = CachedMetricsFactory(metricsFactoryInput)
    val queryPlanner = new QueryPlanner {
      val inner = new DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))

      def plan(plannerQuery: UnionQuery)(implicit context: LogicalPlanningContext) = {
        val result = inner.plan(plannerQuery)
        lastSeenLogicalPlan = result
        result
      }
    }

    val monitors = new WrappedMonitors2_3(new KernelMonitors)
    val planner: CostBasedExecutablePlanBuilder = CostBasedPipeBuilderFactory(monitors, metricsFactory, mock[PlanningMonitor], Clock.SYSTEM_CLOCK, queryPlanner, rewriterSequencer, plannerName = IDPPlannerName)
    val pipeBuilder = new LegacyVsNewExecutablePlanBuilder(new LegacyExecutablePlanBuilder(monitors, rewriterSequencer), planner, mock[NewLogicalPlanSuccessRateMonitor])
    val execPlanBuilder = new ExecutionPlanBuilder(graph, 1, 1, Clock.SYSTEM_CLOCK, pipeBuilder)
    val planCacheFactory = () => new LRUCache[Statement, ExecutionPlan](100)
    val cacheHitMonitor = monitors.newMonitor[CypherCacheHitMonitor[Statement]](monitorTag)
    val cacheFlushMonitor = monitors.newMonitor[CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan]]](monitorTag)
    val cache = new MonitoringCacheAccessor[Statement, ExecutionPlan](cacheHitMonitor)

    new CypherCompiler(parser, checker, execPlanBuilder, rewriter, cache, planCacheFactory, cacheFlushMonitor, monitors)
  }

  val benchmarks = Map(
//    "/Users/ata/dev/neo/datasets/access-control-2.2" -> Seq(
//      "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company) RETURN company.name AS company" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",
//      "MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/resources_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_companies_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company) <-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",
//      "MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/companies_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN employee.name AS employee, account.name AS account" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) RETURN count(p) AS accessCount" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_resources_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company) RETURN company.name AS company" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",
//      "MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/resources_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}), (company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:CHILD_OF*0..3]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}), (company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_companies_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company)<-[:CHILD_OF*0..3]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",
//      "MATCH (company:Company {name:{companyName}}) MATCH (company)-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/companies_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN employee.name AS employee, account.name AS account" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",
//      "MATCH (admin:Administrator {name:{adminName}}), (resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}), (resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource) RETURN count(p) AS accessCount" ->
//        "/Users/ata/dev/neo/datasets/access-control-params/administrators_resources_params.txt"
//    ),
    "/Users/ata/dev/neo/datasets/recommendations" -> Seq(
      "MATCH (p:Person)-[:HAS_COMPETENCY]->(c2:Competency {name:'Java Programming'}), (p)-[:HAS_COMPETENCY]->(c3:Competency {name:'Carpenter'}), (p)-[:HAS_COMPETENCY]->(c4:Competency) RETURN p.first_name, p.last_name, collect(c4.name)" -> "NONE",
      "MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person), (p1)-->(co:Company)<--(p2) RETURN p1.first_name+' '+p1.last_name as Person1, p2.first_name+' '+p2.last_name as Person2, collect(distinct c.name), collect(distinct co.name) as Company LIMIT 50" -> "NONE",
      "MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person), (p1)-[:WORKED_FOR|:WORKS_FOR]->(co:Company)<-[:WORKED_FOR]-(p2) RETURN co.name as Company, c.name as Competency, count(p2) as Count ORDER BY Count desc LIMIT 10" -> "NONE",
      "MATCH path1=(p1:Person)-[:FRIEND_OF]-()-[:FRIEND_OF]-(p2:Person) WHERE NOT((p1)-[:FRIEND_OF]-(p2)) RETURN path1 LIMIT 50" -> "NONE",
      "MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person), (p1)-[:WORKED_FOR|:WORKS_FOR]->(co:Company)<-[:WORKED_FOR]-(p2) WHERE NOT((p1)-[:WORKS_FOR]->(co)<-[:WORKS_FOR]-(p2)) WITH p1,p2,c,co MATCH (p1)-[:FRIEND_OF]-()-[:FRIEND_OF]-(p2) RETURN p1.first_name+' '+p1.last_name as Person1, p2.first_name+' '+p2.last_name as Person2, collect(distinct c.name), collect(distinct co.name) as Company LIMIT 10" -> "NONE",
      "MATCH (p1:Person {first_name: 'Mike'})-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person), (p1)-[:WORKED_FOR|:WORKS_FOR]->(co:Company)<-[:WORKED_FOR]-(p2) WHERE NOT((p1)-[:WORKS_FOR]->(co)<-[:WORKS_FOR]-(p2)) WITH p1,p2,c,co MATCH (p1)-[:FRIEND_OF]-()-[:FRIEND_OF]-(p2) RETURN p1.first_name+' '+p1.last_name as Person1, p2.first_name+' '+p2.last_name as Person2, collect(distinct c.name), collect(distinct co.name) as Company" -> "NONE",
      "MATCH (p1:Person {name:'Shawna Luna'})-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person) WITH p1,p2 MATCH (p1)-->(co:Company)<--(p2), (prod1:Product)<-[:BOUGHT]-(p1)-[:FRIEND_OF]-(p2)-[:BOUGHT]->(prod1) WITH p1, p2, prod1 MATCH (prod1)-[:MADE_BY]->(b:Brand)<-[:MADE_BY]-(prod2:Product) WHERE NOT ((p1)-[:BOUGHT]->(prod2)) RETURN p1.name, prod1.name, prod2.name LIMIT 10" -> "NONE",
      "MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person) WITH p1,p2 MATCH (p1)-->(co:Company)<--(p2), (prod1:Product)<-[:BOUGHT]-(p1)-[:FRIEND_OF]-(p2)-[:BOUGHT]->(prod1) WITH p1, p2, prod1 MATCH (prod1)-[:MADE_BY]->(b:Brand)<-[:MADE_BY]-(prod2:Product) WHERE NOT ((p1)-[:BOUGHT]->(prod2)) RETURN p1.name, prod1.name, prod2.name LIMIT 10" -> "NONE",
      "MATCH p=allShortestPaths((source:Person)-[:FRIEND_OF*]-(target:Person)) WHERE id(source) < id(target) and length(p) > 1 UNWIND nodes(p)[1..-1] as n RETURN n.first_name, n.last_name, count(*) as betweenness ORDER BY betweenness DESC" -> "NONE")
//    "/Users/ata/dev/neo/datasets/QMUL-Maths" -> Seq(
//      "MATCH (a:Person)-->(m)-[r]->(n)-->(a) WHERE a.uid IN ['1195630902','1457065010'] AND HAS(m.location_lat) AND HAS(n.location_lat) RETURN count(r)" -> "NONE",
//      "MATCH (a:Person)-->(m)-[r]->(n)-->(a) WHERE a.uid IN ['1536073686','100002218599254'] AND HAS(m.location_lat) AND HAS(n.location_lat) RETURN count(r)" -> "NONE",
//      "MATCH (a:Person)-->(m)-[r]->(n)-->(ax) WHERE ax=a AND a.uid IN ['1195630902','1457065010'] WITH m, n, r WHERE HAS(m.location_lat) AND HAS(n.location_lat) RETURN count(r)" -> "NONE",
//      "MATCH (a:Person)-->(m)-[r]->(n)-->(ax) WHERE ax=a AND a.uid IN ['1536073686','100002218599254'] WITH m, n, r WHERE HAS(m.location_lat) AND HAS(n.location_lat) RETURN count(r)" -> "NONE"),
//    "/Users/ata/dev/neo/datasets/elections" -> Seq(
//      "MATCH (cand:Candidate)<-[:CAMPAIGNS_FOR]-(camp:Committee) WHERE cand.CAND_OFFICE='P' AND cand.CAND_ELECTION_YR='2012' RETURN camp.CMTE_NM, cand.CAND_NAME" -> "NONE",
//      "MATCH p=(comm:Committee)<-[:INDIVIDUAL_CONTRIBUTION|INTER_COMMITTEE_CONTRIBUTION]-(rec) RETURN p LIMIT 10" -> "NONE",
//      "MATCH (comm:Committee)<-[:INDIVIDUAL_CONTRIBUTION]-(indiv:Individual)-[:EARMARKED_BY]->(rec:Candidate) RETURN indiv.TRANSACTION_AMT, comm.CMTE_NM LIMIT 10" -> "NONE",
//      "MATCH (cand:Candidate) WHERE cand.CAND_OFFICE='P' AND cand.CAND_ELECTION_YR='2012' RETURN cand.CAND_NAME" -> "NONE",
//      "MATCH (cand:Candidate)<-[r:SUPPORTS]-(camp:Committee) WHERE cand.CAND_OFFICE='P' AND cand.CAND_ELECTION_YR='2012' RETURN cand.CAND_NAME, COUNT(camp) as count ORDER BY count desc LIMIT 10" -> "NONE",
//      "MATCH (cand:Candidate {CAND_ID: 'P80003338'})<-[:SUPPORTS]-(camp:Committee)<-[:INDIVIDUAL_CONTRIBUTION]-(contrib:Individual) RETURN contrib.NAME, contrib.TRANSACTION_AMT ORDER BY contrib.TRANSACTION_AMT desc LIMIT 10" -> "NONE",
//      "MATCH (romney:Candidate {CAND_ID: 'P80003353'}), (obama:Candidate {CAND_ID: 'P80003338'}), p=shortestPath((romney)-[*..10]-(obama)) RETURN p" -> "NONE"),
//    "/Users/ata/dev/neo/datasets/musicbrainz" -> Seq(
//      "MATCH (x:Country {name: {name_1}}), (y:Country {name: {name_2}}), (a:Artist)-[:FROM_AREA]-(x), (a:Artist)-[:RECORDING_CONTRACT]-(l:Label), (l)-[:FROM_AREA]-(y) RETURN a,l,y,x" ->
//        "/Users/ata/dev/neo/datasets/musicbrainz-params/countries_params.txt",
//      "MATCH (artist:Artist {name:{name}})-[:COMPOSER]->(song:Song)<-[:LYRICIST]-(artist) MATCH (song)<-[:PERFORMANCE]-(r:Recording)<-[:VOCAL]-(artist) RETURN count(distinct song.name)" ->
//        "/Users/ata/dev/neo/datasets/musicbrainz-params/artists_params.txt",
//      "MATCH (a:Artist {name: {name}})-[:MEMBER_OF_BAND]-(a1)-[:MEMBER_OF_BAND]-(a2)-[:MEMBER_OF_BAND]-(o:Artist) RETURN o.name,count(*) ORDER BY count(*) DESC LIMIT 10" ->
//        "/Users/ata/dev/neo/datasets/musicbrainz-params/artists_params.txt",
//      "MATCH (a:Artist { name: {name}}) - [r] - (b) RETURN type(r), labels(b), count(*) ORDER BY count(*) desc LIMIT 25" ->
//        "/Users/ata/dev/neo/datasets/musicbrainz-params/artists_params.txt",
//      "MATCH (a:Artist {name: {name}})-[:CREDITED_AS]->(b)-[:CREDITED_ON]->(t:Track) RETURN t.name" ->
//        "/Users/ata/dev/neo/datasets/musicbrainz-params/artists_params.txt",
//      "MATCH (a:Artist {name: {name}})-[:CREDITED_AS]->(b)-[:CREDITED_ON]->(t:Track)-[:APPEARS_ON]->(m:Medium)<-[:RELEASED_ON_MEDIUM]-(r:Release) return t.name, r.name" ->
//        "/Users/ata/dev/neo/datasets/musicbrainz-params/artists_params.txt",
//      "MATCH (t:Track)-[:APPEARS_ON]->(a:Album) WHERE id(a) = {id} RETURN * LIMIT 50" ->
//        "/Users/ata/dev/neo/datasets/musicbrainz-params/albums_params.txt",
//      "MATCH (a:Artist)-[r]-(b) WITH r,b LIMIT 10000 RETURN type(r),labels(b),count(*) ORDER BY count(*) desc" -> "NONE",
//      "MATCH (a:Song)-[r]-(b) WITH r,b limit 10000 RETURN type(r),labels(b),count(*) ORDER BY count(*) desc" -> "NONE",
//      "MATCH (gb:Country {name:'United Kingdom'}), (usa:Country {name:'United States'}), (a:Artist)-[:FROM_AREA]-(gb), (a:Artist)-[:RECORDING_CONTRACT]-(l:Label), (l)-[:FROM_AREA]-(usa) RETURN a,l,usa,gb" -> "NONE",
//      "MATCH (artist:Artist {name:'Bob Dylan'})-[:COMPOSER]->(song:Song)<-[:LYRICIST]-(artist) MATCH (song)<-[:PERFORMANCE]-(r:Recording)<-[:VOCAL]-(artist) RETURN count(distinct song.name)" -> "NONE",
//      "MATCH (a:Artist {name:'John Lennon'})-[:MEMBER_OF_BAND]-(a1)-[:MEMBER_OF_BAND]-(a2)-[:MEMBER_OF_BAND]-(o:Artist) RETURN o.name,count(*) ORDER BY count(*) DESC LIMIT 10" -> "NONE",
//      "MATCH (a:Artist {name:'John Lennon'})-[r:MEMBER_OF_BAND*3..6]-(o:Person)  RETURN o.name,count(*)  ORDER BY count(*) DESC LIMIT 10" -> "NONE",
//      "MATCH (a:Artist {name: 'John Lennon'})-[:CREDITED_AS]->(b)-[:CREDITED_ON]->(t:Track)-[:APPEARS_ON]->(m:Medium)<-[:RELEASED_ON_MEDIUM]-(r:Release) return t.name, r.name" -> "NONE",
//      "MATCH (a:Artist {name: 'John Lennon'})-[:CREDITED_AS]->(b)-[:CREDITED_ON]->(t:Track) RETURN t.name" -> "NONE",
//      "MATCH (a:Artist)-->(al:Album) WHERE al.releasedIn = 1979 RETURN * LIMIT 50" -> "NONE",
//      "MATCH (a:Artist)-->(al:Album) WHERE a.gender = 'male' RETURN * LIMIT 50" -> "NONE",
//      "MATCH (t1:Track)--(al:Album)--(t2:Track) WHERE t1.duration = 61 AND t2.duration = 68 RETURN * LIMIT 50" -> "NONE",
//      "MATCH (t:Track)--(al:Album)--(a:Artist) WHERE t.duration = 61 AND a.gender = 'male' RETURN * LIMIT 50" -> "NONE",
//      "MATCH (al:Album) RETURN (:Artist)-->(al)<-[:APPEARS_ON]-(:Track) LIMIT 50" -> "NONE",
//      "MATCH (t:Track) WHERE t.duration IN [60, 61, 62, 63, 64] RETURN * LIMIT 50" -> "NONE",
//      "MATCH (al:Album) WHERE (:Artist)-->(al) AND (al)<-[:APPEARS_ON]-(:Track) RETURN * LIMIT 50" -> "NONE",
//      "MATCH (artist:Artist) WHERE NOT (artist)-->(:Album {relasedIn: 1975}) RETURN * LIMIT 50" -> "NONE",
//      "MATCH (t:Track)-[:APPEARS_ON]->(a:Album) WHERE t.duration IN [60, 61, 62, 63, 64] RETURN * LIMIT 50" -> "NONE",
//      "MATCH (t:Track)-[:APPEARS_ON]->(a:Album) RETURN * LIMIT 50" -> "NONE")
    )
}
