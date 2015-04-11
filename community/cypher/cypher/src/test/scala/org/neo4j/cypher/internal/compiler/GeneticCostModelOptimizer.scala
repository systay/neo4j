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

import scala.collection.mutable
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

  val cache = mutable.HashMap[LogicalPlan, Long]()

  private def testFitness(db: GraphDatabaseAPI, params: Map[FileName, Map[ParamKey, ParamValue]])(genome: Genome): Long = {
    val compiler = ronjaCompiler(genome)(db)

    def testQuery(query: String, fileName: FileName) = {

      val (plan: ExecutionPlan, parameters) = db.withTx {
        tx =>
          val planContext = new TransactionBoundPlanContext(db.statement, db)
          compiler.planQuery(query, planContext, devNullLogger)
      }

      if (cache.contains(lastSeenLogicalPlan))
        print(".")

      val dbHits = cache.getOrElseUpdate(lastSeenLogicalPlan, db.withTx {
        tx =>
          print("|")
          val queryContext = new TransactionBoundQueryContext(db, tx, true, db.statement)
          val result = plan.run(queryContext, db.statement, ProfileMode, parameters ++ params(fileName))
          println(result.dumpToString())
          result.executionPlanDescription().totalDbHits.get
      })

      dbHits
    }
    val total = queries.map {
      case (query, fileName) => testQuery(query, fileName)
    }.sum
    println(s"$total -> $genome")
    total
  }

  private def nextGeneration(parents: Seq[Genome]): Seq[Genome] = {
    val clones = for (ignored <- 1 to 10;
                      l <- parents;
                      r <- parents if l != r) yield l.mateWith(r)
    if (clones.isEmpty) {
      parents.flatMap(_.clone(10))
    } else
      clones
  }

  type FileName = String
  type ParamKey = String
  type ParamValue = String

  lazy val paramsFiles: Map[FileName, List[Map[ParamKey, ParamValue]]] = {
    val fileNames: Seq[FileName] =
      queries.map(_._2)

    val fileNamesAndFileContent =
      fileNames.map { f =>
        val lines: Iterator[String] = scala.io.Source.fromFile(f).getLines()
        val next = lines.next()
        val keys: Array[ParamKey] = next.split('|')
        val result: List[Map[ParamKey, ParamValue]] = lines.map(l => {
          val paramKeyToValue: Map[ParamKey, ParamValue] = (keys zip l.split('|')).toMap
          paramKeyToValue
        }).toList

        f -> result
      }.toMap

    fileNamesAndFileContent
  }

  // Pick a random param from each file
  private def getRandomParams(): Map[FileName, Map[ParamKey, ParamValue]] = paramsFiles.mapValues {
    params: List[Map[ParamKey, ParamValue]] => params.apply(r.nextInt(params.length))
  }

  test("apa") {
    implicit val db =
      new GraphDatabaseFactory().
        newEmbeddedDatabase("/Users/ata/dev/neo/datasets/access-control-2.2").
        asInstanceOf[GraphDatabaseAPI]

    try {
      //      Seed with a specific genome
      //      var population = nextGeneration(Seq(
      //        Genome(CostPerRow(6.926715054967385),CostPerRow(1.70978920816183),CostPerRow(0.002659392740993439),CostPerRow(0.04327718042941619),CostPerRow(0.015126098267194911),Multiplier(0.0010908515356362696))
      //      ))

      //      Random seeds
      var population: Seq[Genome] = for (i <- 1 to 60) yield
      new Genome(
        CPU_BOUND = CostPerRow(r.nextDouble() * 20),
        FAST_STORE = CostPerRow(r.nextDouble() * 20),
        SLOW_STORE = CostPerRow(r.nextDouble() * 20),
        PROBE_BUILD_COST = CostPerRow(r.nextDouble() * 20),
        PROBE_SEARCH_COST = CostPerRow(r.nextDouble() * 20),
        EAGERNESS_MULTIPLIER = Multiplier(r.nextDouble() * 20)
      )

      while (true) {
        val params = getRandomParams()
        val fitnessF: Genome => Long = CachedFunction(testFitness(db, params))
        val winners = population.map(g => g -> fitnessF(g)).sortBy(_._2).take(3)
        println("Surviving this generation:")
        println(winners.mkString("\n"))
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
    "/Users/ata/dev/neo/datasets/access-control-2.2" -> Seq(
      "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company) RETURN company.name AS company" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

      "MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
        "/Users/ata/dev/neo/datasets/access-control-params/resources_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_companies_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company) <-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

      "MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
        "/Users/ata/dev/neo/datasets/access-control-params/companies_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN employee.name AS employee, account.name AS account" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) RETURN count(p) AS accessCount" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_resources_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company) RETURN company.name AS company" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

      "MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
        "/Users/ata/dev/neo/datasets/access-control-params/resources_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}), (company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:CHILD_OF*0..3]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}), (company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_companies_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company)<-[:CHILD_OF*0..3]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

      "MATCH (company:Company {name:{companyName}}) MATCH (company)-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
        "/Users/ata/dev/neo/datasets/access-control-params/companies_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN employee.name AS employee, account.name AS account" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

      "MATCH (admin:Administrator {name:{adminName}}), (resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}), (resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource) RETURN count(p) AS accessCount" ->
        "/Users/ata/dev/neo/datasets/access-control-params/administrators_resources_params.txt"
    )
  )

  // Recommendations
  //  val queries = Seq(
  //    "MATCH (p:Person)-[:HAS_COMPETENCY]->(c2:Competency {name:'Java Programming'}), (p)-[:HAS_COMPETENCY]->(c3:Competency {name:'Carpenter'}), (p)-[:HAS_COMPETENCY]->(c4:Competency) RETURN p.first_name, p.last_name, collect(c4.name)",
  //    "MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person), (p1)-->(co:Company)<--(p2) RETURN p1.first_name+' '+p1.last_name as Person1, p2.first_name+' '+p2.last_name as Person2, collect(distinct c.name), collect(distinct co.name) as Company LIMIT 50",
  //    "MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person), (p1)-[:WORKED_FOR|:WORKS_FOR]->(co:Company)<-[:WORKED_FOR]-(p2) RETURN co.name as Company, c.name as Competency, count(p2) as Count ORDER BY Count desc LIMIT 10",
  //    "MATCH path1=(p1:Person)-[:FRIEND_OF]-()-[:FRIEND_OF]-(p2:Person) WHERE NOT((p1)-[:FRIEND_OF]-(p2)) RETURN path1 LIMIT 50",
  //    "MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person), (p1)-[:WORKED_FOR|:WORKS_FOR]->(co:Company)<-[:WORKED_FOR]-(p2) WHERE NOT((p1)-[:WORKS_FOR]->(co)<-[:WORKS_FOR]-(p2)) WITH p1,p2,c,co MATCH (p1)-[:FRIEND_OF]-()-[:FRIEND_OF]-(p2) RETURN p1.first_name+' '+p1.last_name as Person1, p2.first_name+' '+p2.last_name as Person2, collect(distinct c.name), collect(distinct co.name) as Company LIMIT 10",
  //    "MATCH (p1:Person {first_name: 'Mike'})-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person), (p1)-[:WORKED_FOR|:WORKS_FOR]->(co:Company)<-[:WORKED_FOR]-(p2) WHERE NOT((p1)-[:WORKS_FOR]->(co)<-[:WORKS_FOR]-(p2)) WITH p1,p2,c,co MATCH (p1)-[:FRIEND_OF]-()-[:FRIEND_OF]-(p2) RETURN p1.first_name+' '+p1.last_name as Person1, p2.first_name+' '+p2.last_name as Person2, collect(distinct c.name), collect(distinct co.name) as Company",
  //    "MATCH (p1:Person {name:'Shawna Luna'})-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person) WITH p1,p2 MATCH (p1)-->(co:Company)<--(p2), (prod1:Product)<-[:BOUGHT]-(p1)-[:FRIEND_OF]-(p2)-[:BOUGHT]->(prod1) WITH p1, p2, prod1 MATCH (prod1)-[:MADE_BY]->(b:Brand)<-[:MADE_BY]-(prod2:Product) WHERE NOT ((p1)-[:BOUGHT]->(prod2)) RETURN p1.name, prod1.name, prod2.name LIMIT 10",
  //    "MATCH (p1:Person)-[:HAS_COMPETENCY]->(c:Competency)<-[:HAS_COMPETENCY]-(p2:Person) WITH p1,p2 MATCH (p1)-->(co:Company)<--(p2), (prod1:Product)<-[:BOUGHT]-(p1)-[:FRIEND_OF]-(p2)-[:BOUGHT]->(prod1) WITH p1, p2, prod1 MATCH (prod1)-[:MADE_BY]->(b:Brand)<-[:MADE_BY]-(prod2:Product) WHERE NOT ((p1)-[:BOUGHT]->(prod2)) RETURN p1.name, prod1.name, prod2.name LIMIT 10",
  //    "MATCH p=allShortestPaths((source:Person)-[:FRIEND_OF*]-(target:Person)) WHERE id(source) < id(target) and length(p) > 1 UNWIND nodes(p)[1..-1] as n RETURN n.first_name, n.last_name, count(*) as betweenness ORDER BY betweenness DESC")

  // Access control
  val queries = Seq(
    "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company) RETURN company.name AS company" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

    "MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
      "/Users/ata/dev/neo/datasets/access-control-params/resources_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}),(company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_companies_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company) <-[:CHILD_OF]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

    "MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)-[:CHILD_OF]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
      "/Users/ata/dev/neo/datasets/access-control-params/companies_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN employee.name AS employee, account.name AS account" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF]-(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-(resource) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}),(resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT]-()-[:WORKS_FOR|HAS_ACCOUNT]-(resource) RETURN count(p) AS accessCount" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_resources_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN company.name AS company UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company) RETURN company.name AS company" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

    "MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN admin.name AS admin UNION MATCH (resource:Resource {name:{resourceName}}) MATCH p=(resource)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
      "/Users/ata/dev/neo/datasets/access-control-params/resources_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}), (company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company)<-[:CHILD_OF*0..3]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}), (company:Company {name:{companyName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_companies_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_INHERIT]->(company:Company)<-[:CHILD_OF*0..3]-(subcompany)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(subcompany)) RETURN account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH (admin)-[:MEMBER_OF]->(group)-[:ALLOWED_DO_NOT_INHERIT]->(company:Company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN account.name AS account" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

    "MATCH (company:Company {name:{companyName}}) MATCH (company)-[:CHILD_OF*0..3]->()<-[:ALLOWED_INHERIT]-()<-[:MEMBER_OF]-(admin) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN admin.name AS admin UNION MATCH (company:Company {name:{companyName}}) MATCH (company)<-[:ALLOWED_DO_NOT_INHERIT]-()<-[:MEMBER_OF]-(admin) RETURN admin.name AS admin" ->
      "/Users/ata/dev/neo/datasets/access-control-params/companies_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company)<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN employee.name AS employee, account.name AS account UNION MATCH (admin:Administrator {name:{adminName}}) MATCH paths=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->()<-[:WORKS_FOR]-(employee)-[:HAS_ACCOUNT]->(account) RETURN employee.name AS employee, account.name AS account" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_params.txt",

    "MATCH (admin:Administrator {name:{adminName}}), (resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_INHERIT]->()<-[:CHILD_OF*0..3]-(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource) WHERE NOT ((admin)-[:MEMBER_OF]->()-[:DENIED]->()<-[:CHILD_OF*0..3]-(company)) RETURN count(p) AS accessCount UNION MATCH (admin:Administrator {name:{adminName}}), (resource:Resource {name:{resourceName}}) MATCH p=(admin)-[:MEMBER_OF]->()-[:ALLOWED_DO_NOT_INHERIT]->(company)-[:WORKS_FOR|HAS_ACCOUNT*1..2]-(resource) RETURN count(p) AS accessCount" ->
      "/Users/ata/dev/neo/datasets/access-control-params/administrators_resources_params.txt"
  )
}
