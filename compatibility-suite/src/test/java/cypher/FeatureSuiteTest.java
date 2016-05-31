/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith( UnpackedFeatures.class )
public class FeatureSuiteTest
{

    // These two constants are only used to make testing and debugging easier.
    // If you want to run only a single feature, put the name of the feature file in `FEATURE_TO_RUN` (including .feature)
    // If you want to run only a single scenario, put (part of) its name in the `SCENARIO_NAME_REQUIRED` constant
    // Do not forget to clear these strings to empty strings before you commit!!
    public static final String FEATURE_TO_RUN = "";
    public static final String SCENARIO_NAME_REQUIRED = "";

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    "html:target/rule-interpreted",
                    "cypher.feature.reporting.CypherResultReporter:target/rule-interpreted",
                    "cypher.cucumber.db.DatabaseProvider:target/dbs",
                    "cypher.cucumber.db.DatabaseConfigProvider:/cypher/db/config/rule.json"
            },
            glue = { "classpath:cypher/feature/steps" },
            features = { "target/features/" + FEATURE_TO_RUN },
            strict = true
    )
    public static class RuleInterpreted
    {
    }

    @RunWith( Cucumber.class )
    @CucumberOptions(
            plugin = {
                    "html:target/cost-interpreted",
                    "cypher.feature.reporting.CypherResultReporter:target/cost-interpreted",
                    "cypher.cucumber.db.DatabaseProvider:target/dbs",
                    "cypher.cucumber.db.DatabaseConfigProvider:/cypher/db/config/cost-interpreted.json",
            },
            glue = { "classpath:cypher/feature/steps" },
            features = { "target/features/" + FEATURE_TO_RUN },
            strict = true
    )
    public static class CostInterpreted
    {
    }
}
