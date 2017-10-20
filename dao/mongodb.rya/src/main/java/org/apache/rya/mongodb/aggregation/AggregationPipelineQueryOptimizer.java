/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.mongodb.aggregation;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;

public class AggregationPipelineQueryOptimizer implements QueryOptimizer, Configurable {
    private Configuration conf;
    private Logger logger = Logger.getLogger(getClass());

    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        if (conf instanceof MongoDBRdfConfiguration) {
            MongoDBRdfConfiguration mongoConf = (MongoDBRdfConfiguration) conf;
            SparqlToPipelineTransformVisitor pipelineVisitor = new SparqlToPipelineTransformVisitor(mongoConf);
            try {
                tupleExpr.visit(pipelineVisitor);
            } catch (Exception e) {
                logger.error("Error attempting to transform query using the aggregation pipeline", e);
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
