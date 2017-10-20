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

import java.util.Arrays;

import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.bson.Document;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class SparqlToPipelineTransformVisitor extends QueryModelVisitorBase<Exception> {
    private MongoCollection<Document> inputCollection;

    public SparqlToPipelineTransformVisitor(MongoCollection<Document> inputCollection) {
        this.inputCollection = inputCollection;
    }

    public SparqlToPipelineTransformVisitor(MongoDBRdfConfiguration conf) {
        MongoClient mongo = MongoConnectorFactory.getMongoClient(conf);
        MongoDatabase db = mongo.getDatabase(conf.getMongoDBName());
        this.inputCollection = db.getCollection(conf.getTriplesCollectionName());
    }

    @Override
    public void meet(StatementPattern sp) {
        sp.replaceWith(new AggregationPipelineQueryNode(inputCollection, sp));
    }

    @Override
    public void meet(Join join) throws Exception {
        // If one branch is a single statement pattern, then try replacing the
        // other with a pipeline.
        AggregationPipelineQueryNode pipelineNode = null;
        StatementPattern joinWithSP = null;
        if (join.getRightArg() instanceof StatementPattern) {
            join.getLeftArg().visit(this);
            if (join.getLeftArg() instanceof AggregationPipelineQueryNode) {
                pipelineNode = (AggregationPipelineQueryNode) join.getLeftArg();
                joinWithSP = (StatementPattern) join.getRightArg();
            }
        }
        else if (join.getLeftArg() instanceof StatementPattern) {
            join.getRightArg().visit(this);
            if (join.getRightArg() instanceof AggregationPipelineQueryNode) {
                pipelineNode = (AggregationPipelineQueryNode) join.getRightArg();
                joinWithSP = (StatementPattern) join.getLeftArg();
            }
        }
        else {
            // Otherwise, visit the children to try to replace smaller subtrees
            join.visitChildren(this);
        }
        // If this is now a join between a pipeline node and a statement
        // pattern, add the join step at the end of the pipeline, and replace
        // this node with the extended pipeline node.
        if (pipelineNode != null && joinWithSP != null && pipelineNode.joinWith(joinWithSP)) {
            join.replaceWith(pipelineNode);
        }
    }

    @Override
    public void meet(Projection projectionNode) throws Exception {
        projectionNode.visitChildren(this);
        if (projectionNode.getArg() instanceof AggregationPipelineQueryNode && projectionNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) projectionNode.getArg();
            if (pipelineNode.project(Arrays.asList(projectionNode.getProjectionElemList()))) {
                projectionNode.replaceWith(pipelineNode);
            }
        }
    }

    @Override
    public void meet(MultiProjection projectionNode) throws Exception {
        projectionNode.visitChildren(this);
        if (projectionNode.getArg() instanceof AggregationPipelineQueryNode && projectionNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) projectionNode.getArg();
            if (pipelineNode.project(projectionNode.getProjections())) {
                projectionNode.replaceWith(pipelineNode);
            }
        }
    }

    @Override
    public void meet(Extension extensionNode) throws Exception {
        extensionNode.visitChildren(this);
        if (extensionNode.getArg() instanceof AggregationPipelineQueryNode && extensionNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) extensionNode.getArg();
            if (pipelineNode.extend(extensionNode.getElements())) {
                extensionNode.replaceWith(pipelineNode);
            }
        }
    }

    @Override
    public void meet(Reduced reducedNode) throws Exception {
        reducedNode.visitChildren(this);
        if (reducedNode.getArg() instanceof AggregationPipelineQueryNode && reducedNode.getParentNode() != null) {
            reducedNode.replaceWith(reducedNode.getArg());
        }
    }

    @Override
    public void meet(Distinct distinctNode) throws Exception {
        distinctNode.visitChildren(this);
        if (distinctNode.getArg() instanceof AggregationPipelineQueryNode && distinctNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) distinctNode.getArg();
            pipelineNode.distinct();
            distinctNode.replaceWith(pipelineNode);
        }
    }

    @Override
    public void meet(Filter filterNode) throws Exception {
        filterNode.visitChildren(this);
        if (filterNode.getArg() instanceof AggregationPipelineQueryNode && filterNode.getParentNode() != null) {
            AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) filterNode.getArg();
            if (pipelineNode.filter(filterNode.getCondition())) {
                filterNode.replaceWith(pipelineNode);
            }
        }
    }
}