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
package org.apache.rya.forwardchain.strategy;

import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.RyaQuery;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.forwardchain.ForwardChainException;
import org.apache.rya.forwardchain.rule.AbstractConstructRule;
import org.apache.rya.sail.config.RyaSailFactory;
import org.calrissian.mango.collect.CloseableIterable;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailGraphQuery;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.google.common.base.Preconditions;

/**
 * A naive but back-end-agnostic rule execution strategy that applies a
 * construct rule by submitting the associated query to a Rya SAIL, then
 * converting the resulting bindings (expecting variables "subject",
 * "predicate", and "object") into triples and inserting them into a Rya DAO.
 */
public class SailExecutionStrategy extends AbstractRuleExecutionStrategy {
    private static final Logger logger = Logger.getLogger(SailExecutionStrategy.class);

    private final RdfCloudTripleStoreConfiguration conf;

    private SailRepository repo = null;
    private SailRepositoryConnection conn = null;
    private RyaDAO<?> dao = null;
    private boolean initialized = false;

    /**
     * Initialize a SailExecutionStrategy with the given configuration and a DAO.
     * @param conf Defines Rya connection and query parameters; not null.
     * @param dao The DAO to use for inserting new triples; not null.
     */
    public SailExecutionStrategy(RdfCloudTripleStoreConfiguration conf, RyaDAO<?> dao) {
        Preconditions.checkNotNull(conf);
        Preconditions.checkNotNull(dao);
        this.conf = conf;
        this.dao = dao;
    }

    /**
     * Initialize a SailExecutionStrategy with the given configuration.
     * @param conf Defines Rya connection and query parameters; not null.
     */
    public SailExecutionStrategy(RdfCloudTripleStoreConfiguration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    /**
     * Executes a CONSTRUCT query through the SAIL and inserts the results into
     * the DAO.
     * @param rule A construct query; not null.
     * @param metadata Metadata to add to any inferred triples; not null.
     * @return The number of inferred triples.
     * @throws ForwardChainException if query execution or data insert fails.
     */
    @Override
    public long executeConstructRule(AbstractConstructRule rule,
            StatementMetadata metadata) throws ForwardChainException {
        Preconditions.checkNotNull(rule);
        Preconditions.checkNotNull(metadata);
        if (!initialized) {
            initialize();
        }
        ParsedGraphQuery graphQuery = rule.getQuery();
        long statementsAdded = 0;
        logger.info("Applying inference rule " + rule + "...");
        for (String line : graphQuery.getTupleExpr().toString().split("\n")) {
            logger.debug("\t" + line);
        }
        InferredStatementHandler<?> handler = new InferredStatementHandler<>(dao, metadata);
        try {
            GraphQuery executableQuery = new SailGraphQuery(graphQuery, conn) { };
            executableQuery.evaluate(handler);
            statementsAdded = handler.getNumStatementsAdded();
            logger.info("Added " + statementsAdded + " inferred statements.");
            return statementsAdded;
        } catch (QueryEvaluationException e) {
            throw new ForwardChainException("Error evaluating query portion of construct rule", e);
        } catch (RDFHandlerException e) {
            throw new ForwardChainException("Error processing results of construct rule", e);
        }
    }

    /**
     * Connect to the Rya SAIL. If a DAO wasn't provided, instantiate one from
     * the configuration. 
     * @throws ForwardChainException if connecting fails.
     */
    @Override
    public void initialize() throws ForwardChainException {
        try {
            if (dao == null) {
                dao = RyaSailFactory.getRyaDAO(conf);
            }
            repo = new SailRepository(RyaSailFactory.getInstance(conf));
            conn = repo.getConnection();
            initialized = true;
        } catch (Exception e) {
            shutDown();
            throw new ForwardChainException("Error connecting to SAIL", e);
        }
    }

    /**
     * Shut down the SAIL connection objects.
     */
    @Override
    public void shutDown() {
        initialized = false;
        if (conn != null) {
            try {
                conn.close();
            } catch (RepositoryException e) {
                logger.warn("Error closing SailRepositoryConnection", e);
            }
        }
        if (repo != null && repo.isInitialized()) {
            try {
                repo.shutDown();
            } catch (RepositoryException e) {
                logger.warn("Error shutting down SailRepository", e);
            }
        }
        try {
            if (dao != null && dao.isInitialized()) {
                dao.flush();
            }
        } catch (RyaDAOException e) {
            logger.warn("Error flushing DAO", e);
        }
    }

    private static class InferredStatementHandler<T extends RdfCloudTripleStoreConfiguration> extends RDFHandlerBase {
        private RyaDAO<T> dao;
        private RyaQueryEngine<T> engine;
        private long numStatementsAdded = 0;
        private StatementMetadata metadata;

        InferredStatementHandler(RyaDAO<T> dao, StatementMetadata metadata) {
            this.dao = dao;
            this.engine = dao.getQueryEngine();
            this.metadata = metadata;
            this.engine.setConf(dao.getConf());
        }

        @Override
        public void handleStatement(Statement statement) {
            RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
            ryaStatement.setStatementMetadata(metadata);
            try {
                // Need to check whether the statement already exists, because
                // we need an accurate count of newly added statements.
                CloseableIterable<RyaStatement> iter = engine.query(new RyaQuery(ryaStatement));
                if (!iter.iterator().hasNext()) {
                    dao.add(ryaStatement);
                    numStatementsAdded++;
                }
            } catch (RyaDAOException e) {
                logger.error("Error handling inferred statement", e);
            }
        }

        public long getNumStatementsAdded() {
            return numStatementsAdded;
        }
    }
}
