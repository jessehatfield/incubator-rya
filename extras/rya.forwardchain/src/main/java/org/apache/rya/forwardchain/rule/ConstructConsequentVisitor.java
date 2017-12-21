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
package org.apache.rya.forwardchain.rule;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.openrdf.model.Value;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * Query visitor that identifies all triple patterns produced by a "CONSTRUCT"
 * query. Finds the topmost instance of a {@link Projection} or
 * {@link MultiProjection}, and expects the variables projected to include
 * "subject", "predicate", and "object". Each projection is converted to a
 * {@link StatementPattern}, where any constant values are expected to be
 * provided by an Extension directly underneath the projection, if applicable.
 * <p>
 * Undefined behavior if applied to a query other than a CONSTRUCT query.
 * <p>
 * Does not report any constraints on possible consequent triples beyond the
 * constant values, where appropriate, of each part of the triple. Therefore,
 * this analysis may produce an overly broad set of possible consequents
 * compared to some more sophisticated method.
 */
public class ConstructConsequentVisitor extends QueryModelVisitorBase<RuntimeException> {
    private Set<StatementPattern> consequentStatementPatterns = new HashSet<>();
    private static final String SUBJECT_VAR_NAME = "subject";
    private static final String PREDICATE_VAR_NAME = "predicate";
    private static final String OBJECT_VAR_NAME = "object";

    /**
     * Get the possible conclusions of this construct rule.
     * @return StatementPatterns representing the possible triple patterns that
     *  can be inferred.
     */
    public Set<StatementPattern> getConsequents() {
        return consequentStatementPatterns;
    }

    @Override
    public void meet(Projection projection) {
        if (projection.getArg() instanceof Extension) {
            recordConsequent(projection.getProjectionElemList(),
                    ((Extension) projection.getArg()).getElements());
        }
        else {
            recordConsequent(projection.getProjectionElemList(), Arrays.asList());
        }
    }

    @Override
    public void meet(MultiProjection projection) {
        List<ExtensionElem> bindings;
        if (projection.getArg() instanceof Extension) {
            bindings = ((Extension) projection.getArg()).getElements();
        }
        else {
            bindings = Arrays.asList();
        }
        for (ProjectionElemList template : projection.getProjections()) {
            recordConsequent(template, bindings);
        }
    }

    private void recordConsequent(ProjectionElemList variables, List<ExtensionElem> extensionElements) {
        Map<String, Value> bindings = new ConcurrentHashMap<>();
        Map<String, Value> values = new ConcurrentHashMap<>();
        for (ExtensionElem ee : extensionElements) {
            if (ee.getExpr() instanceof ValueConstant) {
                bindings.put(ee.getName(), ((ValueConstant) ee.getExpr()).getValue());
            }
        }
        for (ProjectionElem var : variables.getElements()) {
            Value constValue = bindings.get(var.getSourceName());
            if (constValue != null) {
                values.put(var.getTargetName(), constValue);
            }
        }
        Var subjVar = new Var(SUBJECT_VAR_NAME, values.get(SUBJECT_VAR_NAME));
        Var predVar = new Var(PREDICATE_VAR_NAME, values.get(PREDICATE_VAR_NAME));
        Var objVar = new Var(OBJECT_VAR_NAME, values.get(OBJECT_VAR_NAME));
        StatementPattern sp = new StatementPattern(subjVar, predVar, objVar);
        consequentStatementPatterns.add(sp);
    }
}
