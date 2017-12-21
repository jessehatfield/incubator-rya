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

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class SpinConstructRuleTest {
    private static ValueFactory VF = ValueFactoryImpl.getInstance();
    private static SPARQLParser PARSER = new SPARQLParser();

    private static URI RL_CAX_SCO = VF.createURI("http://example.org/rl/cax-sco");
    private static URI RL_SCM_CLS = VF.createURI("http://example.org/rl/scm-cls");
    private static URI RL_PRP_SPO1 = VF.createURI("http://example.org/rl/prp-spo");
    private static URI LIVING_THING = VF.createURI("http://example.org/LivingThing");

    private static Var c(Value val) {
        return new Var("-const-" + val.stringValue(), val);
    }
    private static Var ac(Value val) {
        Var v = c(val);
        v.setAnonymous(true);
        return v;
    }

    @Test
    public void testEmptyWhere() throws Exception {
        String text = "CONSTRUCT {\n"
                + "  ?this a <" + LIVING_THING.stringValue() + "> .\n"
                + "} WHERE { }";
        ParsedGraphQuery query = (ParsedGraphQuery) PARSER.parseQuery(text, null);
        SpinConstructRule rule = new SpinConstructRule(FOAF.PERSON, VF.createURI("urn:person-is-living"), query);
        Multiset<StatementPattern> expectedAntecedents = HashMultiset.create(Arrays.asList(
                new StatementPattern(new Var("this"), c(RDF.TYPE), c(FOAF.PERSON))));
        Multiset<StatementPattern> expectedConsequents = HashMultiset.create(Arrays.asList(
                new StatementPattern(new Var("subject"), new Var("predicate", RDF.TYPE), new Var("object", LIVING_THING))));
        Assert.assertEquals(expectedAntecedents, HashMultiset.create(rule.getAntecedentPatterns()));
        Assert.assertEquals(expectedConsequents, HashMultiset.create(rule.getConsequentPatterns()));
        // Basic pattern matches
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("x"), c(RDF.TYPE), c(LIVING_THING))));
        // Broader patterns match (variables in place of constants)
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("x"), c(RDF.TYPE), new Var("y"))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("x"), new Var("y"), c(LIVING_THING))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("a"), new Var("b"), new Var("c"))));
        // Narrower patterns match (constants in place of variables)
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(RDF.TYPE), c(RDF.TYPE), c(LIVING_THING))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(FOAF.MBOX), c(RDF.TYPE), new Var("y"))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(RDF.ALT), new Var("y"), c(LIVING_THING))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(RDF.BAG), new Var("b"), new Var("c"))));
        // Incompatible patterns don't match (different constants)
        Assert.assertFalse(rule.canConclude(new StatementPattern(new Var("x"), c(RDFS.SUBCLASSOF), new Var("y"))));
        Assert.assertFalse(rule.canConclude(new StatementPattern(new Var("x"), new Var("y"), c(FOAF.PERSON))));
        Assert.assertFalse(rule.canConclude(new StatementPattern(c(RDF.TYPE), c(RDF.TYPE), c(RDF.TYPE))));
    }

    @Test
    public void testThisUnbound() throws Exception {
        String text = "CONSTRUCT {\n"
                + "  ?ind a ?superclass .\n"
                + "} WHERE {\n"
                + "  ?ind a ?subclass .\n"
                + "  ?subclass rdfs:subClassOf ?superclass .\n"
                + "}";
        ParsedGraphQuery query = (ParsedGraphQuery) PARSER.parseQuery(text, null);
        SpinConstructRule rule = new SpinConstructRule(OWL.THING, RL_CAX_SCO, query);
        Multiset<StatementPattern> expectedAntecedents = HashMultiset.create(Arrays.asList(
                new StatementPattern(new Var("subclass"), ac(RDFS.SUBCLASSOF), new Var("superclass")),
                new StatementPattern(new Var("ind"), ac(RDF.TYPE), new Var("subclass"))));
        Multiset<StatementPattern> expectedConsequents = HashMultiset.create(Arrays.asList(
                new StatementPattern(new Var("subject"), new Var("predicate", RDF.TYPE), new Var("object"))));
        Assert.assertEquals(expectedAntecedents, HashMultiset.create(rule.getAntecedentPatterns()));
        Assert.assertEquals(expectedConsequents, HashMultiset.create(rule.getConsequentPatterns()));
        // Basic pattern matches
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("x"), c(RDF.TYPE), new Var("y"))));
        // Broader patterns match (variables in place of constants)
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("a"), new Var("b"), new Var("c"))));
        // Narrower patterns match (constants in place of variables)
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(RDF.TYPE), c(RDF.TYPE), c(RDF.TYPE))));
        // Incompatible patterns don't match (different constants)
        Assert.assertFalse(rule.canConclude(new StatementPattern(new Var("x"), c(RDFS.SUBCLASSOF), new Var("y"))));
    }

    @Test
    public void testMultipleConsequents() throws Exception {
        String text = "CONSTRUCT {\n"
                + "  ?this rdfs:subClassOf ?this .\n"
                + "  ?this owl:equivalentClass ?this .\n"
                + "  ?this rdfs:subClassOf owl:Thing .\n"
                + "  owl:Nothing rdfs:subClassOf ?this .\n"
                + "} WHERE { }";
        ParsedGraphQuery query = (ParsedGraphQuery) PARSER.parseQuery(text, null);
        SpinConstructRule rule = new SpinConstructRule(OWL.CLASS, RL_SCM_CLS, query);
        Multiset<StatementPattern> expectedAntecedents = HashMultiset.create(Arrays.asList(
                new StatementPattern(new Var("this"), c(RDF.TYPE), c(OWL.CLASS))));
        Multiset<StatementPattern> expectedConsequents = HashMultiset.create(Arrays.asList(
                new StatementPattern(new Var("subject"), new Var("predicate", RDFS.SUBCLASSOF), new Var("object")),
                new StatementPattern(new Var("subject"), new Var("predicate", OWL.EQUIVALENTCLASS), new Var("object")),
                new StatementPattern(new Var("subject"), new Var("predicate", RDFS.SUBCLASSOF), new Var("object", OWL.THING)),
                new StatementPattern(new Var("subject", OWL.NOTHING), new Var("predicate", RDFS.SUBCLASSOF), new Var("object"))));
        Assert.assertEquals(expectedAntecedents, HashMultiset.create(rule.getAntecedentPatterns()));
        Assert.assertEquals(expectedConsequents, HashMultiset.create(rule.getConsequentPatterns()));
        // Basic pattern matches
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("x"), c(RDFS.SUBCLASSOF), new Var("y"))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("x"), c(OWL.EQUIVALENTCLASS), new Var("y"))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("x"), c(RDFS.SUBCLASSOF), c(OWL.THING))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(OWL.NOTHING), c(RDFS.SUBCLASSOF), new Var("y"))));
        // Broader patterns match (variables in place of constants)
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("a"), new Var("b"), new Var("c"))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("a"), new Var("b"), c(OWL.THING))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(OWL.NOTHING), new Var("b"), new Var("c"))));
        // Narrower patterns match (constants in place of variables)
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(FOAF.PERSON), c(RDFS.SUBCLASSOF), new Var("x"))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(FOAF.PERSON), c(OWL.EQUIVALENTCLASS), c(FOAF.PERSON))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(OWL.NOTHING), c(RDFS.SUBCLASSOF), c(FOAF.PERSON))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(OWL.NOTHING), c(OWL.EQUIVALENTCLASS), c(FOAF.PERSON))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(OWL.NOTHING), c(OWL.EQUIVALENTCLASS), c(OWL.THING))));
        // Incompatible patterns don't match (different constants)
        Assert.assertFalse(rule.canConclude(new StatementPattern(new Var("x"), c(RDFS.SUBPROPERTYOF), c(OWL.THING))));
    }

    @Test
    public void testGeneralConsequent() throws Exception {
        String text = "CONSTRUCT {\n"
                + "  ?x ?p2 ?y"
                + "} WHERE {\n"
                + "  ?x ?p1 ?y .\n"
                + "  ?p1 rdfs:subPropertyOf ?p2 .\n"
                + "}";
        ParsedGraphQuery query = (ParsedGraphQuery) PARSER.parseQuery(text, null);
        SpinConstructRule rule = new SpinConstructRule(OWL.THING, RL_PRP_SPO1, query);
        Multiset<StatementPattern> expectedAntecedents = HashMultiset.create(Arrays.asList(
                new StatementPattern(new Var("p1"), ac(RDFS.SUBPROPERTYOF), new Var("p2")),
                new StatementPattern(new Var("x"), new Var("p1"), new Var("y"))));
        Multiset<StatementPattern> expectedConsequents = HashMultiset.create(Arrays.asList(
                new StatementPattern(new Var("subject"), new Var("predicate"), new Var("object"))));
        Assert.assertEquals(expectedAntecedents, HashMultiset.create(rule.getAntecedentPatterns()));
        Assert.assertEquals(expectedConsequents, HashMultiset.create(rule.getConsequentPatterns()));
        // Basic pattern matches
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("a"), new Var("b"), new Var("c"))));
        // Narrower patterns match (constants in place of variables)
        Assert.assertTrue(rule.canConclude(new StatementPattern(new Var("x"), c(RDFS.SUBPROPERTYOF), c(OWL.THING))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(OWL.NOTHING), new Var("prop"), c(OWL.THING))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(FOAF.PERSON), c(RDFS.SUBCLASSOF), new Var("x"))));
        Assert.assertTrue(rule.canConclude(new StatementPattern(c(OWL.NOTHING), c(RDFS.SUBCLASSOF), c(FOAF.PERSON))));
    }
}
