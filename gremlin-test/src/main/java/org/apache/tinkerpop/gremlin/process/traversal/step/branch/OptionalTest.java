/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Pieter Martin
 */
@RunWith(GremlinProcessRunner.class)
public abstract class OptionalTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX2X_optionalXoutXknowsXX(Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX2X_optionalXinXknowsXX(Object v2Id);

    public abstract Traversal<Vertex, Path> get_g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path();

    public abstract Traversal<Vertex, Path> get_g_V_optionalXout_optionalXoutXX_path();

    public abstract Traversal<Vertex, String> get_g_VX1X_optionalXaddVXdogXX_label(Object v1Id);

    public abstract Traversal<Vertex, Map<String, Element>> get_g_VX1_2X_asXaX_optionalXbothE_dedup_asXbXX_chooseXselectXbX_selectXa_bX_projectXaX_byXselectXaXXX(Object v1Id, Object v2Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_optionalXoutXknowsXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX2X_optionalXoutXknowsXX(convertToVertexId(this.graph, "vadas"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(convertToVertex(this.graph, "vadas"), traversal.next());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_optionalXinXknowsXX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX2X_optionalXinXknowsXX(convertToVertexId(this.graph, "vadas"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(convertToVertex(this.graph, "marko"), traversal.next());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path();
        printTraversalForm(traversal);
        List<Path> paths = traversal.toList();
        assertEquals(6, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "vadas")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "josh")) && p.get(2).equals(convertToVertex(this.graph, "ripple")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "josh")) && p.get(2).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "vadas")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "josh")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "peter"))
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_optionalXout_optionalXoutXX_path() {
        final Traversal<Vertex, Path> traversal = get_g_V_optionalXout_optionalXoutXX_path();
        printTraversalForm(traversal);
        List<Path> paths = traversal.toList();
        assertEquals(10, paths.size());
        List<Predicate<Path>> pathsToAssert = Arrays.asList(
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "vadas")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "josh")) && p.get(2).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 3 && p.get(0).equals(convertToVertex(this.graph, "marko")) && p.get(1).equals(convertToVertex(this.graph, "josh")) && p.get(2).equals(convertToVertex(this.graph, "ripple")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "vadas")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "josh")) && p.get(1).equals(convertToVertex(this.graph, "lop")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "josh")) && p.get(1).equals(convertToVertex(this.graph, "ripple")),
                p -> p.size() == 1 && p.get(0).equals(convertToVertex(this.graph, "ripple")),
                p -> p.size() == 2 && p.get(0).equals(convertToVertex(this.graph, "peter")) && p.get(1).equals(convertToVertex(this.graph, "lop"))
        );
        for (Predicate<Path> pathPredicate : pathsToAssert) {
            Optional<Path> path = paths.stream().filter(pathPredicate).findAny();
            assertTrue(path.isPresent());
            assertTrue(paths.remove(path.get()));
        }
        assertTrue(paths.isEmpty());
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void g_VX1X_optionalXaddVXdogXX_label() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_optionalXaddVXdogXX_label(convertToVertexId(this.graph, "marko"));
        printTraversalForm(traversal);
        checkResults(Collections.singletonList("dog"), traversal);
        assertEquals(7L, g.V().count().next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1_2X_asXaX_optionalXbothE_dedup_asXbXX_chooseXselectXbX_selectXa_bX_projectXaX_byXselectXaXXX() {
        final Vertex marko = convertToVertex(this.graph, "marko");
        final Vertex vadas = convertToVertex(this.graph, "vadas");
        final Traversal<Vertex, Map<String, Element>> traversal = get_g_VX1_2X_asXaX_optionalXbothE_dedup_asXbXX_chooseXselectXbX_selectXa_bX_projectXaX_byXselectXaXXX(marko, vadas);
        printTraversalForm(traversal);
        int a = 0, b = 0;
        int expectedA = 4, expectedB = 3;
        while (traversal.hasNext()) {
            final Map<String, Element> m = traversal.next();
            if (m.containsKey("a")) {
                a++;
                assertTrue(m.get("a").equals(marko) || m.get("a").equals(vadas));
            }
            if (m.containsKey("b")) {
                b++;
                assertTrue(m.get("b") instanceof Edge);
                final Edge edge = (Edge) m.get("b");
                assertTrue(edge.outVertex().equals(marko) || edge.outVertex().equals(vadas));
                if (edge.outVertex().equals(vadas)) expectedA = 3;
            }
        }
        assertEquals(expectedA, a);
        assertEquals(expectedB, b);
    }

    public static class Traversals extends OptionalTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_optionalXoutXknowsXX(Object v2Id) {
            return g.V(v2Id).optional(out("knows"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_optionalXinXknowsXX(Object v2Id) {
            return g.V(v2Id).optional(in("knows"));
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path() {
            return g.V().hasLabel("person").optional(out("knows").optional(out("created"))).path();
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_optionalXout_optionalXoutXX_path() {
            return g.V().optional(out().optional(out())).path();
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_optionalXaddVXdogXX_label(Object v1Id) {
            return g.V(v1Id).optional(addV("dog")).label();
        }

        @Override
        public Traversal<Vertex, Map<String, Element>> get_g_VX1_2X_asXaX_optionalXbothE_dedup_asXbXX_chooseXselectXbX_selectXa_bX_projectXaX_byXselectXaXXX(Object v1Id, Object v2Id) {
            return g.V(v1Id, v2Id).as("a").optional(bothE().dedup().as("b"))
                    .choose(__.select("b"),
                            __.<Edge, Element>select("a", "b"),
                            __.<Edge, Element>project("a").by(select("a")));
        }
    }
}
