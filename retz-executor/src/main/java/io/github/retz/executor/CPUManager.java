/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xerial.jnuma.Numa;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

public class CPUManager {
    private static final Logger LOG = LoggerFactory.getLogger(CPUManager.class);

    private static final CPUManager MANAGER = new CPUManager();

    private List<Node> nodes;

    public static CPUManager get() {
        return MANAGER;
    }

    private CPUManager() {
        init();
    }

    private void init() {
        this.nodes = new LinkedList<>();
        LOG.info(Numa.numNodes() + " nodes, " + Numa.numCPUs() + " CPUs + Numa available: " + Numa.isAvailable());

        if (!Numa.isAvailable()) {
            LOG.debug("Handling this node as non-NUMA system.");
            Node node = new Node(0);
            for (int i = 0; i < Numa.numCPUs(); ++i) {
                node.cpus.add(i);
            }
            nodes.add(node);

        } else {
            LOG.debug("Handling this node as NUMA system.");

            for (int n = 0; n < Numa.numNodes(); ++n) {
                Node node = new Node(n);
                long[] masks = Numa.nodeToCpus(n);

                for (int i = 0; i < masks.length; ++i) {
                    LOG.debug(n + ":> " + masks[i]);
                    for (int j = 0; j < 64; ++j) {
                        if (((masks[i] >> j) & 1) > 0) {
                            node.cpus.add(j + i * 8);
                            LOG.debug("Numa node (" + n + ")" + ", Local CPU: " + (j + i * 8));
                        }
                    }
                }
                nodes.add(node);
            }
            LOG.info("NUMA topology in this system: " + this.toString());

            if (availableCPUCount() != Numa.numCPUs()) {
                LOG.error("Number of total CPUs does not match.");
                throw new AssertionError();
            }
        }
    }

    public int availableCPUCount() {
        return nodes.stream().mapToInt(n -> n.cpus.size()).sum();
    }

    // REVIEW: This method call has race with self and free method calls.
    // TODO: optimization; find out best task assignment regarding locality
    public List<Integer> assign(String taskID, int numCpus) {
        if (availableCPUCount() < numCpus) {
            return new ArrayList<>();
        }

        for (Node node : nodes) {
            if (node.used.containsKey(taskID)) {
                // Tasks with the same name must not exist
                // REVIEW: It's possible that other node instance has been modified
                // prior in loop for "nodes". In current implementation, it is
                // "node.cpus.remove(0)" loop below, those cpus are lost.
                // I guess this if statement will not be true, but code with
                //  corruption of static data looks horrible.
                throw new InvalidParameterException();
            }
            if (node.cpus.size() < numCpus) {
                continue;
            }
            List<Integer> list = new LinkedList<>();
            for (int n = numCpus; n > 0; --n) {
                list.add(node.cpus.remove(0));
            }
            node.used.put(taskID, list); // TODO: this could be immutable list
            return list;
        }

        // Couldn't find bunch of cores in a single NUMA node; finding them out from
        // multiple NUMA node. While there might be several strategies, Retz tries
        // to gather all cores as concentrated as possible
        List<Integer> list = new LinkedList<>();
        for (Node node : nodes) {
            List<Integer> list1 = new LinkedList<>();
            while (!node.cpus.isEmpty()) {
                list1.add(node.cpus.remove(0));
                if (list.size() + list1.size() >= numCpus) {
                    list.addAll(list1);
                    node.used.put(taskID, list1);
                    return list;
                }
            }
            list.addAll(list1);
            node.used.put(taskID, list1);
        }
        // Nothing found; returning empty CPU list. Or an AssertionError?
        return new ArrayList();
    }

    public void free(String pid) {
        for (Node node : nodes) {
            if (node.used.containsKey(pid)) {
                node.cpus.addAll(node.used.remove(pid));
                //node.cpus = node.cpus.stream().sorted().collect(Collectors.toList());
            }
        }
        return;
    }

    @Override
    public String toString() {
        List<String> nodeStrings = nodes.stream().map(node -> {
            StringBuilder b = new StringBuilder();
            b.append("Node ")
                    .append(node.id)
                    .append(": free=[");
            b.append(CPUManager.listIntToString(node.cpus))
                    .append("], used={");
            for (Map.Entry<String, List<Integer>> m : node.used.entrySet()) {
                b.append(m.getKey())
                        .append("=(")
                        .append(CPUManager.listIntToString(m.getValue()))
                        .append("), ");
            }
            b.append("}");
            return b.toString();
        }).collect(Collectors.toList());
        return String.join("\n", nodeStrings);
    }

    public static String listIntToString(List<Integer> list) {
        return String.join(", ", list.stream().map(i -> Integer.toString(i)).collect(Collectors.toList()));
    }

    static private class Node {
        int id;
        List<Integer> cpus; // Available CPUs
        Map<String, List<Integer>> used;

        Node(int id) {
            this.id = id;
            this.cpus = new LinkedList<>();
            this.used = new LinkedHashMap<>();
        }
    }

    // For tests
    static void setTopology(List<Integer> topology) {
        List<Node> nodes = new LinkedList<>();
        int base = 0;
        for (int i = 0; i < topology.size(); ++i) {
            Node node = new Node(i);
            for (int j = 0; j < topology.get(i); j++) {
                node.cpus.add(j + base);
            }
            base += topology.get(i);
            nodes.add(node);
        }
        MANAGER.nodes = nodes;
    }

    static void reset() {
        MANAGER.init();
    }
}
