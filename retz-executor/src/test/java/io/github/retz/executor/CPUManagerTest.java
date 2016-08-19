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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.actors.threadpool.Arrays;
import xerial.jnuma.Numa;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CPUManagerTest {

    @Before
    public void before() {
    }

    @After
    public void after() {
        CPUManager.reset();
    }

    @Test
    public void numaTopologyDetection() {
        CPUManager manager = CPUManager.get();
        if (Numa.isAvailable()) {
            // Only one socket isn't NUMA?
            assertTrue(Numa.numNodes() > 0);
            assertTrue(Numa.numCPUs() > 1);

            assertEquals(Numa.numCPUs(), manager.availableCPUCount());
        }
        assertEquals(Runtime.getRuntime().availableProcessors(), manager.availableCPUCount());
    }

    @Test
    public void cpuManagerTest() {
        CPUManager manager = CPUManager.get();

        // Assuming test machine has more than two CPU cores
        List<Integer> cpus = manager.assign("foobar", 2);
        assertEquals(cpus.size(), 2);

        manager.free("foobar");

        assertEquals(Runtime.getRuntime().availableProcessors(), manager.availableCPUCount());
    }

    @Test
    public void crossNodeTest() {
        Integer[] nodes = {4, 4, 4, 4}; // 4x4 numa structure
        CPUManager.setTopology(Arrays.asList(nodes));

        assertEquals(4 * nodes.length, CPUManager.get().availableCPUCount());
        System.err.println(CPUManager.get().toString());

        List<Integer> cpus = CPUManager.get().assign("hey><", 7);
        System.err.println("Assigned: [" + CPUManager.listIntToString(cpus) + "]");
        assertEquals(7, cpus.size());

        CPUManager.get().free("hey><");
        assertEquals(4 * nodes.length, CPUManager.get().availableCPUCount());
    }

    @Test
    public void notEnough () {
        Integer[] nodes = {4, 4, 4, 4};
        CPUManager.setTopology(Arrays.asList(nodes));

        List<Integer> cpus = CPUManager.get().assign("not-enough", 32);
        assertTrue(cpus.isEmpty());
    }
}
