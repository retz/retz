/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
package com.asakusafw.retz.executor;

import com.asakusafw.retz.executor.CPUManager;
import org.junit.After;
import org.junit.Test;
import scala.actors.threadpool.Arrays;
import xerial.jnuma.Numa;

import java.util.List;

public class CPUManagerTest {

    @After
    public void after() {
        CPUManager.reset();
    }

    @Test
    public void numaTopologyDetection() {

        if (Numa.isAvailable()) {
            // Only one socket isn't NUMA?
            assert Numa.numNodes() > 0;
            assert Numa.numCPUs() > 1;

            CPUManager manager = CPUManager.get();
            assert manager.availableCPUCount() == Numa.numCPUs();
            assert manager.availableCPUCount() == Runtime.getRuntime().availableProcessors();

        }
    }

    @Test
    public void cpuManagerTest() {
        CPUManager manager = CPUManager.get();

        // Assuming test machine has more than two CPU cores
        List<Integer> cpus = manager.assign("foobar", 2);
        assert cpus.size() == 2;

        manager.free("foobar");
        assert manager.availableCPUCount() == Runtime.getRuntime().availableProcessors();
    }
    @Test
    public void crossNodeTest() {
        Integer[] nodes = {4, 4, 4, 4}; // 4x4 numa structure
        CPUManager.setTopology(Arrays.asList(nodes));

        assert CPUManager.get().availableCPUCount() == 4 * nodes.length;
        System.err.println(CPUManager.get().toString());

        List<Integer> cpus = CPUManager.get().assign("hey><", 7);
        System.err.println("Assigned: [" + CPUManager.listIntToString(cpus) + "]");
        assert cpus.size() == 7;

        CPUManager.get().free("hey><");
        assert CPUManager.get().availableCPUCount() == 4 * nodes.length;
    }
}
