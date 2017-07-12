/**
 *    Retz
 *    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
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
package io.github.retz.planner.spi;

import io.github.retz.protocol.data.Range;
import io.github.retz.protocol.data.ResourceQuantity;

import java.util.*;
import java.util.stream.Collectors;

// TODO: overall cleanup is needed; is this is just for counting the amount of resources?
// This is because non-scalar resources conflicts (which is not commutative), while scalar resources does not conflict
// Otherwise, merge should be done only for the *same* slave, which is not clear
public class Resource {

    private double cpu;
    private int memMB;
    private int gpu;
    private List<Range> ports;
    private int diskMB;

    public Resource(double cpu, int memMB, int diskMB) {
        this(cpu, memMB, diskMB, 0, new ArrayList<>());
    }

    public Resource(double cpu, int memMB, int diskMB, int gpu, List<Range> ports) {
        this.cpu = cpu;
        this.memMB = memMB;
        this.ports = ports;
        this.diskMB = diskMB;
        this.gpu = gpu;
    }

    //Shouldn't do merge, which should be done by Mesos
    public void merge(Resource rhs) {
        this.cpu += rhs.cpu();
        this.memMB += rhs.memMB();
        this.diskMB += rhs.diskMB();
        for (Range lhs : ports) {
            for (Range r : rhs.ports()) {
                if (lhs.overlap(r)) {
                    throw new RuntimeException("Port range overlapping: " + lhs + " and " + r);
                }
            }
        }
        this.ports.addAll(rhs.ports());
        this.ports.sort((l, r) -> (int) (r.getMin() - l.getMin()));
        this.gpu += rhs.gpu();
    }

    public double cpu() {
        return cpu;
    }

    public int memMB() {
        return memMB;
    }

    public int diskMB() {
        return diskMB;
    }

    public List<Range> ports() {
        return ports;
    }

    public int lastPort() {
        if (ports.isEmpty()) {
            return 0;
        }
        return (int) ports.get(ports.size() - 1).getMax();
    }

    public int portAmount() {
        return (int) ports.stream().mapToLong(range -> range.getMax() - range.getMin() + 1).sum();
    }

    public int gpu() {
        return gpu;
    }

    public Resource cut(ResourceQuantity q, int lastPort) {
        return cut(q.getCpu(), q.getMemMB(), q.getDiskMB(), q.getGpu(), q.getPorts(), lastPort);
    }
    public Resource cut(int cpu, int memMB, int gpus, int ports, int lastPort) {
        return cut(cpu, memMB, 0, gpus, ports, lastPort);
    }
    public Resource cut(int cpu, int memMB, int diskMB, int gpus, int ports, int lastPort) {
        List<Range> ranges = new ArrayList<>();
        int sum = 0;
        for (Range range : this.ports) {
            if (range.getMax() <= lastPort) {
                continue;
            } else if (sum >= ports) {
                break;
            }
            int start = 0;
            if (lastPort < range.getMin()) {
                start = (int) range.getMin();
            } else {
                start = lastPort + 1;
            }
            int end = 0;
            if (range.getMax() - start > ports - sum) {
                end = start + ports - sum - 1;
            } else {
                end = (int) range.getMax();
            }
            sum += (end - start) + 1;
            if (start <= end) {
                ranges.add(new Range(start, end));
            }
        }

        this.cpu -= cpu;
        this.memMB -= memMB;
        this.diskMB -= diskMB;
        this.gpu -= gpus;

        List<Range> newPorts = this.ports.stream().map( (r) -> {
            List<Range> remain = new LinkedList<>();
            for (Range range : ranges) {
                List<Range> s = r.subtract(range);
                if (! (s.size() == 1 && s.get(0).equals(r))) {
                    //System.err.println(r + remain.getClass().getName());
                    //System.err.println(s.stream().map( (ra) -> ra.toString()).collect(Collectors.joining(",")));
                    remain.addAll(s);
                }
            }
            return remain;
        }).reduce((lhs, rhs) -> {
            lhs.addAll(rhs);
            return lhs;
        }).orElse(Collections.emptyList());
        this.ports = newPorts;

        return new Resource(cpu, memMB, diskMB, gpus, ranges);
    }


    public ResourceQuantity toQuantity() {
        return new ResourceQuantity((int) cpu, memMB, gpu, portAmount(), diskMB, 0);
    }

    @Override
    public String toString() {
        String portRanges = ports.stream().map(port -> port.toString()).collect(Collectors.joining(", "));
        return String.format("cpus=%.1f, mem=%dMB, disk=%dMB, gpu=%d, ports=[%s]", cpu, memMB, diskMB, gpu, portRanges);
    }
}
