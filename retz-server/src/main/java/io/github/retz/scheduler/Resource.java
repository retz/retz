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
package io.github.retz.scheduler;

import io.github.retz.protocol.data.Range;
import io.github.retz.protocol.data.ResourceQuantity;
import org.apache.mesos.Protos;

import java.util.LinkedList;
import java.util.List;
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
        this(cpu, memMB, diskMB, 0, new LinkedList<>());
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
        this.ports.sort( (l, r) -> r.getMin() - l.getMin());
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
        return ports.get(ports.size() - 1).getMax();
    }

    public int portAmount() {
        return ports.stream().mapToInt(range -> range.getMax() - range.getMin() + 1).sum();
    }

    public int gpu() {
        return gpu;
    }

    public Resource cut(ResourceQuantity q, int lastPort) {
        return cut(q.getCpu(), q.getMemMB(), q.getGpu(), q.getPorts(), lastPort);
    }

    public Resource cut(int cpu, int memMB, int gpus, int ports, int lastPort) {
        List<Range> ranges = new LinkedList<>();
        int sum = 0;
        for(Range range : this.ports) {
            if (range.getMax() <= lastPort) {
                continue;
            } else if (sum >= ports) {
                break;
            }
            int start = 0;
            if (lastPort < range.getMin()) {
                start = range.getMin();
            } else {
                start = lastPort + 1;
            }
            int end = 0;
            if (range.getMax() - start > ports - sum) {
                end = start + ports - sum - 1;
            } else {
                end = range.getMax();
            }
            sum += (end - start) + 1;
            if (start <= end) {
                ranges.add(new Range(start, end));
            }
        }
        return new Resource(cpu, memMB, 0, gpus, ranges);
    }

    public List<Protos.Resource> construct() {
        List<Protos.Resource> list = new LinkedList<Protos.Resource>();
        list.add(Protos.Resource.newBuilder()
                .setName("cpus")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu).build())
                .setType(Protos.Value.Type.SCALAR)
                .build());
        list.add(Protos.Resource.newBuilder()
                .setName("mem")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(memMB))
                .setType(Protos.Value.Type.SCALAR)
                .build());
        if (diskMB > 0) {
            list.add(Protos.Resource.newBuilder()
                    .setName("disk")
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskMB))
                    .setType(Protos.Value.Type.SCALAR)
                    .build());
        }
        if (gpu > 0) {
            list.add(Protos.Resource.newBuilder()
                    .setName("gpus")
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(gpu))
                    .setType(Protos.Value.Type.SCALAR)
                    .build());
        }
        if (! ports.isEmpty()) {
            list.add(Protos.Resource.newBuilder()
                    .setName("ports")
                    .setRanges(Protos.Value.Ranges.newBuilder().addAllRange(
                            ports.stream().map(range ->
                                    Protos.Value.Range.newBuilder()
                                            .setBegin(range.getMin())
                                            .setEnd(range.getMax())
                                            .build()
                            ).collect(Collectors.toList())).build())
                    .setType(Protos.Value.Type.RANGES)
                    .build());
        }
        return list;
    }

    public ResourceQuantity toQuantity() {
        return new ResourceQuantity((int)cpu, memMB, gpu, portAmount(), diskMB, 0);
    }

    @Override
    public String toString() {
        String portRanges = String.join(", ", ports.stream().map(port -> port.toString()).collect(Collectors.toList()));
        return String.format("cpus=%.1f, mem=%dMB, disk=%dMB, gpu=%d, ports=[%s]", cpu, memMB, diskMB, gpu, portRanges);
    }
}
