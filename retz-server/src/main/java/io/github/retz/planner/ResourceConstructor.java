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
package io.github.retz.planner;

import io.github.retz.planner.spi.Resource;
import io.github.retz.protocol.data.Range;
import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ResourceConstructor {
    public static List<Protos.Resource> construct(Resource r) {
        List<Protos.Resource> list = new ArrayList<Protos.Resource>();
        list.add(Protos.Resource.newBuilder()
                .setName("cpus")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(r.cpu()).build())
                .setType(Protos.Value.Type.SCALAR)
                .build());
        list.add(Protos.Resource.newBuilder()
                .setName("mem")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(r.memMB()))
                .setType(Protos.Value.Type.SCALAR)
                .build());
        if (r.diskMB() > 0) {
            list.add(Protos.Resource.newBuilder()
                    .setName("disk")
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(r.diskMB()))
                    .setType(Protos.Value.Type.SCALAR)
                    .build());
        }
        if (r.gpu() > 0) {
            list.add(Protos.Resource.newBuilder()
                    .setName("gpus")
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(r.gpu()))
                    .setType(Protos.Value.Type.SCALAR)
                    .build());
        }
        if (! r.ports().isEmpty()) {
            list.add(Protos.Resource.newBuilder()
                    .setName("ports")
                    .setRanges(Protos.Value.Ranges.newBuilder().addAllRange(
                            r.ports().stream().map(range ->
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

    public static List<Protos.Resource> construct(int cpus, int memMB) {
        List<Protos.Resource> list = new ArrayList<Protos.Resource>();
        list.add(Protos.Resource.newBuilder()
                .setName("cpus")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build())
                .setType(Protos.Value.Type.SCALAR)
                .build());
        list.add(Protos.Resource.newBuilder()
                .setName("mem")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(memMB))
                .setType(Protos.Value.Type.SCALAR)
                .build());
        return list;
    }

    public static List<Protos.Resource> construct(int cpus, int memMB, int disk, int gpu) {
        List<Protos.Resource> list = construct(cpus, memMB);
        list.add(buildDisk(disk));
        list.add(Protos.Resource.newBuilder()
                .setName("gpus")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(gpu))
                .setType(Protos.Value.Type.SCALAR)
                .build());
        return list;
    }

    public static Protos.Resource buildDisk(int sizeMB) {
        return Protos.Resource.newBuilder()
                .setName("disk")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(sizeMB))
                .setType(Protos.Value.Type.SCALAR)
                .build();
    }

    public static Resource decode(List<Protos.Resource> resources) {
        int memMB = 0, diskMB = 0, gpu = 0;
        List<Range> ports = new ArrayList<>();
        double cpu = 0;
        for (Protos.Resource r : resources) {
            if (r.getName().equals("cpus")) {
                // Will be screwed up if we have reserved CPUs
                cpu = r.getScalar().getValue();
            } else if (r.getName().equals("mem")) {
                // Will be screwed up if we have reserved CPUs
                memMB = (int) (r.getScalar().getValue());
            } else if (r.getName().equals("disk")) {
                int size = (int) (r.getScalar().getValue());
                diskMB = size;
            } else if (r.getName().equals("ports")) {
                 for(Protos.Value.Range range : r.getRanges().getRangeList()) {
                     ports.add(new Range((int)range.getBegin(), (int)range.getEnd()));
                 }
            } else if (r.getName().equals("gpus")) {
                gpu = (int) (r.getScalar().getValue());
            }
        }
        Resource ret = new Resource(cpu, memMB, diskMB, gpu, ports);
        return ret;
    }
}
