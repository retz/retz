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
package io.github.retz.localexecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class EnvBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(EnvBuilder.class);

    private Map<String, String> env;
    private int cpu;
    private int memMB;
    private String pv;

    public EnvBuilder(int cpu, int memMB, String pv) {
        this.env = new HashMap<>();
        this.cpu = cpu;
        this.memMB = memMB;
        this.pv = pv;
        env.put("RETZ_CPU", Integer.toString(this.cpu));
        env.put("RETZ_MEM", Integer.toString(this.memMB));
        env.put("RETZ_PVNAME", this.pv);
    }

    public void putAll(Properties props) {
        for (Map.Entry entry : props.entrySet()) {
            env.put((String) entry.getKey(), (String) entry.getValue());
        }
    }

    public void put(String key, String value) {
        env.put(key, value);
    }

    public Map<String, String> build() {
        Map<String, String> ret = new HashMap<>();
        String cpuString = Integer.toString(this.cpu);
        String memString = Integer.toString(this.memMB);
        // apply all logic here
        for (Map.Entry<String, String> entry : env.entrySet()) {
            // replace $(CPU) => cpu
            // replace $(MEM) => memMB
            // replace $(PV)  => pv
            String value = entry.getValue()
                    .replaceAll("\\$RETZ_CPU", cpuString)
                    .replaceAll("\\$RETZ_MEM", memString)
                    .replaceAll("\\$RETZ_PVNAME", this.pv);
            ret.put(entry.getKey(), value);
        }
        LOG.info("env: {}", this);
        return ret;
    }

    @Override
    public String toString() {
        List<String> list = env.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.toList());
        return String.join(", ", list);
    }
}
