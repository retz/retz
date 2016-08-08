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
package io.github.retz.scheduler;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * @doc JSON parsing test
 */
public class MesosHTTPFetcherTest {
    @Test
    public void parseTypicalJSON() throws IOException {
        InputStream in;
        Optional<String> s;

        in = MesosHTTPFetcherTest.class.getResourceAsStream("/master-slaves.json");
        s = MesosHTTPFetcher.extractSlaveAddr(in, "6c751ae7-6856-4127-aea1-42f3a9210846-S0");
        assert s.isPresent();
        assert s.get().equals("127.0.0.1:5051");

        in = MesosHTTPFetcherTest.class.getResourceAsStream("/slave-flags.json");
        s = MesosHTTPFetcher.extractSlaveBasePath(in);
        assert s.isPresent();
        assert s.get().equals("/tmp/mesos");

        in = MesosHTTPFetcherTest.class.getResourceAsStream("/slave-state.json");
        s = MesosHTTPFetcher.extractContainerId(in, "3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000", "sum");
        assert s.isPresent();
        assert s.get().equals("927b4c8a-bcfb-40fb-bf24-fcd4a430e2aa");

        in = MesosHTTPFetcherTest.class.getResourceAsStream("/slave-state.json");
        s = MesosHTTPFetcher.extractDirectory(in, "3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000", "sum");
        assert s.isPresent();
        assert s.get().equals("/tmp/mesos/slaves/6c751ae7-6856-4127-aea1-42f3a9210846-S0/frameworks/3a3e9491-84a5-4c9d-8fed-5ca10c23d922-0000/executors/sum/runs/927b4c8a-bcfb-40fb-bf24-fcd4a430e2aa");
    }
}
