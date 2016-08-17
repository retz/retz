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

import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MesosExecutorLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(MesosExecutorLauncher.class);

    public static void main(String... argv) {
        String cwd = System.getProperty("user.dir");
        LOG.info("Executor running on {} with {}", cwd, String.join(" ", argv));
        LOG.info(CPUManager.get().toString());
        RetzExecutor executor = new RetzExecutor();
        MesosExecutorDriver driver = new MesosExecutorDriver(executor);
        LocalProcessManager.start(driver);
        Protos.Status state = driver.run();
        int status = (state == Protos.Status.DRIVER_STOPPED ? 0 : 1);
        System.exit(status);
    }

    public static String getFullClassName() {
        return MesosExecutorLauncher.class.getName();
    }
}
