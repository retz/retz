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

import io.github.retz.db.Database;
import io.github.retz.web.WebConsole;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownThread.class);

    private SchedulerDriver driver;

    public ShutdownThread(SchedulerDriver driver) {
        this.driver = driver;
    }
    public void run() {
        LOG.info("Retz shutting down");
        // TODO: graceful stop
        // Close up all incoming requests to prevent database update
        WebConsole.stop();
        // Close all database connections; it may take time
        Database.getInstance().stop();
        // Shut down connection to Mesos; always will be preserved for failover
        // true - possibly connect with same FrameworkID, Mesos doesn't collect tasks
        // false - Same FrameworkID will never connect again, Mesos would refuse reconnection
        driver.stop(true);
        LOG.info("All clean up finished");
    }
}
