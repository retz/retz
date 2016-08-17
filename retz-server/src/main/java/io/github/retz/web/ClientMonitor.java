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
package io.github.retz.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

public class ClientMonitor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ClientMonitor.class);

    private final int interval_sec;
    private boolean stop = false;
    public ClientMonitor (int interval_sec){
        assert interval_sec > 1;
        this.interval_sec = interval_sec;
    }
    public void stop() {
        this.stop = true;
    }
    @Override
    public void run() {
        LOG.info("Starting client monitor thread with interval {} seconds", interval_sec);
        int counter = 1; // Magic start number
        while (!stop) {
            try {
                sleep(1000);
            } catch (InterruptedException e) {
            }
            if (counter % interval_sec == 0) {
                ConsoleWebSocketHandler.sendPingAll();
            }
            counter = (counter+1) % interval_sec;
        }
    }
}
