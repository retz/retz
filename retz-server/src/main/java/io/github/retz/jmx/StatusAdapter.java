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
package io.github.retz.jmx;

import io.github.retz.bean.StatusMXBean;
import io.github.retz.protocol.StatusResponse;
import io.github.retz.web.StatusCache;

public class StatusAdapter implements StatusMXBean {
    @Override
    public String getMaster() {
        return getStatusResponse().master().orElse("");
    }

    @Override
    public int getNumSlaves() {
        return getStatusResponse().numSlaves();
    }

    @Override
    public int getOffers() {
        return getStatusResponse().offers();
    }

    @Override
    public int getQueueLength() {
       return getStatusResponse().queueLength();
    }

    @Override
    public int getRunningLength() {
        return getStatusResponse().runningLength();
    }

    @Override
    public String getServerVersion() {
        return getStatusResponse().serverVersion();
    }

    @Override
    public String getStatus() {
        return getStatusResponse().status();
    }

    @Override
    public String getVersion() {
        return getStatusResponse().version();
    }

    private StatusResponse getStatusResponse() {
        return StatusCache.getRawStatusResponse();
    }
}
