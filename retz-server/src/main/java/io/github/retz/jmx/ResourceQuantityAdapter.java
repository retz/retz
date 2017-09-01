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

import io.github.retz.bean.ResourceQuantityMXBean;
import io.github.retz.protocol.StatusResponse;
import io.github.retz.protocol.data.ResourceQuantity;
import io.github.retz.web.StatusCache;

import java.util.function.Function;

public class ResourceQuantityAdapter implements ResourceQuantityMXBean {
    private final Function<StatusResponse, ResourceQuantity> resourceGetter;

    public ResourceQuantityAdapter(Function<StatusResponse, ResourceQuantity> resourceGetter) {
        this.resourceGetter = resourceGetter;
    }

    @Override
    public int getCPU() {
        return getResourceQuantity().getCpu();
    }

    @Override
    public int getDiskMB() {
        return getResourceQuantity().getDiskMB();
    }

    @Override
    public int getGPU() {
        return getResourceQuantity().getGpu();
    }

    @Override
    public int getMemMB() {
        return getResourceQuantity().getMemMB();
    }

    @Override
    public int getNodes() {
        return getResourceQuantity().getNodes();
    }

    @Override
    public int getPorts() {
        return getResourceQuantity().getPorts();
    }

    private ResourceQuantity getResourceQuantity() {
        return resourceGetter.apply(StatusCache.getRawStatusResponse());
    }

    public static ResourceQuantityAdapter newTotalOfferedQuantityAdapter() {
        Function<StatusResponse, ResourceQuantity> getTotalOfferedFun = (status) -> status.totalOffered();
        return new ResourceQuantityAdapter(getTotalOfferedFun);
    }

    public static ResourceQuantityAdapter newTotalUsedQuantityAdapter() {
        Function<StatusResponse, ResourceQuantity> getTotalUsedFun = (status) -> status.totalUsed();
        return new ResourceQuantityAdapter(getTotalUsedFun);
    }
}
