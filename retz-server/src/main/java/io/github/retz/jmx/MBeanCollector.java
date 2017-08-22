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

import io.github.retz.misc.Pair;
import io.github.retz.protocol.StatusResponse;
import io.github.retz.protocol.data.ResourceQuantity;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class MBeanCollector {
    private static final String JMX_DOMAIN = "io.github.retz";
    static final String JMX_NAME_STATUS = JMX_DOMAIN + ":type=Stats,name=Status";
    static final String JMX_NAME_TOTAL_OFFERED = JMX_DOMAIN + ":type=Stats,name=TotalOffered";
    static final String JMX_NAME_TOTAL_USED = JMX_DOMAIN + ":type=Stats,name=TotalUsed";
    public static List<Pair<Object, String>> beans() {
        List<Pair<Object, String>> beans = new ArrayList<>();
        beans.add(new Pair<>(new StatusAdapter(), JMX_NAME_STATUS));

        Function<StatusResponse, ResourceQuantity> getTotalOfferedFun = (status) -> status.totalOffered();
        beans.add(new Pair<>( new ResourceQuantityAdapter(getTotalOfferedFun) , JMX_NAME_TOTAL_OFFERED));

        Function<StatusResponse, ResourceQuantity> getTotalUsedFun = (status) -> status.totalUsed();
        beans.add(new Pair<>(new ResourceQuantityAdapter(getTotalUsedFun), JMX_NAME_TOTAL_USED));

        return beans;
    }
}
