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


import io.github.retz.planner.spi.Planner;
import io.github.retz.scheduler.PlannerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ExtensiblePlannerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ExtensiblePlannerFactory.class);

    private static URLClassLoader classLoader = null;

    public static Planner create(String name, String classpath) throws Throwable {
        // Load!! Dynamically!!
        URL[] urls = findJarFiles(classpath);
        if (classLoader == null) {
            classLoader = URLClassLoader.newInstance(urls, ClassLoader.getSystemClassLoader());
        } else {
            LOG.warn("Classloader is somehow already not null");
        }
        Class plannerClass = Class.forName(name, true, classLoader);
        if (io.github.retz.planner.spi.Planner.class.isAssignableFrom(plannerClass)) {
            return (Planner)plannerClass.newInstance();
        }
        throw new IllegalArgumentException(name + " does not implement retz.spi.Planner");
    }

    static URL[] findJarFiles(String dir) throws URISyntaxException, MalformedURLException {
        File[] files = new File(dir).listFiles();
        List<URI> jars = new LinkedList<>();

        if (files == null) {
            // Directory does not exist
            LOG.warn("Directory {} as retz.classpath does not denote anything", dir);
            return new URL[]{};
        }

        for(File file : files) {
            if (file.isFile() && file.getName().endsWith(".jar")) {
                jars.add(file.toURI());
            } else if (file.isDirectory()) {
                URL[] urls = findJarFiles(file.getAbsolutePath());
                for (URL url : urls) {
                    jars.add(url.toURI());
                }
            }
        }
        return (URL[])jars.stream().map(uri -> {
            try {
                return uri.toURL();
            }catch (MalformedURLException e){
                LOG.error("Cannot convert URI {} to URL", uri, e);
                return null;
            }
        }).collect(Collectors.toList()).toArray(new URL[]{});
    }
}
