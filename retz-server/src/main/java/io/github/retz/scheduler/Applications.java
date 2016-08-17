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
package io.github.retz.scheduler;

import org.apache.commons.io.FilenameUtils;
import org.apache.mesos.Protos;
import io.github.retz.mesos.Resource;
import io.github.retz.mesos.ResourceConstructor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Applications {

    private static final Map<String, Application> DICTIONARY = new ConcurrentHashMap<>();

    private static Map<String, Application> get() {
        return DICTIONARY;
    }

    public static Optional<Application> get(String appName) {
        return Optional.ofNullable(get().get(appName));
    }

    public static List<String> volumes(String role) {
        return (DICTIONARY.entrySet().stream()
                .map( entry -> entry.getValue().toVolumeId(role))
                .collect(Collectors.toList()));
    }

    public static boolean load(io.github.retz.protocol.Application app) {
        Application application = new Application();
        application.appName = app.getAppid();
        application.persistentFiles = app.getPersistentFiles();
        application.appFiles = app.getFiles();
        application.diskMB = app.getDiskMB();

        DICTIONARY.putIfAbsent(application.appName, application);
        return true;
    }

    public static io.github.retz.protocol.Application encodable(Application app) {
        io.github.retz.protocol.Application encodable = new io.github.retz.protocol.Application(
                app.appName, app.persistentFiles, app.appFiles, app.diskMB);
        return encodable;
    }

    public static void unload(String appName) {
        get().remove(appName);
        // Volumes are destroyed lazily
    }

    public static List<Application> getAll() {
        List<Application> apps = new LinkedList<>();
        apps.addAll(get().values());
        return apps;
    }

    public static List<Application> needsPersistentVolume(Resource resource, String role) {
        List<Application> apps = DICTIONARY.entrySet().stream()
                .filter(entry -> !entry.getValue().persistentFiles.isEmpty())
                .filter(entry -> entry.getValue().diskMB.isPresent())
                .filter(entry -> !resource.volumes().containsKey(entry.getValue().toVolumeId(role)))
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());
        return apps;
    }

    public static class Application {
        public String appName;
        public List<String> persistentFiles;
        public List<String> appFiles;
        public Optional<Integer> diskMB;

        public Protos.ExecutorInfo toExecutorInfo(Protos.FrameworkID frameworkID) {

            String jarFile = FilenameUtils.getName(RetzScheduler.getJarUri());

            // COMMENT: Yes, this jarFile is actually a fat jar and could be executable, but
            //          I didn't want to mess up the build script any further
            // REVIEW: if only one jar file is on classpath, you can use "executable jar" file
            String cmd = "java -jar " + jarFile;
            Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder()
                    .setEnvironment(Protos.Environment.newBuilder()
                            .addVariables(Protos.Environment.Variable.newBuilder()
                                    .setName("ASAKUSA_HOME").setValue(".").build()))
                    .setValue(cmd)
                    .setShell(true)
                    .addUris(Protos.CommandInfo.URI.newBuilder().setValue(RetzScheduler.getJarUri()).setCache(false)); // In production, set this true

            for (String file : appFiles) {
                commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(true));
            }

            Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                    .setCommand(commandInfoBuilder.build())
                    .setExecutorId(Protos.ExecutorID.newBuilder().setValue(appName).build())
                    .setFrameworkId(frameworkID)
                    .build();
            return executorInfo;
        }

        public String toVolumeId(String role) {
            return io.github.retz.protocol.Application.toVolumeId(appName);
        }
    }
}
