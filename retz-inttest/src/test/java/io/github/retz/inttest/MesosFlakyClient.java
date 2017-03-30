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
package io.github.retz.inttest;

import io.github.retz.protocol.exception.JobNotFoundException;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MesosFlakyClient {
    static String getTaskState(URI uri, String taskId) throws JobNotFoundException {
        for (Map.Entry<String, Object> entry : getTask(uri, taskId).entrySet()) {
            if (entry.getKey().equals("state")) {
                return (String) entry.getValue();
            }
        }
        throw new JobNotFoundException(-2024);
    }

    static Map<String, Object> getTask(URI uri, String taskId) throws JobNotFoundException {
        Objects.requireNonNull(taskId);
        Objects.requireNonNull(uri);
        MesosFlakyClientInterface mesosClient = MesosFlakyClientInterface.connect(uri);
        Map<String, Object> tasks = mesosClient.tasks(1024, 0, "desc");
        //System.err.println("Tasks: " + tasks + " real task id=" + taskId);
        List list = (List)tasks.get("tasks");
        //System.err.println("Task list length: " + list.size() + " " + tasks.size());
        for (Object o: list) {
            System.err.println("task: " + o);
            Map<String, Object> task = (Map<String, Object>)o;
            //for (Map.Entry<String, Object> entry : task.entrySet()) {
            //System.err.println(entry.getKey() + "=>" + entry.getValue());
            //}
            String tid = (String)task.get("id");
            if (tid != null && taskId.equals(tid)) {
                return task;
                //String state = (String)task.get("state");
                //System.err.println("task id=" + taskId + " state=" + state);
            }
        }
        System.err.println(taskId + "is not found in " + tasks);
        throw new JobNotFoundException(-1);
    }
}
