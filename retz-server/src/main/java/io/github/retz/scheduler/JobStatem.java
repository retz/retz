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

import io.github.retz.misc.Pair;
import io.github.retz.protocol.data.Job;
import org.apache.mesos.Protos;

public class JobStatem {

    /**
     * State diagram of Events and Job to callback action method name
     * <p>
     * Events   \ in Retz    | QUEUED   | STARTING | STARTED  | FINISHED | KILLED
     * -----------------------+----------+----------+----------+----------+----------
     * TASK_FINISHED_VALUE: | finished | finished | finished | noop     | never
     * TASK_ERROR_VALUE:    | failed   | failed   | failed   | never    | noop
     * TASK_FAILED_VALUE:   | ^^       | ^^       | ^^       | ^^       | noop
     * TASK_KILLED_VALUE:   | ^^       | ^^       | ^^       | ^^       | noop
     * TASK_LOST_VALUE:     | noop     | retry    | retry    | ^^       | noop
     * TASK_KILLING_VALUE:  | ^^       | noop     | noop     | noop     | noop
     * TASK_RUNNING_VALUE:  | started  | started  | started  | noop     | noop
     * TASK_STAGING_VALUE:  | noop     | noop     | noop     | noop     | noop
     * TASK_STARTING_VALUE: | starting | starting | noop     | noop     | noop
     * TASK_DROPPED         | noop     | retry    | retry    | noop     | noop
     * TASK_UNREACHABLE     | noop     | log      | log      | noop     | noop
     * TASK_GONE            | noop     | retry    | retry    | noop     | noop
     * TASK_GONE_BY_OPERATOR| noop     | log      | log      | noop     | noop
     * TASK_UNKNOWN         | noop     | log      | log      | noop     | noop
     * default              | log      | log      | log      | never    | never
     * (kill from user*     | killed   | killed   | killed   | noop     | noop)
     * <p>
     * 'kill from user' is at JobRequestHandler::kill
     * TODO: Actual implementation is still different from this spec; fix it
     * TODO: Add QuickCheck test to this state diagram
     * TODO: Update Events periodically from mesos.proto (last updated on Jan 13 2017)
     *
     * State transitions:
     * finished =&gt; FINISHED
     * failed =&gt; KILLED
     * retry =&gt; QUEUED
     * noop =&gt; keep
     * never =&gt; assert! could be a bug or race; write an error log
     * log =&gt; unexpected; just write a warn log?
     * started =&gt; STARTED
     * starting =&gt; STARTING
     * killed =&gt; KILLED
     *
     * @param job The job that requires some action (state)
     * @param taskState Task state change (event)
     * @return an Action that should be taken (action, indicates next state)
     **/
    public static Action handleCall(Job job, Protos.TaskState taskState) {
        switch (job.state()) {
            case CREATED:
                break;
            case QUEUED:
                return handleQueued(taskState);
            case STARTING:
            case STARTED:
                return handleStart(job, taskState);
            case FINISHED:
                return handleFinished(taskState);
            case KILLED:
                return handleKilled(taskState);
            default:
                break;
        }
        throw new AssertionError("Strange job state:" + job.state());
    }

    static Action handleQueued(Protos.TaskState taskState) {
        switch (taskState.getNumber()) {

            case Protos.TaskState.TASK_FINISHED_VALUE:
                return Action.FINISHED;

            case Protos.TaskState.TASK_ERROR_VALUE:
            case Protos.TaskState.TASK_FAILED_VALUE:
            case Protos.TaskState.TASK_KILLED_VALUE:
                return Action.FAILED;

            case Protos.TaskState.TASK_LOST_VALUE:
            case Protos.TaskState.TASK_KILLING_VALUE:
                return Action.NOOP;

            case Protos.TaskState.TASK_RUNNING_VALUE:
                return Action.STARTED;

            case Protos.TaskState.TASK_STAGING_VALUE:
                return Action.NOOP;

            case Protos.TaskState.TASK_STARTING_VALUE:
                return Action.STARTING;

            case Protos.TaskState.TASK_DROPPED_VALUE:
            case Protos.TaskState.TASK_UNREACHABLE_VALUE:
            case Protos.TaskState.TASK_GONE_VALUE:
            case Protos.TaskState.TASK_GONE_BY_OPERATOR_VALUE:
            case Protos.TaskState.TASK_UNKNOWN_VALUE:
                return Action.NOOP;

            default:
                return Action.LOG;
        }
    }

    static Action handleStart(Job job, Protos.TaskState taskState) {
        switch (taskState.getNumber()) {
            case Protos.TaskState.TASK_FINISHED_VALUE:
                return Action.FINISHED;

            case Protos.TaskState.TASK_ERROR_VALUE:
            case Protos.TaskState.TASK_FAILED_VALUE:
            case Protos.TaskState.TASK_KILLED_VALUE:
                return Action.FAILED;

            case Protos.TaskState.TASK_LOST_VALUE:
                return Action.RETRY;

            case Protos.TaskState.TASK_KILLING_VALUE:
                return Action.NOOP;

            case Protos.TaskState.TASK_RUNNING_VALUE:
                return Action.STARTED;

            case Protos.TaskState.TASK_STAGING_VALUE:
                return Action.NOOP;

            case Protos.TaskState.TASK_STARTING_VALUE:
                if (job.state() == Job.JobState.STARTING) {
                    return Action.STARTING;
                } else if (job.state() == Job.JobState.STARTED){
                    return Action.NOOP;
                } else {
                    return Action.LOG;
                }

            case Protos.TaskState.TASK_DROPPED_VALUE:
                return Action.RETRY;

            case Protos.TaskState.TASK_UNREACHABLE_VALUE:
                return Action.LOG;

            case Protos.TaskState.TASK_GONE_VALUE:
                return Action.RETRY;

            case Protos.TaskState.TASK_GONE_BY_OPERATOR_VALUE:
            case Protos.TaskState.TASK_UNKNOWN_VALUE:
            default:
                return Action.LOG;
        }
    }

    static Action handleFinished(Protos.TaskState taskState) {
        switch (taskState.getNumber()) {
            case Protos.TaskState.TASK_FINISHED_VALUE:
                return Action.NOOP;

            case Protos.TaskState.TASK_ERROR_VALUE:
            case Protos.TaskState.TASK_FAILED_VALUE:
            case Protos.TaskState.TASK_KILLED_VALUE:
                return Action.NEVER;

            case Protos.TaskState.TASK_LOST_VALUE:
            case Protos.TaskState.TASK_KILLING_VALUE:
            case Protos.TaskState.TASK_RUNNING_VALUE:
            case Protos.TaskState.TASK_STAGING_VALUE:
            case Protos.TaskState.TASK_STARTING_VALUE:
            case Protos.TaskState.TASK_DROPPED_VALUE:
            case Protos.TaskState.TASK_UNREACHABLE_VALUE:
            case Protos.TaskState.TASK_GONE_VALUE:
            case Protos.TaskState.TASK_GONE_BY_OPERATOR_VALUE:
            case Protos.TaskState.TASK_UNKNOWN_VALUE:
                return Action.NOOP;

            default:
                return Action.NEVER;
        }
    }

    static Action handleKilled(Protos.TaskState taskState) {
        switch (taskState.getNumber()) {
            case Protos.TaskState.TASK_FINISHED_VALUE:
                return Action.NEVER;

            case Protos.TaskState.TASK_ERROR_VALUE:
            case Protos.TaskState.TASK_FAILED_VALUE:
            case Protos.TaskState.TASK_KILLED_VALUE:

            // To reproduce gh117, change the action against TASK_LOST to RETRY
            case Protos.TaskState.TASK_LOST_VALUE:

            case Protos.TaskState.TASK_KILLING_VALUE:
            case Protos.TaskState.TASK_RUNNING_VALUE:
            case Protos.TaskState.TASK_STAGING_VALUE:
            case Protos.TaskState.TASK_STARTING_VALUE:
            case Protos.TaskState.TASK_DROPPED_VALUE:
            case Protos.TaskState.TASK_UNREACHABLE_VALUE:
            case Protos.TaskState.TASK_GONE_VALUE:
            case Protos.TaskState.TASK_GONE_BY_OPERATOR_VALUE:
            case Protos.TaskState.TASK_UNKNOWN_VALUE:
                return Action.NOOP;

            default:
                return Action.NEVER;
        }
    }

    public enum Action {
        FINISHED,
        FAILED,
        RETRY,
        NOOP,
        NEVER,
        LOG,
        STARTED,
        STARTING,
        KILLED
    }

}
