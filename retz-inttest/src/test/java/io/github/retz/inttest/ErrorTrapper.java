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

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

import java.util.*;

// A error trapper that watches log for a set of patterns
public class ErrorTrapper implements TailerListener {

    private Set<String> dictionary;
    private boolean fail = false;
    private List<String> errors;

    public ErrorTrapper(String... dictionary) {
        this(Arrays.asList(dictionary));
    }
    public ErrorTrapper(Collection<String> dictionary) {
        this.dictionary = new HashSet<>(Objects.requireNonNull(dictionary));
        this.errors = new ArrayList<>();
    }

    @Override
    public void init(Tailer tailer) {
        System.err.println("Starting error trapper with " + tailer.getFile().getName());
    }

    @Override
    public void fileNotFound() {
        fail = true;
        throw new AssertionError("File not found!");
    }

    @Override
    public void fileRotated() {
        throw new AssertionError("File rotated..");
    }

    @Override
    public void handle(Exception ex) {
        fail = true;
        throw new AssertionError(ex);
    }

    @Override
    public void handle(String line) {
        for(String pattern : dictionary) {
            if(line.contains(pattern)) {
                fail = true;
                errors.add(line);
            }
        }
    }

    public boolean getFail() {
        return fail;
    }

    public List<String> getErrors() {
        return errors;
    }
}
