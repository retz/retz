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
package io.github.retz.cli;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Use like this:
 * ```
 * TableFormatter tf = new TableFormatter("title a", "title b", "title c");
 * tf.feed("A1", "B1", "C1");
 * tf.feed("A2", "B2 boooooooom", "C2");
 * tf.feed("A3", "B3", "C3");
 * System.err.println(tf.titles());
 * for (String line : tf.lines()) {
 * System.err.println(line);
 * }
 * ```
 * Provides output like this:
 * ```
 * title a | title b      | title c
 * ======= | ============ | =======
 * A1      | B1           | C1
 * A2      | B2 booooooom | C2
 * A3      | B3           | C3
 * ```
 */
public class TableFormatter implements Iterable<String> {
    private final String[] titles;
    private final int[] widths;
    private final List<String[]> lines;

    public TableFormatter(String... columnsTitles) {
        // TODO: assert not null, being non-empty list
        this.titles = columnsTitles;
        this.widths = new int[columnsTitles.length];
        for (int i = 0; i < columnsTitles.length; i++) {
            this.widths[i] = this.titles[i].length();
        }
        lines = new LinkedList<>();
    }

    public void feed(String... cells) throws IllegalArgumentException {
        if (cells.length != titles.length) {
            throw new IllegalArgumentException("Input array length does not match the width");
        }
        for (int i = 0; i < widths.length; i++) {
            if (cells[i].length() > this.widths[i]) {
                this.widths[i] = cells[i].length();
            }
        }
        lines.add(cells);
    }

    @Override
    public Iterator<String> iterator() {
        return new TableFormatIterator(lines.iterator(), widths);
    }

    public String titles() {
        return TableFormatter.format(titles, widths);
    }

    static String format(String[] cells, int[] widths) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < widths.length; i++) {
            builder.append(cells[i]);
            for (int j = 0; j < widths[i] - cells[i].length(); j++) {
                builder.append(' ');
            }
            builder.append(' ');
        }
        return builder.toString();
    }

    public static class TableFormatIterator implements Iterator<String> {
        Iterator<String[]> iterator;
        int[] widths;

        TableFormatIterator(Iterator<String[]> iterator, int[] widths) {
            this.iterator = iterator;
            this.widths = widths;
        }

        @Override
        public String next() {
            String[] cells = iterator.next();
            return TableFormatter.format(cells, widths);
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
    }
}
