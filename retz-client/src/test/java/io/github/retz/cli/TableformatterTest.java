package io.github.retz.cli;

import org.junit.Test;

public class TableformatterTest {
    @Test
    public void formatTest() {
        TableFormatter formatter = new TableFormatter("a", "b");
        formatter.feed("1111", "2");
        System.err.println(formatter.titles());
        for(String line : formatter) {
            System.err.println(line);
        }
    }
}
