package io.github.retz.cli;

import io.github.retz.web.Client;
import org.junit.Test;

public class ClientTest {
    @Test
    public void version() {
        System.err.println(Client.VERSION_STRING);
    }
}
