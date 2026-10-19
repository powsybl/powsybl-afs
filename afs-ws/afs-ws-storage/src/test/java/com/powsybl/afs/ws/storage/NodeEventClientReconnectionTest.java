/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage;

import com.powsybl.afs.ws.storage.websocket.AutoReconnectionConnectionManager;
import com.powsybl.afs.ws.storage.websocket.WebsocketConnectionManager;
import com.powsybl.commons.util.WeakListenerList;
import jakarta.websocket.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Sylvain Leclerc {@literal <sylvain.leclerc@rte-france.com>}
 */
class NodeEventClientReconnectionTest {

    private Session session;

    @BeforeEach
    void setUp() {
        session = mock(Session.class);
        when(session.getId()).thenReturn("session-id");
    }

    @Test
    void shouldTryToReconnectUntilSuccess() throws InterruptedException {

        CountDownLatch reconnectionAttempts = new CountDownLatch(5);

        Runnable reconnection = () -> {
            reconnectionAttempts.countDown();
            if (reconnectionAttempts.getCount() > 0) {
                throw new RuntimeException("Could not connect");
            }
        };

        NodeEventClient client = new NodeEventClient(createWebsocketManager(reconnection), "fs", new WeakListenerList<>());
        client.connect(); //emulate connection
        client.onClose(session); //emulate session closing

        assertTrue(reconnectionAttempts.await(2, TimeUnit.SECONDS));
    }

    @Test
    void shouldStopReconnectingOnClose() throws InterruptedException {

        CountDownLatch attemptsBeforeClosing = new CountDownLatch(2);
        CompletableFuture<Void> reconnectionAfterClosing = new CompletableFuture<>();

        // Completes the above future if any attempt of reconnection is made after closing
        Runnable reconnection = () -> {
            if (attemptsBeforeClosing.getCount() == 0) {
                reconnectionAfterClosing.complete(null);
            }
            attemptsBeforeClosing.countDown();
            throw new RuntimeException("Could not connect");
        };

        NodeEventClient client = new NodeEventClient(createWebsocketManager(reconnection), "fs", new WeakListenerList<>());
        client.connect(); //emulate connection
        client.onClose(session); //emulate session close

        // Wait for 2 reconnection attempts
        assertTrue(attemptsBeforeClosing.await(2, TimeUnit.SECONDS));
        client.close();

        assertThrows(TimeoutException.class, () -> reconnectionAfterClosing.get(500, TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldNotTryToReconnectOnClose() {

        CompletableFuture<Void> reconnectionAfterClosing = new CompletableFuture<>();
        //Completes the above future if any attempt of reconnection is made
        Runnable reconnection = () -> reconnectionAfterClosing.complete(null);

        NodeEventClient client = new NodeEventClient(createWebsocketManager(reconnection), "fs", new WeakListenerList<>());
        client.connect();
        client.close();
        client.onClose(session);

        //Check the reconnection function will never be called
        assertThrows(TimeoutException.class, () -> reconnectionAfterClosing.get(500, TimeUnit.MILLISECONDS));
    }

    /**
     * A connection manager which tries to reconnect every 10 ms.
     * The reconnection function should throw to emulate a connection failure.
     */
    private WebsocketConnectionManager createWebsocketManager(Runnable reconnection) {

        return new AutoReconnectionConnectionManager(URI.create("http://test"), 50, TimeUnit.MILLISECONDS) {

            private boolean initialConnection = false;

            @Override
            public Session connect(Object clientEndoint) {
                if (!initialConnection) {
                    initialConnection = true;
                    return session;
                }
                reconnection.run();
                return session;
            }
        };
    }

}
