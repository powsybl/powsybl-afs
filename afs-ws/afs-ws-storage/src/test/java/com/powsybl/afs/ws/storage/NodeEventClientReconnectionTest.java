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
import org.junit.Before;
import org.junit.Test;

import javax.websocket.Session;
import java.net.URI;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Sylvain Leclerc {@literal <sylvain.leclerc@rte-france.com>}
 */
public class NodeEventClientReconnectionTest {

    private Session session;

    @Before
    public void setUp() {
        session = mock(Session.class);
        when(session.getId()).thenReturn("session-id");
    }

    @Test
    public void shouldTryToReconnectUntilSuccess() throws InterruptedException {

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

        assertTrue(reconnectionAttempts.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void shouldStopReconnectingOnClose() throws InterruptedException, ExecutionException {

        CountDownLatch attemptsBeforeClosing = new CountDownLatch(2);

        CompletableFuture<Void> reconnectionAfterClosing = new CompletableFuture<>();

        //Completes the above future if any attemps of reconnection is made after closing
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

        assertTrue(attemptsBeforeClosing.await(1, TimeUnit.SECONDS)); //wait for 2 reconnection attempts
        client.close();

        assertTimesOut(reconnectionAfterClosing);
    }

    @Test
    public void shouldNotTryToReconnectOnClose() throws ExecutionException, InterruptedException {

        CompletableFuture<Void> reconnectionAfterClosing = new CompletableFuture<>();
        //Completes the above future if any attempt of reconnection is made
        Runnable reconnection = () -> reconnectionAfterClosing.complete(null);

        NodeEventClient client = new NodeEventClient(createWebsocketManager(reconnection), "fs", new WeakListenerList<>());
        client.connect();
        client.close();
        client.onClose(session);

        //Check the reconnection function will never be called
        assertTimesOut(reconnectionAfterClosing);
    }

    private void assertTimesOut(CompletableFuture<Void> completableFuture) throws ExecutionException, InterruptedException {
        try {
            completableFuture.get(100, TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            //OK
        }
    }

    /**
     * A connection manager which tries to reconnect every 10ms.
     * The reconnection function should throw to emulate a connectionfailure.
     */
    private WebsocketConnectionManager createWebsocketManager(Runnable reconnection) {

        return new AutoReconnectionConnectionManager(URI.create("http://test"), 10, TimeUnit.MILLISECONDS) {

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
