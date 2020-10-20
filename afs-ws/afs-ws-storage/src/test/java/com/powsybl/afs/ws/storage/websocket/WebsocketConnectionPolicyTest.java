/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.storage.websocket;

import com.powsybl.afs.ws.client.utils.RemoteServiceConfig;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

/**
 *
 * @author Sylvain Leclerc <sylvain.leclerc@rte-france.com>
 */
public class WebsocketConnectionPolicyTest {

    @Test
    public void createFromConfig() throws URISyntaxException {
        RemoteServiceConfig config = new RemoteServiceConfig("host", "app", 80, false);
        URI uri = new URI("http://test");
        WebsocketConnectionManager standardManager = WebsocketConnectionPolicy.forConfig(config).newConnectionManager(uri);
        assertTrue(standardManager instanceof StandardConnectionManager);

        config.setAutoReconnectionEnabled(true);
        WebsocketConnectionManager autoReconnectionManager = WebsocketConnectionPolicy.forConfig(config).newConnectionManager(uri);
        assertTrue(autoReconnectionManager instanceof AutoReconnectionConnectionManager);
    }
}
