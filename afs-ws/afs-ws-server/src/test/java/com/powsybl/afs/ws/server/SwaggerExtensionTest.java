/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ws.server;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.servers.Server;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Java6Assertions.assertThat;

/**
 * @author Paul Bui-Quang {@literal <paul.buiquang at rte-france.com>}
 */
public class SwaggerExtensionTest {

    @Test
    public void test() {
        AppStorageApplication appStorageApplication = new AppStorageApplication();
        OpenAPI swaggerConfig = appStorageApplication.initSwaggerConfig(Arrays.asList(
            () -> {
                OpenAPI oas = new OpenAPI();
                Server server = new Server();
                server.setUrl("/foo");
                oas.servers(List.of(server));
                return oas;
            },
            () -> {
                OpenAPI oas = new OpenAPI();
                Server server = new Server();
                server.setUrl("/bar");
                oas.servers(List.of(server));
                return oas;
            })
        );
        assertThat(swaggerConfig.getServers().get(0).getUrl()).isEqualTo("/foo");

        swaggerConfig = appStorageApplication.initSwaggerConfig(Collections.emptyList());
        assertThat(swaggerConfig.getServers().get(0).getUrl()).isEqualTo("/rest");
    }
}
