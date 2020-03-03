/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.server;

import com.powsybl.afs.ws.server.utils.SwaggerConfigExtension;
import com.powsybl.afs.ws.utils.AfsRestApi;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.models.Info;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author Ali Tahanout <ali.tahanout at rte-france.com>
 */
@ApplicationPath("/rest")
public class AppStorageApplication extends Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppStorageApplication.class);

    public AppStorageApplication() {
        ServiceLoader<SwaggerConfigExtension> swaggerConfigExtensionsIterator = ServiceLoader.load(SwaggerConfigExtension.class);
        ArrayList<SwaggerConfigExtension> swaggerConfigExtensions = Lists.newArrayList(swaggerConfigExtensionsIterator.iterator());
        initSwaggerConfig(swaggerConfigExtensions);
    }

    private BeanConfig initSwaggerConfig(List<SwaggerConfigExtension> swaggerConfigExtensions) {
        if (swaggerConfigExtensions.size() > 1) {
            LOGGER.warn("Multiple swagger bean supplier found! Will take the first found and ignore the rest.");
        }

        if (swaggerConfigExtensions.isEmpty()) {
            // Default original configuration
            BeanConfig beanConfig = new BeanConfig();
            beanConfig.setBasePath("/rest");
            beanConfig.setResourcePackage(AppStorageServer.class.getPackage().getName());
            beanConfig.setScan();
            beanConfig.setInfo(new Info()
                    .title("AFS storage API")
                    .version(AfsRestApi.VERSION)
                    .description("This is the documentation of AFS REST API"));
            return beanConfig;
        }

        return swaggerConfigExtensions.get(0).get();
    }
}
