/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ws.server;

import com.powsybl.afs.ws.server.utils.SwaggerConfigExtension;
import io.swagger.config.SwaggerConfig;
import io.swagger.jaxrs.config.BeanConfig;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Java6Assertions.assertThat;

/**
 * @author Paul Bui-Quang <paul.buiquang at rte-france.com>
 */
public class SwaggerExtensionTest {

    @Test
    public void test() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method initSwaggerConfig = AppStorageApplication.class.getDeclaredMethod("initSwaggerConfig", List.class);
        initSwaggerConfig.setAccessible(true);
        AppStorageApplication appStorageApplication = new AppStorageApplication();
        BeanConfig swaggerConfig = (BeanConfig) initSwaggerConfig.invoke(appStorageApplication, Arrays.asList((SwaggerConfigExtension) () -> {
            BeanConfig beanConfig = new BeanConfig();
            beanConfig.setBasePath("/foo");
            return beanConfig;
        }, () -> {
            BeanConfig beanConfig = new BeanConfig();
            beanConfig.setBasePath("/bar");
            return beanConfig;
        }));
        assertThat(swaggerConfig.getBasePath()).isEqualTo("/foo");

        swaggerConfig = (BeanConfig) initSwaggerConfig.invoke(appStorageApplication, Collections.emptyList());
        assertThat(swaggerConfig.getBasePath()).isEqualTo("/rest");
    }
}
