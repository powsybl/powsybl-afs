/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.server.jwt;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the {@link JwtTokenFilter} .
 *
 * @author Pratheep Thiyagarasa <pratheep.thiyagarasa at rte-france.com>
 */
@Configuration
public class JwtFilterRegistration {

    @Bean
    public FilterRegistrationBean<JwtTokenFilter> registerJwtTokenFilter() {
        FilterRegistrationBean<JwtTokenFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new JwtTokenFilter());
        return registration;
    }
}
