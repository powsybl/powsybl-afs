/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ws.server.utils;

import io.swagger.v3.oas.models.OpenAPI;

import java.util.function.Supplier;

/**
 * Swagger BeanConfig supplier.
 * Only one extension can be defined per application.
 * The first extension found will be used.
 * @author Paul Bui-Quang <paul.buiquang at rte-france.com>
 */
public interface SwaggerConfigExtension extends Supplier<OpenAPI> {

}
