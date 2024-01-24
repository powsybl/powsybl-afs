/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.ws.utils;

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import com.powsybl.afs.storage.json.AppStorageJsonModule;
import com.powsybl.commons.json.JsonUtil;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.ext.Provider;

/**
 * @author Ali Tahanout {@literal <ali.tahanout at rte-france.com>}
 */
@Provider
@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
public class JsonProvider extends JacksonJsonProvider {

    public JsonProvider() {
        setMapper(JsonUtil.createObjectMapper().registerModule(new AppStorageJsonModule()));
    }
}
