/*
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ws.utils.exceptions;

import java.util.List;

/**
 * @author Paul Bui-Quang <paul.buiquang at rte-france.com>
 */
public interface RegisteredExceptionForwards {

    List<Class<? extends RuntimeException>> getExceptionClasses();
}
