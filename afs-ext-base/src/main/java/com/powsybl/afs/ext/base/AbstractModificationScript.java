/*
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.powsybl.afs.ext.base;

import com.powsybl.afs.ProjectFileCreationContext;

/**
 * @deprecated since the creation of the repository
 * @author Paul Bui-Quang {@literal <paul.buiquang at rte-france.com>}
 */
@Deprecated(since = "the creation of the repository")
public abstract class AbstractModificationScript extends AbstractScript<AbstractModificationScript> {
    public AbstractModificationScript(ProjectFileCreationContext context, int codeVersion, String scriptContentName) {
        super(context, codeVersion, scriptContentName);
    }
}
