/*
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package com.powsybl.afs;

import com.google.auto.service.AutoService;
import com.powsybl.tools.AbstractVersion;
import com.powsybl.tools.Version;

/**
 * @author Paul Bui-Quang <paul.buiquang at rte-france.com>
 */
@AutoService(Version.class)
public class PowsyblAfsVersion extends AbstractVersion {

    public PowsyblAfsVersion() {
        super("powsybl-afs", "${project.version}", "${buildNumber}", "${scmBranch}", Long.parseLong("${timestamp}"));
    }
}
