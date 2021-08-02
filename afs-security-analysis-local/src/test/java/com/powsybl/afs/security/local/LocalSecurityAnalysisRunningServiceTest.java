/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.security.local;

import com.google.common.collect.ImmutableList;
import com.powsybl.afs.ServiceExtension;
import com.powsybl.afs.ext.base.LocalNetworkCacheServiceExtension;
import com.powsybl.afs.security.SecurityAnalysisRunnerTest;

import java.util.List;

/**
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public class LocalSecurityAnalysisRunningServiceTest extends SecurityAnalysisRunnerTest {

    protected List<ServiceExtension> getServiceExtensions() {
        return ImmutableList.of(new LocalSecurityAnalysisRunningServiceExtension(),
                new LocalNetworkCacheServiceExtension());
    }
}
