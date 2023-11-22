/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.security.local;

import com.google.auto.service.AutoService;
import com.powsybl.afs.ServiceCreationContext;
import com.powsybl.afs.ServiceExtension;
import com.powsybl.afs.security.SecurityAnalysisRunningService;
import com.powsybl.security.SecurityAnalysis;

import java.util.Objects;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
@AutoService(ServiceExtension.class)
public class LocalSecurityAnalysisRunningServiceExtension implements ServiceExtension<SecurityAnalysisRunningService> {

    private final SecurityAnalysis.Runner runner;

    public LocalSecurityAnalysisRunningServiceExtension() {
        this(SecurityAnalysis.find());
    }

    public LocalSecurityAnalysisRunningServiceExtension(SecurityAnalysis.Runner runner) {
        this.runner = Objects.requireNonNull(runner);
    }

    @Override
    public ServiceKey<SecurityAnalysisRunningService> getServiceKey() {
        return new ServiceKey<>(SecurityAnalysisRunningService.class, false);
    }

    @Override
    public SecurityAnalysisRunningService createService(ServiceCreationContext context) {
        return new LocalSecurityAnalysisRunningService(runner);
    }
}
