/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.security.local;

import com.powsybl.contingency.ContingenciesProvider;
import com.powsybl.contingency.violations.LimitViolation;
import com.powsybl.contingency.violations.LimitViolationBuilder;
import com.powsybl.contingency.violations.LimitViolationType;
import com.powsybl.iidm.network.Network;
import com.powsybl.loadflow.LoadFlowResult;
import com.powsybl.security.LimitViolationsResult;
import com.powsybl.security.SecurityAnalysisProvider;
import com.powsybl.security.SecurityAnalysisReport;
import com.powsybl.security.SecurityAnalysisResult;
import com.powsybl.security.SecurityAnalysisRunParameters;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Sylvain Leclerc {@literal <sylvain.leclerc at rte-france.com>}
 */
public class SecurityAnalysisProviderMock implements SecurityAnalysisProvider {

    @Override
    public String getName() {
        return "SecurityAnalysisMock";
    }

    @Override
    public String getVersion() {
        return "1.0";
    }

    @Override
    public CompletableFuture<SecurityAnalysisReport> run(Network network,
                                                         String workingVariantId,
                                                         ContingenciesProvider contingenciesProvider,
                                                         SecurityAnalysisRunParameters runParameters) {
        LimitViolation limitViolation = new LimitViolationBuilder()
            .subject("s1")
            .type(LimitViolationType.HIGH_VOLTAGE)
            .limit(400.0)
            .scaling(1f)
            .value(440.0)
            .build();
        LimitViolationsResult preContingencyResult = new LimitViolationsResult(List.of(limitViolation));
        SecurityAnalysisResult result = new SecurityAnalysisResult(preContingencyResult,
            LoadFlowResult.ComponentResult.Status.CONVERGED, Collections.emptyList());
        return CompletableFuture.completedFuture(new SecurityAnalysisReport(result));

    }
}
