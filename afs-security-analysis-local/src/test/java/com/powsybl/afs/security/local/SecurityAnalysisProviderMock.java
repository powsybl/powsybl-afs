/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.security.local;

import com.google.common.collect.ImmutableList;
import com.powsybl.commons.reporter.Reporter;
import com.powsybl.computation.ComputationManager;
import com.powsybl.contingency.ContingenciesProvider;
import com.powsybl.iidm.network.Network;
import com.powsybl.loadflow.LoadFlowResult;
import com.powsybl.security.*;
import com.powsybl.security.action.Action;
import com.powsybl.security.interceptors.SecurityAnalysisInterceptor;
import com.powsybl.security.monitor.StateMonitor;
import com.powsybl.security.strategy.OperatorStrategy;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
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
    public CompletableFuture<SecurityAnalysisReport> run(
            Network network,
            String workingVariantId,
            LimitViolationDetector detector,
            LimitViolationFilter filter,
            ComputationManager computationManager,
            SecurityAnalysisParameters parameters,
            ContingenciesProvider contingenciesProvider,
            List<SecurityAnalysisInterceptor> interceptors,
            List<OperatorStrategy> operatorStrategies,
            List<Action> actions,
            List<StateMonitor> monitors,
            Reporter reporter) {
        LimitViolationsResult preContingencyResult = new LimitViolationsResult(
                ImmutableList.of(new LimitViolation("s1", LimitViolationType.HIGH_VOLTAGE, 400.0, 1f, 440.0))
        );
        SecurityAnalysisResult result = new SecurityAnalysisResult(preContingencyResult,
                LoadFlowResult.ComponentResult.Status.CONVERGED, Collections.emptyList());
        return CompletableFuture.completedFuture(new SecurityAnalysisReport(result));
    }
}
