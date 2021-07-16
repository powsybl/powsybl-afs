package com.powsybl.afs.security.local;

import com.google.auto.service.AutoService;
import com.powsybl.computation.ComputationManager;
import com.powsybl.contingency.ContingenciesProvider;
import com.powsybl.iidm.network.Network;
import com.powsybl.security.*;
import com.powsybl.security.interceptors.SecurityAnalysisInterceptor;
import com.powsybl.security.monitor.StateMonitor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@AutoService(SecurityAnalysisProvider.class)
public class SecurityAnalysisProviderMock implements SecurityAnalysisProvider {
    private SecurityAnalysisMock mock;

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
                                                         LimitViolationDetector detector,
                                                         LimitViolationFilter filter,
                                                         ComputationManager computationManager,
                                                         SecurityAnalysisParameters parameters,
                                                         ContingenciesProvider contingenciesProvider,
                                                         List<SecurityAnalysisInterceptor> interceptors,
                                                         List<StateMonitor> monitors) {
        if (mock == null) {
            mock = new SecurityAnalysisMock();
        }
        return mock.run();
    }
}
