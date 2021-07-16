package com.powsybl.afs.security.local;

import com.google.common.collect.ImmutableList;
import com.powsybl.security.*;
import com.powsybl.security.results.PreContingencyResult;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

class SecurityAnalysisMock {
    CompletableFuture<SecurityAnalysisReport> run() {
        CompletableFuture<SecurityAnalysisReport> cfReport = Mockito.mock(CompletableFuture.class);
        SecurityAnalysisReport report = Mockito.mock(SecurityAnalysisReport.class);
        SecurityAnalysisResult result = Mockito.mock(SecurityAnalysisResult.class);
        PreContingencyResult preContingencyResult = Mockito.mock(PreContingencyResult.class);
        LimitViolationsResult preLimitViolationsResult = new LimitViolationsResult(true, ImmutableList.of(new LimitViolation("s1", LimitViolationType.HIGH_VOLTAGE, 400.0, 1f, 440.0)));
        Mockito.when(preContingencyResult.getLimitViolationsResult()).thenReturn(preLimitViolationsResult);
        Mockito.when(result.getPreContingencyResult()).thenReturn(preContingencyResult);
        Mockito.when(result.getPreContingencyLimitViolationsResult()).thenReturn(preLimitViolationsResult);
        Mockito.when(result.getPostContingencyResults()).thenReturn(Collections.emptyList());
        Mockito.when(report.getResult()).thenReturn(result);
        Mockito.when(cfReport.join()).thenReturn(report);
        return cfReport;
    }
}
