/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.security.local;

import com.powsybl.afs.AfsException;
import com.powsybl.afs.AppLogger;
import com.powsybl.afs.ext.base.ProjectCase;
import com.powsybl.afs.security.SecurityAnalysisRunner;
import com.powsybl.afs.security.SecurityAnalysisRunningService;
import com.powsybl.computation.ComputationManager;
import com.powsybl.contingency.ContingenciesProvider;
import com.powsybl.contingency.EmptyContingencyListProvider;
import com.powsybl.iidm.network.Network;
import com.powsybl.security.LimitViolationFilter;
import com.powsybl.security.SecurityAnalysis;
import com.powsybl.security.SecurityAnalysisParameters;
import com.powsybl.security.SecurityAnalysisReport;
import com.powsybl.security.detectors.DefaultLimitViolationDetector;
import com.powsybl.security.interceptors.SecurityAnalysisInterceptor;
import com.powsybl.security.interceptors.SecurityAnalysisInterceptors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class LocalSecurityAnalysisRunningService implements SecurityAnalysisRunningService {

    @Override
    public void run(SecurityAnalysisRunner runner) {
        Objects.requireNonNull(runner);

        ProjectCase aCase = (ProjectCase) runner.getCase().orElseThrow(() -> new AfsException("Invalid case link"));
        ContingenciesProvider contingencyListProvider = runner.getContingencyStore()
                .map(store -> (ContingenciesProvider) store)
                .orElse(new EmptyContingencyListProvider());
        SecurityAnalysisParameters parameters = runner.readParameters();
        ComputationManager computationManager = runner.getFileSystem().getData().getLongTimeExecutionComputationManager();

        UUID taskId = runner.startTask();
        try {
            AppLogger logger = runner.createLogger(taskId);

            logger.log("Loading network...");
            Network network = aCase.getNetwork();
            // add all interceptors
            List<SecurityAnalysisInterceptor> interceptors = new ArrayList<>();
            for (String interceptorName : SecurityAnalysisInterceptors.getExtensionNames()) {
                interceptors.add(SecurityAnalysisInterceptors.createInterceptor(interceptorName));
            }

            logger.log("Running security analysis...");
            SecurityAnalysisReport securityAnalysisReport = SecurityAnalysis.run(network,
                    network.getVariantManager().getWorkingVariantId(),
                    new DefaultLimitViolationDetector(),
                    new LimitViolationFilter(),
                    computationManager,
                    parameters,
                    contingencyListProvider,
                    interceptors);

            runner.writeResult(securityAnalysisReport.getResult());
        } finally {
            runner.stopTask(taskId);
        }
    }
}
