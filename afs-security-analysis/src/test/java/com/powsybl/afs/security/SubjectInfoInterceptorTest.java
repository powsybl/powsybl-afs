/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.security;

import com.powsybl.iidm.network.Country;
import com.powsybl.iidm.network.Network;
import com.powsybl.iidm.network.TwoSides;
import com.powsybl.iidm.network.test.EurostagTutorialExample1Factory;
import com.powsybl.loadflow.LoadFlowResult;
import com.powsybl.security.LimitViolation;
import com.powsybl.security.LimitViolationType;
import com.powsybl.security.LimitViolationsResult;
import com.powsybl.security.interceptors.DefaultSecurityAnalysisResultContext;
import com.powsybl.security.results.PreContingencyResult;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class SubjectInfoInterceptorTest {

    @Test
    void test() {
        Network network = EurostagTutorialExample1Factory.create();
        SubjectInfoInterceptorExtension interceptorExtension = new SubjectInfoInterceptorExtension();
        assertEquals("SubjectInfoInterceptor", interceptorExtension.getName());
        SubjectInfoInterceptor interceptor = interceptorExtension.createInterceptor();
        assertNotNull(interceptor);

        LimitViolation violation1 = new LimitViolation("NHV1_NHV2_1", LimitViolationType.CURRENT, "N/A", 60 * 20, 300, 1, 400, TwoSides.ONE);
        LimitViolation violation2 = new LimitViolation("VLGEN", LimitViolationType.HIGH_VOLTAGE, 300, 1, 400);
        assertNull(violation1.getExtension(SubjectInfoExtension.class));
        assertNull(violation2.getExtension(SubjectInfoExtension.class));

        LimitViolationsResult result = new LimitViolationsResult(Arrays.asList(violation1, violation2));
        PreContingencyResult preContingencyResult = new PreContingencyResult(LoadFlowResult.ComponentResult.Status.CONVERGED,
                result, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        interceptor.onPreContingencyResult(preContingencyResult, new DefaultSecurityAnalysisResultContext(network));

        SubjectInfoExtension extension1 = violation1.getExtension(SubjectInfoExtension.class);
        assertNotNull(extension1);
        assertEquals(Collections.singleton(380d), extension1.getNominalVoltages());
        assertEquals(Collections.singleton(Country.FR), extension1.getCountries());

        SubjectInfoExtension extension2 = violation2.getExtension(SubjectInfoExtension.class);
        assertNotNull(extension2);
        assertEquals(Collections.singleton(24d), extension2.getNominalVoltages());
        assertEquals(Collections.singleton(Country.FR), extension2.getCountries());
    }
}
