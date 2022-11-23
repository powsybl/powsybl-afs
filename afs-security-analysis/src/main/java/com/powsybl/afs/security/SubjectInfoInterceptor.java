/**
 * Copyright (c) 2018, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.security;

import com.powsybl.iidm.network.*;
import com.powsybl.security.LimitViolation;
import com.powsybl.security.LimitViolationsResult;
import com.powsybl.security.interceptors.DefaultSecurityAnalysisInterceptor;
import com.powsybl.security.interceptors.SecurityAnalysisResultContext;
import com.powsybl.security.results.PostContingencyResult;
import com.powsybl.security.results.PreContingencyResult;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at rte-france.com>
 */
public class SubjectInfoInterceptor extends DefaultSecurityAnalysisInterceptor {
    private void addSubjectInfo(SecurityAnalysisResultContext context, LimitViolationsResult result) {
        for (LimitViolation violation : result.getLimitViolations()) {
            Identifiable identifiable = context.getNetwork().getIdentifiable(violation.getSubjectId());
            if (identifiable instanceof Branch) {
                addBranchExtension(violation, (Branch) identifiable);
            } else if (identifiable instanceof VoltageLevel) {
                addVoltageLevelExtension(violation, (VoltageLevel) identifiable);
            }
        }
    }

    private void addVoltageLevelExtension(LimitViolation violation, VoltageLevel identifiable) {
        violation.addExtension(SubjectInfoExtension.class, subjectInfoExtension(identifiable));
    }

    private void addBranchExtension(LimitViolation violation, Branch branch) {
        violation.addExtension(
                SubjectInfoExtension.class,
                new SubjectInfoExtension(countries(branch), nominalVoltages(branch))
        );
    }

    private SubjectInfoExtension subjectInfoExtension(VoltageLevel vl) {
        return vl.getSubstation()
                .flatMap(Substation::getCountry)
                .map(country -> new SubjectInfoExtension(country, vl.getNominalV()))
                .orElseGet(() -> new SubjectInfoExtension(new TreeSet<>(), Collections.singleton(vl.getNominalV())));
    }

    private Set<Double> nominalVoltages(Branch branch) {
        Set<Double> nominalVoltages = new TreeSet<>();
        nominalVoltages.add(branch.getTerminal1().getVoltageLevel().getNominalV());
        nominalVoltages.add(branch.getTerminal2().getVoltageLevel().getNominalV());
        return nominalVoltages;
    }

    private Set<Country> countries(Branch branch) {
        Set<Country> countries = new TreeSet<>();
        country(branch.getTerminal1()).ifPresent(countries::add);
        country(branch.getTerminal2()).ifPresent(countries::add);
        return countries;
    }

    private Optional<Country> country(Terminal terminal) {
        return terminal.getVoltageLevel()
                .getSubstation()
                .flatMap(Substation::getCountry);
    }

    @Override
    public void onPreContingencyResult(PreContingencyResult preContingencyResult, SecurityAnalysisResultContext context) {
        addSubjectInfo(context, preContingencyResult.getLimitViolationsResult());
    }

    @Override
    public void onPostContingencyResult(PostContingencyResult postContingencyResult, SecurityAnalysisResultContext context) {
        addSubjectInfo(context, postContingencyResult.getLimitViolationsResult());
    }
}
