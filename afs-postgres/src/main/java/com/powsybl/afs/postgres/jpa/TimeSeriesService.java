/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import org.springframework.stereotype.Service;

import java.util.Objects;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Service
public class TimeSeriesService {

    private final TimeSeriesMetadataRepository metadataRepository;

    public TimeSeriesService(TimeSeriesMetadataRepository metadataRepository) {
        this.metadataRepository = Objects.requireNonNull(metadataRepository);
    }
}
