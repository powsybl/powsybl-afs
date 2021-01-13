/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TimeSeriesMetadataRepository extends CrudRepository<TimeSeriesMetadataEntity, Long> {

    Iterable<TimeSeriesMetadataEntity> findAllByNodeId(String nodeId);

    Iterable<TimeSeriesMetadataEntity> findAllByNodeIdAndName(String nodeId, String name);
}
