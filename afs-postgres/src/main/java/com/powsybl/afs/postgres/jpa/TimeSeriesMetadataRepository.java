/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public interface TimeSeriesMetadataRepository extends CrudRepository<TimeSeriesMetadataEntity, Long> {

    @Query(value = "select t.name from time_series_metadata_entity t where t.node_id = ?1", nativeQuery = true)
    Set<String> getTimeSeriesNames(String nodeId);

    boolean existsByNodeIdAndName(String nodeId, String name);

    Iterable<TimeSeriesMetadataEntity> findAllByNodeIdAndName(String nodeId, Iterable<String> names);
}
