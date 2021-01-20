/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;

@Repository
public interface TimeSeriesMetadataRepository extends CrudRepository<TimeSeriesMetadataEntity, Long> {

    @Query(value = "select t.name from meta_ts t where t.node_id = ?1", nativeQuery = true)
    Set<String> getTimeSeriesNames(String nodeId);

    List<TimeSeriesMetadataEntity> findAllByNodeId(String nodeId);

    boolean existsByNodeIdAndName(String nodeId, String name);

    Iterable<TimeSeriesMetadataEntity> findAllByNodeIdAndName(String nodeId, Iterable<String> names);

    @Transactional
    @Modifying
    void deleteAllByNodeId(String nodeId);
}
