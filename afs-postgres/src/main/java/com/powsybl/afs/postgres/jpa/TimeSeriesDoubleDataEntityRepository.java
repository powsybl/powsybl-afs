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

import java.util.List;
import java.util.Set;

@Repository
public interface TimeSeriesDoubleDataEntityRepository extends CrudRepository<TimeSeriesDoubleDataEntity, Long> {

    @Query("SELECT DISTINCT t.version FROM TimeSeriesDoubleDataEntity t WHERE t.nodeId = ?1")
    List<Integer> findDistinctVersionsByNodeId(String nodeId);

    @Query("SELECT DISTINCT t.version FROM TimeSeriesDoubleDataEntity t WHERE t.nodeId = ?1 and t.name = ?2")
    Set<Integer> findDistinctVersionsByNodeIdAndName(String nodeId, String tsName);

    List<TimeSeriesDoubleDataEntity> findAllByNodeIdAndNameAndVersionOrderByPoint(String nodeId, String name, int version);
}
