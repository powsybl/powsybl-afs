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
public interface ChunkRepository extends CrudRepository<ChunkEntity, Long> {

    List<ChunkEntity> findAllByNodeIdAndTsNameAndVersionAndDataType(String nodeId, String tsName,
                                                                    int version, String dataType);

    @Query("SELECT DISTINCT t.version FROM ChunkEntity t WHERE t.nodeId = ?1")
    Set<Integer> findDistinctVersionsByNodeId(String nodeId);

    @Query("SELECT DISTINCT t.version FROM ChunkEntity t WHERE t.nodeId = ?1 and t.tsName = ?2")
    Set<Integer> findDistinctVersionsByNodeIdAndTsName(String nodeId, String tsName);

    @Transactional
    @Modifying
    void deleteAllByNodeId(String nodeId);

    Iterable<ChunkEntity> findAllByNodeId(String nodeId);
}
