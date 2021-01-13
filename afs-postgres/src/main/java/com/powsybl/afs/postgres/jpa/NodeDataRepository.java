/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Repository
public interface NodeDataRepository extends CrudRepository<NodeDataEntity, Long> {

    List<NodeDataEntity> findAllByNodeId(String nodeId);

    @Transactional
    Optional<NodeDataEntity> findByNodeIdAndKey(String nodeId, String key);

    @Transactional
    @Modifying
    void deleteByNodeId(String nodeId);

    @Transactional
    @Modifying
    void deleteByNodeIdAndKey(String nodeId, String key);
}
