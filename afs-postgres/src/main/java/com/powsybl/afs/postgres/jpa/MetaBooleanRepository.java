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

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Repository
public interface MetaBooleanRepository extends CrudRepository<MetaBooleanEntity, Long> {

    List<MetaBooleanEntity> findAllByNodeId(String nodeId);

    @Transactional
    @Modifying
    void deleteByNodeId(String nodeId);
}
