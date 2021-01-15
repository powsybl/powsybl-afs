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

@Repository
public interface DependencyRepository extends CrudRepository<DependencyEntity, Long> {

    List<DependencyEntity> findAllByFrom(NodeInfoEntity from);

    List<DependencyEntity> findAllByTo(NodeInfoEntity to);

    List<DependencyEntity> findAllByFromAndName(NodeInfoEntity from, String name);

    @Transactional
    @Modifying
    void deleteByFrom(NodeInfoEntity from);

    @Transactional
    @Modifying
    void deleteByTo(NodeInfoEntity to);

    @Transactional
    @Modifying
    void deleteByFromAndNameAndTo(NodeInfoEntity from, String name, NodeInfoEntity to);
}
