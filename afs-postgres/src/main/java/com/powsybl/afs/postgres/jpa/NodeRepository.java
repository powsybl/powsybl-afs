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
import java.util.Optional;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Repository
public interface NodeRepository extends CrudRepository<NodeInfoEntity, String> {

    Optional<NodeInfoEntity> findByName(String name);

    @Transactional
    @Modifying
    @Query("update NodeInfoEntity info set info.description = ?2 where info.id = ?1")
    void updateDescriptionById(String id, String desc);

    @Transactional
    @Modifying
    @Query("update NodeInfoEntity info set info.modificationTime = ?2 where info.id = ?1")
    void updateModificationTimeById(String id, long modificationTime);

    @Transactional
    @Modifying
    @Query("update NodeInfoEntity info set info.consistence = ?2 where info.id = ?1")
    void updateConsistenceById(String id, boolean consistence);

    @Transactional
    @Modifying
    @Query("update NodeInfoEntity info set info.parentId = ?2 where info.id = ?1")
    void updateParentById(String id, String newParentId);

    @Transactional
    @Modifying
    @Query("update NodeInfoEntity info set info.name = ?2 where info.id = ?1")
    void updateNameById(String id, String name);

    List<NodeInfoEntity> findAllByParentId(String parentId);

    List<NodeInfoEntity> findAllByConsistenceFalse();

    Optional<NodeInfoEntity> findByParentIdAndName(String parentId, String childName);

}
