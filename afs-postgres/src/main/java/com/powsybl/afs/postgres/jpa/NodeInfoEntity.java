/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.afs.storage.NodeInfo;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Accessors(chain = true)
@Data()
@Entity
@NoArgsConstructor
@Table(name = "node")
public class NodeInfoEntity {

    @Id
    private String id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String pseudoClass;

    @Column(nullable = false)
    private String description;

    @Column(nullable = false)
    private long creationTime;

    @Column(nullable = false)
    private long modificationTime;

    @Column(nullable = false)
    private int version;

    private boolean consistence;

    private String parentId;

    public NodeInfoEntity(String id, String parentId, String name, String pseudoClass, String description, long creationTime, long modificationTime,
                          int version) {
        this.id = id;
        this.parentId = parentId;
        this.name = name;
        this.pseudoClass = pseudoClass;
        this.description = description;
        this.creationTime = creationTime;
        this.modificationTime = modificationTime;
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public NodeInfo toNodeInfo(NodeGenericMetadata metadata) {
        return new NodeInfo(id, name, pseudoClass, description, creationTime, modificationTime, version, metadata);
    }

    public NodeInfo toNodeInfo() {
        return toNodeInfo(new NodeGenericMetadata());
    }
}
