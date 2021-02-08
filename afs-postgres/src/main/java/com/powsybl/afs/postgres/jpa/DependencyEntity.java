/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.persistence.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Entity
@Table(name = "dependency")
@Accessors(chain = true)
@Data()
public class DependencyEntity {

    @Id
    @GeneratedValue
    long id;

    @ManyToOne
    @JoinColumn(name = "from_node")
    private NodeInfoEntity from;

    @ManyToOne
    @JoinColumn(name = "to_node")
    private NodeInfoEntity to;

    private String name;
}
