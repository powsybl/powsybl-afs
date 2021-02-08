/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@MappedSuperclass
@Data
@Accessors(chain = true)
abstract class AbstractMetaEntity<I, V> {
    @Id
    @GeneratedValue
    protected Long id;
    protected String nodeId;
    protected String key;

    public abstract I setValue(V value);
}
