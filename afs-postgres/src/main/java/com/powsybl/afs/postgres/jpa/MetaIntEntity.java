/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;
import java.util.Objects;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Entity
@Accessors(chain = true)
@Data()
@Table(indexes = @Index(name = "int_meta",  columnList = "nodeId, key", unique = true))
public class MetaIntEntity extends AbstractMetaEntity<MetaIntEntity, Integer> {

    private Integer value;

    public MetaIntEntity setKey(String key) {
        super.key = Objects.requireNonNull(key);
        return this;
    }

    @Override
    public MetaIntEntity setValue(Integer integer) {
        value = integer;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MetaIntEntity that = (MetaIntEntity) o;

        return getValue() != null ? getValue().equals(that.getValue()) : that.getValue() == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getValue() != null ? getValue().hashCode() : 0);
        return result;
    }
}