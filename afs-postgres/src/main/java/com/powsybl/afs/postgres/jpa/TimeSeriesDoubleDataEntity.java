/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Entity
@Accessors(chain = true)
@Data()
@NoArgsConstructor
@Table(name = "ts_double_data")
public class TimeSeriesDoubleDataEntity {

    @Id
    @GeneratedValue
    long id;

    String nodeId;

    int version;

    String name;

    int point;

    double value;
}
