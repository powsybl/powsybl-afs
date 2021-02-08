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
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Entity
@Data
@Accessors(chain = true)
@Table(name = "irr_ts_index")
public class IrregularTimeSeriesIndexEntity {

    @Id
    @GeneratedValue
    long id;

    // TODO foreign_key?
    long tsmdId;

    int point;

    long epoch;
}
