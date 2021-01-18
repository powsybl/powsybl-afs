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

import javax.persistence.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Entity
@Accessors(chain = true)
@Data()
@Table(name = "reg_ts_index")
public class RegularTimeSeriesIndexEntity {

    @Id
    @GeneratedValue
    Long id;

    long start;
    long endEpochMille;
    long spacing;

    @OneToOne
    @JoinColumn(name = "metadata_id")
    TimeSeriesMetadataEntity metadataEntity;
}
