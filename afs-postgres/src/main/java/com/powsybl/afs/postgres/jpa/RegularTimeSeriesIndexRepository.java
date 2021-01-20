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

import javax.transaction.Transactional;

@Repository
public interface RegularTimeSeriesIndexRepository extends CrudRepository<RegularTimeSeriesIndexEntity, Long> {

    RegularTimeSeriesIndexEntity findByMetadataEntity(TimeSeriesMetadataEntity metadataEntity);

    @Transactional
    @Modifying
    void deleteByMetadataEntity(TimeSeriesMetadataEntity metadataEntity);
}
