/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.postgres.jpa.IrregularTimeSeriesIndexEntity;
import com.powsybl.afs.postgres.jpa.IrregularTimeSeriesIndexEntityRepository;
import com.powsybl.afs.postgres.jpa.TimeSeriesMetadataEntity;
import com.powsybl.afs.postgres.jpa.TimeSeriesMetadataRepository;
import com.powsybl.timeseries.IrregularTimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesIndex;
import com.powsybl.timeseries.TimeSeriesMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Service
public class TimeSeriesService {

    private final TimeSeriesMetadataRepository metaRepo;
    private final IrregularTimeSeriesIndexEntityRepository irrTsiRepo;

    @Autowired
    protected TimeSeriesService(TimeSeriesMetadataRepository tsmdRepo, IrregularTimeSeriesIndexEntityRepository irrTsiRepo) {
        metaRepo = Objects.requireNonNull(tsmdRepo);
        this.irrTsiRepo = Objects.requireNonNull(irrTsiRepo);
    }

    void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        // TODO tags
        TimeSeriesMetadataEntity metadataEntity = new TimeSeriesMetadataEntity();
        metadataEntity.setNodeId(nodeId);
        metadataEntity.setName(metadata.getName());
        metadataEntity.setDataType(metadata.getDataType().toString());
        final TimeSeriesMetadataEntity save = metaRepo.save(metadataEntity);

        final TimeSeriesIndex tsi = metadata.getIndex();
        if (tsi.getType().equals(IrregularTimeSeriesIndex.TYPE)) {
            final List<Instant> instances = tsi.stream().collect(Collectors.toList());
            final List<IrregularTimeSeriesIndexEntity> entities = new ArrayList<>();
            for (int i = 0; i < instances.size(); i++) {
                IrregularTimeSeriesIndexEntity entity = new IrregularTimeSeriesIndexEntity();
                entity.setTsmdId(save.getId())
                        .setPoint(i)
                        .setEpoch(instances.get(i).toEpochMilli());
            }
            irrTsiRepo.saveAll(entities);
        }
    }

    Set<String> getTimeSeriesNames(String nodeId) {
        return metaRepo.getTimeSeriesNames(nodeId);
    }

    boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        return metaRepo.existsByNodeIdAndName(nodeId, timeSeriesName);
    }
}
