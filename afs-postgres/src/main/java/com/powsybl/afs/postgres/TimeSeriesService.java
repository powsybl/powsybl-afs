/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.postgres.jpa.*;
import com.powsybl.timeseries.*;
import lombok.AccessLevel;
import lombok.Setter;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.threeten.extra.Interval;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Service
public class TimeSeriesService {

    private final TimeSeriesMetadataRepository metaRepo;
    private final TsTagRepository tagRepo;
    private final RegularTimeSeriesIndexRepository regTsiRepo;
    private final IrregularTimeSeriesIndexEntityRepository irrTsiRepo;

    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> timeSeriesCreated;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> timeSeriesDataUpdated;

    @Autowired
    protected TimeSeriesService(TimeSeriesMetadataRepository tsmdRepo,
                                TsTagRepository tsTagRepository,
                                RegularTimeSeriesIndexRepository regularTimeSeriesIndexRepository,
                                IrregularTimeSeriesIndexEntityRepository irrTsiRepo) {
        metaRepo = Objects.requireNonNull(tsmdRepo);
        tagRepo = Objects.requireNonNull(tsTagRepository);
        regTsiRepo = Objects.requireNonNull(regularTimeSeriesIndexRepository);
        this.irrTsiRepo = Objects.requireNonNull(irrTsiRepo);
    }

    void createTimeSeries(String nodeId, TimeSeriesMetadata metadata) {
        TimeSeriesMetadataEntity metadataEntity = new TimeSeriesMetadataEntity();
        metadataEntity.setNodeId(nodeId);
        metadataEntity.setName(metadata.getName());
        metadataEntity.setDataType(metadata.getDataType().toString());
        final TimeSeriesMetadataEntity save = metaRepo.save(metadataEntity);

        // save tags
        List<TsTagEntity> tags = new ArrayList<>();
        metadata.getTags().forEach((k, v) -> tags.add(new TsTagEntity().setMetadataEntity(save)
                .setTagKey(k).setTagValue(v)));
        tagRepo.saveAll(tags);

        saveTsi(metadata.getIndex(), save);

        timeSeriesCreated.accept(nodeId, metadata.getName());
    }

    private void saveTsi(TimeSeriesIndex tsi, TimeSeriesMetadataEntity save) {
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
        } else if (tsi.getType().equals(RegularTimeSeriesIndex.TYPE)) {
            final RegularTimeSeriesIndex regIdx = (RegularTimeSeriesIndex) tsi;
            RegularTimeSeriesIndexEntity entity = new RegularTimeSeriesIndexEntity()
                    .setStart(regIdx.getStartTime())
                    .setEndEpochMille(regIdx.getEndTime())
                    .setSpacing(regIdx.getSpacing())
                    .setMetadataEntity(save);
            regTsiRepo.save(entity);
        } else {
            throw new NotImplementedException("other types");
        }
    }

    Set<String> getTimeSeriesNames(String nodeId) {
        return metaRepo.getTimeSeriesNames(nodeId);
    }

    boolean timeSeriesExists(String nodeId, String timeSeriesName) {
        return metaRepo.existsByNodeIdAndName(nodeId, timeSeriesName);
    }

    List<TimeSeriesMetadata> getTimeSeriesMetadata(String nodeId, Set<String> timeSeriesNames) {
        List<TimeSeriesMetadata> res = new ArrayList<>();
        final Iterable<TimeSeriesMetadataEntity> allByNodeIdAndName = metaRepo.findAllByNodeIdAndName(nodeId, timeSeriesNames);
        for (TimeSeriesMetadataEntity e : allByNodeIdAndName) {
            final TimeSeriesDataType type = TimeSeriesDataType.valueOf(e.getDataType());
            final TimeSeriesIndex index = getIndex(e);
            Map<String, String> tags = new HashMap<>();
            tagRepo.findAllByMetadataEntity(e).forEach(tsTagEntity -> tags.put(tsTagEntity.getTagKey(), tsTagEntity.getTagValue()));
            res.add(new TimeSeriesMetadata(e.getName(), type, tags, index));
        }
        return res;
    }

    private TimeSeriesIndex getIndex(TimeSeriesMetadataEntity e) {
        if (e.getDataType().equals(IrregularTimeSeriesIndex.TYPE)) {
            throw new NotImplementedException("other types");
        } else if (e.getDataType().equals(RegularTimeSeriesIndex.TYPE)) {
            final RegularTimeSeriesIndexEntity regTsi = regTsiRepo.findByMetadataEntity(e);
            return new RegularTimeSeriesIndex(regTsi.getStart(), regTsi.getEndEpochMille(), regTsi.getSpacing());
        }
        return RegularTimeSeriesIndex.create(Interval.parse("2015-01-01T00:00:00Z/2015-01-01T01:15:00Z"),
                Duration.ofMinutes(15));
    }

    void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {

    }
}
