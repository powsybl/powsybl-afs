/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.powsybl.afs.postgres.jpa.*;
import com.powsybl.timeseries.*;
import lombok.AccessLevel;
import lombok.Setter;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
    private final TimeSeriesDoubleDataEntityRepository doubleDataRepo;
    private final TimeSeriesStringDataEntityRepository stringDataRepo;

    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> timeSeriesCreated;
    @Setter(AccessLevel.PACKAGE)
    private BiConsumer<String, String> timeSeriesDataUpdated;
    @Setter(AccessLevel.PACKAGE)
    private Consumer<String> timeSeriesCleared;

    @Autowired
    protected TimeSeriesService(TimeSeriesMetadataRepository tsmdRepo,
                                TsTagRepository tsTagRepository,
                                RegularTimeSeriesIndexRepository regularTimeSeriesIndexRepository,
                                IrregularTimeSeriesIndexEntityRepository irrTsiRepo,
                                TimeSeriesDoubleDataEntityRepository doubleDataRepo,
                                TimeSeriesStringDataEntityRepository stringDataRepo) {
        metaRepo = Objects.requireNonNull(tsmdRepo);
        tagRepo = Objects.requireNonNull(tsTagRepository);
        regTsiRepo = Objects.requireNonNull(regularTimeSeriesIndexRepository);
        this.irrTsiRepo = Objects.requireNonNull(irrTsiRepo);
        this.doubleDataRepo = Objects.requireNonNull(doubleDataRepo);
        this.stringDataRepo = Objects.requireNonNull(stringDataRepo);
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
                entities.add(entity);
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
        final TimeSeriesIndex regIndex = getRegIndex(e);
        if (regIndex != null) {
            return regIndex;
        }
        throw new NotImplementedException("not yet");
    }

    private TimeSeriesIndex getRegIndex(TimeSeriesMetadataEntity metadataEntity) {
        final RegularTimeSeriesIndexEntity regIdxEntity = regTsiRepo.findByMetadataEntity(metadataEntity);
        if (regIdxEntity == null) {
            return null;
        }
        return new RegularTimeSeriesIndex(regIdxEntity.getStart(), regIdxEntity.getEndEpochMille(), regIdxEntity.getSpacing());
    }

    void addDoubleTimeSeriesData(String nodeId, int version, String timeSeriesName, List<DoubleDataChunk> chunks) {
        chunks.forEach(c -> {
            int offset = c.getOffset();
            if (c.isCompressed()) {
                final CompressedDoubleDataChunk c1 = (CompressedDoubleDataChunk) c;
                throw new NotImplementedException("not yet");
            } else {
                final UncompressedDoubleDataChunk uncompressedDoubleDataChunk = (UncompressedDoubleDataChunk) c;
                uncompressedDoubleDataChunk.tryToCompress();
                final double[] values = uncompressedDoubleDataChunk.getValues();
                for (int i = 0; i < values.length; i++) {
                    final TimeSeriesDoubleDataEntity entity = new TimeSeriesDoubleDataEntity().setName(timeSeriesName)
                            .setNodeId(nodeId).setPoint(offset + i).setValue(values[i])
                            .setVersion(version);
                    doubleDataRepo.save(entity);
                }
            }
        });
        timeSeriesDataUpdated.accept(nodeId, timeSeriesName);
    }

    Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        Set<Integer> res = new HashSet<>();
        res.addAll(doubleDataRepo.findDistinctVersionsByNodeId(nodeId));
        return res;
    }

    Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        Set<Integer> res = new HashSet<>();
        res.addAll(doubleDataRepo.findDistinctVersionsByNodeIdAndName(nodeId, timeSeriesName));
        return res;
    }

    Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        Map<String, List<DoubleDataChunk>> map = new HashMap<>();
        for (String name : timeSeriesNames) {
            final List<TimeSeriesDoubleDataEntity> entities = doubleDataRepo.findAllByNodeIdAndNameAndVersionOrderByPoint(nodeId, name, version);
            if (entities.isEmpty()) {
                continue;
            }
            List<DoubleDataChunk> chunks = new ArrayList<>();
            int lastPoint = -1;
            List<Double> data = new ArrayList<>();
            boolean isContinue = false;
            int point = lastPoint;
            double value = 0.0;
            for (TimeSeriesDoubleDataEntity entity : entities) {
                point = entity.getPoint();
                value = entity.getValue();
                if (lastPoint == -1) {
                    // first element
                    lastPoint = point;
                    data.add(value);
                    isContinue = false;
                    continue;
                }
                if (point != (lastPoint + 1)) {
                    // close previous chunk
                    int offset = lastPoint - data.size() + 1;
                    double[] arrData = Doubles.toArray(data);
                    chunks.add(new UncompressedDoubleDataChunk(offset, arrData));
                    // prepare next chunk
                    data = new ArrayList<>();
                    data.add(value);
                    isContinue = false;
                } else {
                    data.add(value);
                    isContinue = true;
                }
                lastPoint = point;
            }
            // close last chunk
            if (isContinue) {
                int offset = lastPoint - data.size() + 1;
                double[] arrData = Doubles.toArray(data);
                chunks.add(new UncompressedDoubleDataChunk(offset, arrData));
            } else {
                chunks.add(new UncompressedDoubleDataChunk(point, new double[]{value}));
            }
            map.put(name, chunks);
        }
        return map;
    }

    Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        Map<String, List<StringDataChunk>> map = new HashMap<>();
        for (String name : timeSeriesNames) {
            final List<TimeSeriesStringDataEntity> entities = stringDataRepo.findAllByNodeIdAndNameAndVersionOrderByPoint(nodeId, name, version);
            if (entities.isEmpty()) {
                continue;
            }
            List<StringDataChunk> chunks = new ArrayList<>();
            int lastPoint = -1;
            List<String> data = new ArrayList<>();
            boolean isContinue = false;
            int point = lastPoint;
            String value = "";
            for (TimeSeriesStringDataEntity entity : entities) {
                point = entity.getPoint();
                value = entity.getValue();
                if (lastPoint == -1) {
                    // first element
                    lastPoint = point;
                    data.add(value);
                    isContinue = false;
                    continue;
                }
                if (point != (lastPoint + 1)) {
                    // close previous chunk
                    int offset = lastPoint - data.size() + 1;
                    chunks.add(new UncompressedStringDataChunk(offset, data.toArray(new String[0])));
                    // prepare next chunk
                    data = new ArrayList<>();
                    data.add(value);
                    isContinue = false;
                } else {
                    data.add(value);
                    isContinue = true;
                }
                lastPoint = point;
            }
            // close last chunk
            if (isContinue) {
                int offset = lastPoint - data.size() + 1;
                chunks.add(new UncompressedStringDataChunk(offset, data.toArray(new String[0])));
            } else {
                chunks.add(DataChunk.create(point, new String[]{value}));
            }
            map.put(name, chunks);
        }
        return map;
    }

    void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        chunks.forEach(c -> {
            int offset = c.getOffset();
            if (c.isCompressed()) {
                final CompressedStringDataChunk c1 = (CompressedStringDataChunk) c;
                throw new NotImplementedException("not yet");
            } else {
                final UncompressedStringDataChunk uncompressedChunk = (UncompressedStringDataChunk) c;
                final String[] values = uncompressedChunk.getValues();
                for (int i = 0; i < values.length; i++) {
                    final TimeSeriesStringDataEntity entity = new TimeSeriesStringDataEntity().setName(timeSeriesName)
                            .setNodeId(nodeId).setPoint(offset + i).setValue(values[i])
                            .setVersion(version);
                    stringDataRepo.save(entity);
                }
            }
        });
        timeSeriesDataUpdated.accept(nodeId, timeSeriesName);
    }

    void clearTimeSeries(String nodeId) {
        metaRepo.deleteAllByNodeId(nodeId);
        doubleDataRepo.deleteAllByNodeId(nodeId);
        stringDataRepo.deleteAllByNodeId(nodeId);
        timeSeriesCleared.accept(nodeId);
    }
}
