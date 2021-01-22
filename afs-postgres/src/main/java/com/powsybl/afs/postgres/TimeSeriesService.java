/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
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
    private final UncompressedDoubleDataRepository uncompressedDoubleRepo;
    private final CompressedDoubleDataRepository compressedDoubleRepo;
    private final UncompressedStringDataRepository uncompressedStringRepo;
    private final CompressedStringDataRepository compressedStringRepo;
    private final ChunkRepository chunkRepository;

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
                                ChunkRepository chunkRepository,
                                UncompressedDoubleDataRepository uncompressedDoubleDataRepository,
                                CompressedDoubleDataRepository compressedDoubleDataRepository,
                                UncompressedStringDataRepository uncompressedStringDataRepository,
                                CompressedStringDataRepository compressedStringDataRepository) {
        metaRepo = Objects.requireNonNull(tsmdRepo);
        tagRepo = Objects.requireNonNull(tsTagRepository);
        regTsiRepo = Objects.requireNonNull(regularTimeSeriesIndexRepository);
        this.irrTsiRepo = Objects.requireNonNull(irrTsiRepo);
        uncompressedDoubleRepo = Objects.requireNonNull(uncompressedDoubleDataRepository);
        uncompressedStringRepo = Objects.requireNonNull(uncompressedStringDataRepository);
        compressedDoubleRepo = Objects.requireNonNull(compressedDoubleDataRepository);
        compressedStringRepo = Objects.requireNonNull(compressedStringDataRepository);
        this.chunkRepository = Objects.requireNonNull(chunkRepository);
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
                        .setPoint(i) // TODO remove point
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
        return getIrrIndex(e);
    }

    private IrregularTimeSeriesIndex getIrrIndex(TimeSeriesMetadataEntity e) {
        final Iterable<IrregularTimeSeriesIndexEntity> irrPoints = irrTsiRepo.findAllByTsmdIdOrderByPoint(e.getId());
        List<Instant> list = new ArrayList<>();
        for (IrregularTimeSeriesIndexEntity entity : irrPoints) {
            list.add(Instant.ofEpochMilli(entity.getEpoch()));
        }
        return IrregularTimeSeriesIndex.create(list);
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
            final ChunkEntity chunkEntity = new ChunkEntity()
                    .setNodeId(nodeId).setVersion(version)
                    .setTsName(timeSeriesName).setMyOffset(offset).setDataType(TimeSeriesDataType.DOUBLE.name());
            final ChunkEntity savedChunk = chunkRepository.save(chunkEntity);
            if (c.isCompressed()) {
                final CompressedDoubleDataChunk chunk = (CompressedDoubleDataChunk) c;
                final double[] stepValues = chunk.getStepValues();
                final int[] stepLengths = chunk.getStepLengths();
                for (int i = 0; i < stepLengths.length; i++) {
                    CompressedDoubleDataEntity entity = new CompressedDoubleDataEntity()
                            .setChunk(savedChunk).setI(i).setStepLength(stepLengths[i]).setValue(stepValues[i]);
                    compressedDoubleRepo.save(entity);
                }
            } else {
                final UncompressedDoubleDataChunk chunk = (UncompressedDoubleDataChunk) c;
                final double[] values = chunk.getValues();
                for (int i = 0; i < values.length; i++) {
                    UncompressedDoubleDataEntity doubleDataEntity = new UncompressedDoubleDataEntity()
                            .setChunk(savedChunk).setI(i).setValue(values[i]);
                    uncompressedDoubleRepo.save(doubleDataEntity);
                }
            }
        });
        timeSeriesDataUpdated.accept(nodeId, timeSeriesName);
    }

    Set<Integer> getTimeSeriesDataVersions(String nodeId) {
        return chunkRepository.findDistinctVersionsByNodeId(nodeId);
    }

    Set<Integer> getTimeSeriesDataVersions(String nodeId, String timeSeriesName) {
        return chunkRepository.findDistinctVersionsByNodeIdAndTsName(nodeId, timeSeriesName);
    }

    Map<String, List<DoubleDataChunk>> getDoubleTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        Map<String, List<DoubleDataChunk>> map = new HashMap<>();
        for (String name : timeSeriesNames) {
            final List<ChunkEntity> chunks = chunkRepository.findAllByNodeIdAndTsNameAndVersionAndDataType(nodeId, name, version, TimeSeriesDataType.DOUBLE.name());
            if (chunks.isEmpty()) {
                continue;
            }
            List<DoubleDataChunk> res = new ArrayList<>();
            for (ChunkEntity chunkEntity : chunks) {
                tryGetFromUncompressedDouble(res, chunkEntity);
                tryGetFromCompressedDouble(res, chunkEntity);
            }
            map.put(name, res);
        }
        return map;
    }

    private void tryGetFromUncompressedDouble(List<DoubleDataChunk> res, ChunkEntity chunkEntity) {
        final List<UncompressedDoubleDataEntity> chunkFromUncompressed = uncompressedDoubleRepo.findAllByChunkOrderByI(chunkEntity);
        if (chunkFromUncompressed.isEmpty()) {
            return;
        }
        List<Double> values = new ArrayList<>();
        for (UncompressedDoubleDataEntity dataEntity : chunkFromUncompressed) {
            values.add(dataEntity.getValue());
        }
        res.add(new UncompressedDoubleDataChunk(chunkEntity.getMyOffset(), Doubles.toArray(values)));
    }

    private void tryGetFromUncompressedString(List<StringDataChunk> res, ChunkEntity chunkEntity) {
        final List<UncompressedStringDataEntity> chunkFromUncompressed = uncompressedStringRepo.findAllByChunkOrderByI(chunkEntity);
        if (chunkFromUncompressed.isEmpty()) {
            return;
        }
        List<String> values = new ArrayList<>();
        for (UncompressedStringDataEntity dataEntity : chunkFromUncompressed) {
            values.add(dataEntity.getValue());
        }
        res.add(new UncompressedStringDataChunk(chunkEntity.getMyOffset(), values.toArray(new String[0])));
    }

    private void tryGetFromCompressedDouble(List<DoubleDataChunk> res, ChunkEntity chunkEntity) {
        final List<CompressedDoubleDataEntity> datas = compressedDoubleRepo.findAllByChunkOrderByI(chunkEntity);
        if (datas.isEmpty()) {
            return;
        }
        List<Double> stepValues = new ArrayList<>();
        List<Integer> stepLength = new ArrayList<>();
        for (CompressedDoubleDataEntity dataEntity : datas) {
            stepValues.add(dataEntity.getValue());
            stepLength.add(dataEntity.getStepLength());
        }
        int sumStep = stepLength.stream().mapToInt(i -> i).sum();
        res.add(new CompressedDoubleDataChunk(chunkEntity.getMyOffset(), sumStep, Doubles.toArray(stepValues), Ints.toArray(stepLength)));
    }

    private void tryGetFromCompressedString(List<StringDataChunk> res, ChunkEntity chunkEntity) {
        final List<CompressedStringDataEntity> datas = compressedStringRepo.findAllByChunkOrderByI(chunkEntity);
        if (datas.isEmpty()) {
            return;
        }
        List<String> stepValues = new ArrayList<>();
        List<Integer> stepLength = new ArrayList<>();
        for (CompressedStringDataEntity dataEntity : datas) {
            stepValues.add(dataEntity.getValue());
            stepLength.add(dataEntity.getStepLength());
        }
        int sumStep = stepLength.stream().mapToInt(i -> i).sum();
        res.add(new CompressedStringDataChunk(chunkEntity.getMyOffset(), sumStep, stepValues.toArray(new String[0]), Ints.toArray(stepLength)));
    }

    Map<String, List<StringDataChunk>> getStringTimeSeriesData(String nodeId, Set<String> timeSeriesNames, int version) {
        Map<String, List<StringDataChunk>> map = new HashMap<>();
        for (String name : timeSeriesNames) {
            final List<ChunkEntity> chunks = chunkRepository.findAllByNodeIdAndTsNameAndVersionAndDataType(nodeId, name, version, TimeSeriesDataType.STRING.name());
            if (chunks.isEmpty()) {
                continue;
            }
            List<StringDataChunk> res = new ArrayList<>();
            for (ChunkEntity chunkEntity : chunks) {
                tryGetFromUncompressedString(res, chunkEntity);
                tryGetFromCompressedString(res, chunkEntity);
            }
            map.put(name, res);
        }
        return map;
    }

    void addStringTimeSeriesData(String nodeId, int version, String timeSeriesName, List<StringDataChunk> chunks) {
        chunks.forEach(c -> {
            int offset = c.getOffset();
            final ChunkEntity chunkEntity = new ChunkEntity()
                    .setNodeId(nodeId).setVersion(version)
                    .setTsName(timeSeriesName).setMyOffset(offset).setDataType(TimeSeriesDataType.STRING.name());
            final ChunkEntity savedChunk = chunkRepository.save(chunkEntity);
            if (c.isCompressed()) {
                final CompressedStringDataChunk chunk = (CompressedStringDataChunk) c;
                final String[] stepValues = chunk.getStepValues();
                final int[] stepLengths = chunk.getStepLengths();
                for (int i = 0; i < stepLengths.length; i++) {
                    CompressedStringDataEntity entity = new CompressedStringDataEntity()
                            .setChunk(savedChunk).setI(i).setStepLength(stepLengths[i]).setValue(stepValues[i]);
                    compressedStringRepo.save(entity);
                }
            } else {
                final UncompressedStringDataChunk chunk = (UncompressedStringDataChunk) c;
                final String[] values = chunk.getValues();
                for (int i = 0; i < values.length; i++) {
                    UncompressedStringDataEntity doubleDataEntity = new UncompressedStringDataEntity()
                            .setChunk(savedChunk).setI(i).setValue(values[i]);
                    uncompressedStringRepo.save(doubleDataEntity);
                }
            }
        });
        timeSeriesDataUpdated.accept(nodeId, timeSeriesName);
    }

    void clearTimeSeries(String nodeId) {
        // TODO jpa relationship delete
        final List<TimeSeriesMetadataEntity> allByNodeId = metaRepo.findAllByNodeId(nodeId);
        for (TimeSeriesMetadataEntity metadataEntity : allByNodeId) {
            regTsiRepo.deleteByMetadataEntity(metadataEntity);
            tagRepo.deleteByMetadataEntity(metadataEntity);
        }
        metaRepo.deleteAllByNodeId(nodeId);
        final Iterable<ChunkEntity> chunks = chunkRepository.findAllByNodeId(nodeId);
        for (ChunkEntity chunk : chunks) {
            uncompressedDoubleRepo.deleteAllByChunk(chunk);
            uncompressedStringRepo.deleteAllByChunk(chunk);
            compressedDoubleRepo.deleteAllByChunk(chunk);
            compressedStringRepo.deleteAllByChunk(chunk);
        }
        chunkRepository.deleteAllByNodeId(nodeId);
        timeSeriesCleared.accept(nodeId);
    }
}
