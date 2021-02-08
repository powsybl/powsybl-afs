/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import com.powsybl.afs.storage.NodeGenericMetadata;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Service
public class MetaDataService {

    private final MetaStringRepository strRepo;
    private final MetaDoubleRepository douRepo;
    private final MetaIntRepository intRepo;
    private final MetaBooleanRepository booRepo;

    @Setter
    private BiConsumer<String, NodeGenericMetadata> nodeMetadataUpdated;

    @Autowired
    public MetaDataService(MetaStringRepository stringRepository, MetaDoubleRepository doubleRepository,
                           MetaIntRepository integerRepository, MetaBooleanRepository booleanRepository) {
        strRepo = Objects.requireNonNull(stringRepository);
        douRepo = Objects.requireNonNull(doubleRepository);
        intRepo = Objects.requireNonNull(integerRepository);
        booRepo = Objects.requireNonNull(booleanRepository);
    }

    public void saveMetaData(String nodeId, NodeGenericMetadata metadata) {
        deleteMetaDate(nodeId);
        final Map<String, String> strings = metadata.getStrings();
        strings.forEach((k, v) -> strRepo.save(new MetaStringEntity()
                .setField(new NodeMetadataField(nodeId, k))
                .setValue(v)));

        final Map<String, Double> doubles = metadata.getDoubles();
        doubles.forEach((k, v) -> douRepo.save(new MetaDoubleEntity()
                .setField(new NodeMetadataField(nodeId, k))
                .setValue(v)));

        final Map<String, Integer> ints = metadata.getInts();
        ints.forEach((k, v) -> intRepo.save(new MetaIntEntity()
                .setField(new NodeMetadataField(nodeId, k))
                .setValue(v)));

        final Map<String, Boolean> booleans = metadata.getBooleans();
        booleans.forEach((k, v) -> booRepo.save(new MetaBooleanEntity()
                .setField(new NodeMetadataField(nodeId, k))
                .setValue(v)));
    }

    public NodeGenericMetadata getMetaDataByNodeId(String nodeId) {
        Map<String, String> strMap = new HashMap<>();
        final List<MetaStringEntity> strs = strRepo.findAllByFieldId(nodeId);
        strs.forEach(entity -> strMap.put(entity.getField().getField(), entity.getValue()));

        Map<String, Double> doubleMap = new HashMap<>();
        final List<MetaDoubleEntity> doubles = douRepo.findAllByFieldId(nodeId);
        doubles.forEach(entity -> doubleMap.put(entity.getField().getField(), entity.getValue()));

        final List<MetaIntEntity> ints = intRepo.findAllByFieldId(nodeId);
        Map<String, Integer> intMap = new HashMap<>();
        ints.forEach(entity -> intMap.put(entity.field.getField(), entity.getValue()));

        final List<MetaBooleanEntity> bools = booRepo.findAllByFieldId(nodeId);
        Map<String, Boolean> booleanMap = new HashMap<>();
        bools.forEach(e -> booleanMap.put(e.getField().getField(), e.getValue()));
        return new NodeGenericMetadata(strMap, doubleMap, intMap, booleanMap);
    }

    public void deleteMetaDate(String nodeId) {
        strRepo.deleteByFieldId(nodeId);
        douRepo.deleteByFieldId(nodeId);
        intRepo.deleteByFieldId(nodeId);
        booRepo.deleteByFieldId(nodeId);
    }
}
