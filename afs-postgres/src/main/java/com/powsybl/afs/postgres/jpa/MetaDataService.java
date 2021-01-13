/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import com.powsybl.afs.storage.NodeGenericMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@Service
public class MetaDataService {

    private final MetaStringRepository strRepo;
    private final MetaDoubleRepository douRepo;
    private final MetaIntRepository intRepo;
    private final MetaBooleanRepository booRepo;

    @Autowired
    public MetaDataService(MetaStringRepository stringRepository, MetaDoubleRepository doubleRepository,
                           MetaIntRepository integerRepository, MetaBooleanRepository booleanRepository) {
        strRepo = Objects.requireNonNull(stringRepository);
        douRepo = Objects.requireNonNull(doubleRepository);
        intRepo = Objects.requireNonNull(integerRepository);
        booRepo = Objects.requireNonNull(booleanRepository);
    }

    public void saveMetaData(String nodeId, NodeGenericMetadata metadata) {
        final Map<String, String> strings = metadata.getStrings();
        strings.forEach((k, v) -> strRepo.save(new MetaStringEntity().setKey(k).setNodeId(nodeId).setValue(v)));

        final Map<String, Double> doubles = metadata.getDoubles();
        doubles.forEach((k, v) -> douRepo.save(new MetaDoubleEntity().setKey(k).setNodeId(nodeId).setValue(v)));

        final Map<String, Integer> ints = metadata.getInts();
        ints.forEach((k, v) -> intRepo.save(new MetaIntEntity().setKey(k).setNodeId(nodeId).setValue(v)));

        final Map<String, Boolean> booleans = metadata.getBooleans();
        booleans.forEach((k, v) -> booRepo.save(new MetaBooleanEntity().setKey(k).setNodeId(nodeId).setValue(v)));
    }

    public NodeGenericMetadata getMetaDataByNodeId(String nodeId) {
        Map<String, String> strMap = new HashMap<>();
        final List<MetaStringEntity> strs = strRepo.findAllByNodeId(nodeId);
        strs.forEach(entity -> strMap.put(entity.getKey(), entity.getValue()));

        Map<String, Double> doubleMap = new HashMap<>();
        final List<MetaDoubleEntity> doubles = douRepo.findAllByNodeId(nodeId);
        doubles.forEach(entity -> doubleMap.put(entity.getKey(), entity.getValue()));

        final List<MetaIntEntity> ints = intRepo.findAllByNodeId(nodeId);
        Map<String, Integer> intMap = new HashMap<>();
        ints.forEach(entity -> intMap.put(entity.getKey(), entity.getValue()));

        final List<MetaBooleanEntity> bools = booRepo.findAllByNodeId(nodeId);
        Map<String, Boolean> booleanMap = new HashMap<>();
        bools.forEach(e -> booleanMap.put(e.getKey(), e.getValue()));
        return new NodeGenericMetadata(strMap, doubleMap, intMap, booleanMap);
    }

    public void deleteMetaDate(String nodeId) {
        strRepo.deleteByNodeId(nodeId);
        douRepo.deleteByNodeId(nodeId);
        intRepo.deleteByNodeId(nodeId);
        booRepo.deleteByNodeId(nodeId);
    }
}
