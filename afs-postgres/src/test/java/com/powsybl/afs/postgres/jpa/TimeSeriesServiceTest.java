/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {TimeSeriesServiceTest.class})
@EnableAutoConfiguration
public class TimeSeriesServiceTest {

    @Autowired
    TimeSeriesMetadataRepository metadataRepository;

    TimeSeriesService service;

    @BeforeEach
    void setup() {
        service = new TimeSeriesService(metadataRepository);
    }

    @Test
    void test() {
//        final AfsEntityId id = new AfsEntityId("nodeId", "name");
//        final TimeSeriesMetadataEntity timeSeriesMetadataEntity = new TimeSeriesMetadataEntity(id, "type");
//        metadataRepository.save(timeSeriesMetadataEntity);

//        assertThat(metadataRepository.findById(id)).hasValueSatisfying(e -> {
//            assertEquals("type", e.getType());
//        });
//
//        final List<String> nodeId = metadataRepository.findAllMetadataNameByNodeId("nodeId");
//        assertThat(nodeId).hasOnlyOneElementSatisfying(e -> assertEquals("name", e));
    }
}
