/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres.jpa;

import com.powsybl.afs.storage.NodeGenericMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {MetaStringRepositoryTest.class})
@EnableAutoConfiguration
class MetaStringRepositoryTest {

    @Autowired
    private MetaStringRepository strRepo;
    @Autowired
    private MetaDoubleRepository douRepo;
    @Autowired
    private MetaIntRepository intRepo;
    @Autowired
    private MetaBooleanRepository booRepo;

    private MetaDataService service;

    @BeforeEach
    void setup() {
        service = new MetaDataService(strRepo, douRepo, intRepo, booRepo);
    }

    @Test
    void test() {
        final NodeGenericMetadata metadata = new NodeGenericMetadata();
        metadata.setString("str_k", "str_v");
        metadata.setInt("int_k", 42);
        metadata.setDouble("double_k", 42.0);
        metadata.setBoolean("boolean_k", true);
        service.saveMetaData("nodeId", metadata);

        final NodeGenericMetadata get = service.getMetaDataByNodeId("nodeId");
        assertEquals(metadata, get);
        service.deleteMetaDate("nodeId");
        assertThat(booRepo.findAll()).isNotNull().isEmpty();
        assertThat(booRepo.findAll()).isNotNull().isEmpty();
        assertThat(booRepo.findAll()).isNotNull().isEmpty();
        assertThat(booRepo.findAll()).isNotNull().isEmpty();
    }
}
