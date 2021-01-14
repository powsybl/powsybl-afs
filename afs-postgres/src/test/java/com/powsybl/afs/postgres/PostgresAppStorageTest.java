/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs.postgres;

import com.powsybl.afs.postgres.jpa.NodeDataRepository;
import com.powsybl.afs.storage.AbstractAppStorageTest;
import com.powsybl.afs.storage.AppStorage;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Yichen TANG <yichen.tang at rte-france.com>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {PostgresAppStorageTest.class})
@ComponentScan(basePackages = {"com.powsybl.afs.postgres.jpa", "com.powsybl.afs.postgres"})
@EnableAutoConfiguration
public class PostgresAppStorageTest extends AbstractAppStorageTest {
    @Autowired
    private NodeService nodeService;
    @Autowired
    private TimeSeriesService tsService;
    @Autowired
    private NodeDataRepository nodeDataRepository;

    @Override
    protected AppStorage createStorage() {
        return new PostgresAppStorage(nodeService, nodeDataRepository, tsService);
    }
}
