/**
 * Copyright (c) 2017, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.afs;

import com.google.re2j.Pattern;
import com.powsybl.afs.mapdb.storage.MapDbAppStorage;
import com.powsybl.afs.storage.AppStorage;
import com.powsybl.afs.storage.NodeGenericMetadata;
import com.powsybl.computation.ComputationManager;
import com.powsybl.tools.Command;
import com.powsybl.tools.Tool;
import com.powsybl.tools.ToolRunningContext;
import com.powsybl.tools.test.AbstractToolTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Geoffroy Jamgotchian {@literal <geoffroy.jamgotchian at rte-france.com>}
 */
class AppFileSystemToolTest extends AbstractToolTest {

    private final AppFileSystemTool tool;

    private static final String FOLDER_PSEUDO_CLASS = "folder";

    public AppFileSystemToolTest() {
        ComputationManager computationManager = Mockito.mock(ComputationManager.class);
        tool = new AppFileSystemTool() {
            @Override
            protected AppData createAppData(ToolRunningContext context) {
                AppData appData = new AppData(computationManager, computationManager, Collections.emptyList(),
                    Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
                AppStorage storage = MapDbAppStorage.createMem("mem", appData.getEventsBus());
                AppFileSystem afs = new AppFileSystem("mem", false, storage);
                appData.addFileSystem(afs);
                afs.getRootFolder().createProject("test_project1");
                afs.getRootFolder().createProject("test_project2");
                storage.createNode(afs.getRootFolder().getId(), "test", FOLDER_PSEUDO_CLASS, "", 0,
                    new NodeGenericMetadata().setString("k", "v"));
                storage.flush();
                return appData;
            }
        };
    }

    @Override
    protected Iterable<Tool> getTools() {
        return Collections.singleton(tool);
    }

    @Override
    public void assertCommand() {
        Command command = tool.getCommand();
        assertCommand(command, "afs", 10, 0);
        assertOption(command.getOptions(), "ls", false, true);
        assertOption(command.getOptions(), "archive", false, true);
        assertOption(command.getOptions(), "unarchive", false, true);
        assertOption(command.getOptions(), "ls-inconsistent-nodes", false, true);
        assertOption(command.getOptions(), "fix-inconsistent-nodes", false, true);
        assertOption(command.getOptions(), "rm-inconsistent-nodes", false, true);

        assertEquals("Application file system", command.getTheme());
        assertEquals("application file system command line tool", command.getDescription());
        assertNull(command.getUsageFooter());
    }

    @Test
    void testArchive() throws IOException {
        Path archivePath = fileSystem.getPath("/tmp", UUID.randomUUID().toString());
        Files.createDirectories(archivePath);
        try (Stream<Path> paths = Files.list(archivePath)) {
            assertEquals(0, paths.count());
        } catch (Exception e) {
            fail();
        }
        assertCommandSuccessful(new String[] {"afs", "--archive", "mem", "--dir", archivePath.toString(), "--deleteResults"});
        try (Stream<Path> paths = Files.list(archivePath)) {
            assertEquals(1, paths.count());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    void testLs() {
        assertCommandSuccessful(new String[] {"afs", "--ls"}, "mem" + System.lineSeparator());
        assertCommandSuccessfulMatch(new String[] {"afs", "--ls", "mem:/"}, String.join(System.lineSeparator(), "test_project1", "test_project2"));
    }

    @Test
    void testLsInconsistentNodes() {
        assertCommandSuccessfulRegex(new String[] {"afs", "--ls-inconsistent-nodes", "mem"},
            Pattern.compile("mem:" + System.lineSeparator() + "[a-z0-9-]+" + System.lineSeparator()));
        assertCommandSuccessfulRegex(new String[] {"afs", "--ls-inconsistent-nodes"},
            Pattern.compile("mem:" + System.lineSeparator() + "[a-z0-9-]+" + System.lineSeparator()));
    }

    @Test
    void testFixInconsistentNodes() {
        assertCommandSuccessfulRegex(new String[] {"afs", "--fix-inconsistent-nodes", "mem"},
            Pattern.compile("mem:" + System.lineSeparator() + "[a-z0-9-]+ fixed"));
        assertCommandErrorMatch(new String[] {"afs", "--fix-inconsistent-nodes"}, 3, "IllegalArgumentException");
        assertCommandSuccessfulRegex(new String[] {"afs", "--ls-inconsistent-nodes", "mem", "nodeId"},
            Pattern.compile("mem:" + System.lineSeparator() + "[a-z0-9-]+" + System.lineSeparator()));
    }

    @Test
    void testRemoveInconsistentNodes() {
        assertCommandSuccessfulRegex(new String[] {"afs", "--rm-inconsistent-nodes", "mem"},
            Pattern.compile("mem:" + System.lineSeparator() + "[a-z0-9-]+ cleaned"));
        assertCommandErrorMatch(new String[] {"afs", "--rm-inconsistent-nodes"}, 3, "IllegalArgumentException");
    }
}
