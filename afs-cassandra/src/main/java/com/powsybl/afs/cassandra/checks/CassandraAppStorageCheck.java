package com.powsybl.afs.cassandra.checks;

import com.powsybl.afs.storage.check.FileSystemCheckIssue;
import com.powsybl.afs.storage.check.FileSystemCheckOptions;

import java.util.List;

/**
 * A sanity check for cassandra app storage.
 *
 * @author Sylvain Leclerc <sylvain.leclerc at rte-france.com>
 */
public interface CassandraAppStorageCheck {

    /**
     * Name of that check
     */
    String getName();

    /**
     * Perform the app storage check and return identified issues.
     */
    List<FileSystemCheckIssue> check(CassandraAppStorageCheckSupport context, FileSystemCheckOptions options);

}
