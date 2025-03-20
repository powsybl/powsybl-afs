package com.powsybl.afs.server;

import com.powsybl.afs.AppData;
import com.powsybl.afs.storage.AfsFileSystemNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 *
 * @author THIYAGARASA Pratheep Ext
 */
@ExtendWith(MockitoExtension.class)
class AppDataWrapperTest {

    @InjectMocks
    private AppDataWrapper appDataWrapper;

    @Mock
    private AppData appData;

    @Test
    void failedToGetStorage() {
        Mockito.when(appData.getRemotelyAccessibleStorage("fileSystem")).thenReturn(null);
        AfsFileSystemNotFoundException error = assertThrows(AfsFileSystemNotFoundException.class, () -> appDataWrapper.getStorage("fileSystem"));
        assertEquals("App file system 'fileSystem' not found", error.getMessage());
    }

    @Test
    void failedToGetFileSystem() {
        Mockito.when(appData.getFileSystem("fileSystem")).thenReturn(null);
        AfsFileSystemNotFoundException error = assertThrows(AfsFileSystemNotFoundException.class, () -> appDataWrapper.getFileSystem("fileSystem"));
        assertEquals("App file system 'fileSystem' not found", error.getMessage());
    }
}
