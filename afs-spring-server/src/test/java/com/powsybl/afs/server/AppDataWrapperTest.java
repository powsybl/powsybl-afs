package com.powsybl.afs.server;

import com.powsybl.afs.AppData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.server.ResponseStatusException;

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
        ResponseStatusException error = assertThrows(ResponseStatusException.class, () -> appDataWrapper.getStorage("fileSystem"));
        assertEquals("404 NOT_FOUND \"App file system 'fileSystem' not found\"", error.getMessage());
    }

    @Test
    void failedToGetFileSystem() {
        Mockito.when(appData.getFileSystem("fileSystem")).thenReturn(null);
        ResponseStatusException error = assertThrows(ResponseStatusException.class, () -> appDataWrapper.getFileSystem("fileSystem"));
        assertEquals("404 NOT_FOUND \"App file system 'fileSystem' not found\"", error.getMessage());
    }
}
