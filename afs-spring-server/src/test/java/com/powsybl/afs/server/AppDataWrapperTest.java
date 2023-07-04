package com.powsybl.afs.server;

import com.powsybl.afs.AppData;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 *
 * @author THIYAGARASA Pratheep Ext
 */
@RunWith(MockitoJUnitRunner.class)
public class AppDataWrapperTest {

    @InjectMocks
    private AppDataWrapper appDataWrapper;

    @Mock
    private AppData appData;

    @Test
    public void failedToGetStorage() {
        Mockito.when(appData.getRemotelyAccessibleStorage("fileSystem")).thenReturn(null);
        assertThatThrownBy(() -> appDataWrapper.getStorage("fileSystem"))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessage("404 NOT_FOUND \"App file system 'fileSystem' not found\"");
    }

    @Test
    public void failedToGetFileSystem() {
        Mockito.when(appData.getFileSystem("fileSystem")).thenReturn(null);
        assertThatThrownBy(() -> appDataWrapper.getFileSystem("fileSystem"))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessage("404 NOT_FOUND \"App file system 'fileSystem' not found\"");
    }
}
