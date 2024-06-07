package com.powsybl.afs.ws.server.utils;

import com.powsybl.computation.*;
import org.junit.jupiter.api.*;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;

public class AppDataBeanTest {

    @Mock
    private ComputationManager shortTimeExecutionComputationManager;
    @Mock
    private ComputationManager longTimeExecutionComputationManager;
    @Mock
    private DefaultComputationManagerConfig config;
    @InjectMocks
    private AppDataBean appDataBeanUnderTest;

    @BeforeEach
    void init() {
        appDataBeanUnderTest = new AppDataBean();
        shortTimeExecutionComputationManager = mock(ComputationManager.class);
        longTimeExecutionComputationManager = mock(ComputationManager.class);
        config = mock(DefaultComputationManagerConfig.class);
        appDataBeanUnderTest.config = config;
        openMocks(shortTimeExecutionComputationManager);
        openMocks(longTimeExecutionComputationManager);
    }

    @Test
    void reinitComputationManagerNominal() {
        // GIVEN
        appDataBeanUnderTest.longTimeExecutionComputationManager = longTimeExecutionComputationManager;
        appDataBeanUnderTest.shortTimeExecutionComputationManager = shortTimeExecutionComputationManager;
        // WHEN
        appDataBeanUnderTest.reinitComputationManager();
        // THEN
        verify(longTimeExecutionComputationManager, times(1)).close();
        verify(shortTimeExecutionComputationManager, times(1)).close();
        try (
                com.powsybl.computation.ComputationManager shortComputationManager = verify(config, times(1)).createShortTimeExecutionComputationManager();
                com.powsybl.computation.ComputationManager longComputationManager = verify(config, times(1)).createLongTimeExecutionComputationManager()
        ) {
            // do nothing
        } finally {
            // do nothing
        }
    }

    @Test
    void reinitComputationManagerManagerThrowsException() {
        // GIVEN
        appDataBeanUnderTest.longTimeExecutionComputationManager = longTimeExecutionComputationManager;
        appDataBeanUnderTest.shortTimeExecutionComputationManager = shortTimeExecutionComputationManager;
        doThrow(new Exception("SHORT")).when(shortTimeExecutionComputationManager).close();
        doThrow(new Exception("LONG")).when(longTimeExecutionComputationManager).close();
        // WHEN
        appDataBeanUnderTest.reinitComputationManager();
        // THEN
        verify(longTimeExecutionComputationManager, times(1)).close();
        verify(shortTimeExecutionComputationManager, times(1)).close();
        try (
                com.powsybl.computation.ComputationManager shortComputationManager = verify(config, times(1)).createShortTimeExecutionComputationManager();
                com.powsybl.computation.ComputationManager longComputationManager = verify(config, times(1)).createLongTimeExecutionComputationManager()
        ){
            verifyNoMoreInteractions(config);
        }
    }

    @Test
    void reinitComputationManagerManagerNull() {
        // GIVEN
        appDataBeanUnderTest.longTimeExecutionComputationManager = null;
        appDataBeanUnderTest.shortTimeExecutionComputationManager = null;
        // WHEN
        appDataBeanUnderTest.reinitComputationManager();
        // THEN
        verify(longTimeExecutionComputationManager, times(0)).close();
        verify(shortTimeExecutionComputationManager, times(0)).close();
        try (
                com.powsybl.computation.ComputationManager shortComputationManager = verify(config, times(1)).createShortTimeExecutionComputationManager();
                com.powsybl.computation.ComputationManager longComputationManager = verify(config, times(1)).createLongTimeExecutionComputationManager()
        ){
            verifyNoMoreInteractions(config);
        }
    }
}
