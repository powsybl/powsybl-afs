package com.powsybl.afs.ws.server.utils;

import com.powsybl.afs.AppData;
import com.powsybl.commons.PowsyblException;
import com.powsybl.computation.*;
import org.junit.jupiter.api.*;
import org.mockito.Mock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;

class AppDataBeanTest {

    @Mock
    private ComputationManager shortTimeExecutionComputationManager;
    @Mock
    private ComputationManager longTimeExecutionComputationManager;
    @Mock
    private DefaultComputationManagerConfig config;
    @Mock
    private AppData appData;

    private AppDataBean appDataBeanUnderTest;

    @BeforeEach
    void init() {
        appDataBeanUnderTest = new AppDataBean();
        shortTimeExecutionComputationManager = mock(ComputationManager.class);
        longTimeExecutionComputationManager = mock(ComputationManager.class);
        appData = mock(AppData.class);
        config = mock(DefaultComputationManagerConfig.class);
        appDataBeanUnderTest.config = config;
        appDataBeanUnderTest.appData = appData;
        openMocks(shortTimeExecutionComputationManager);
        openMocks(longTimeExecutionComputationManager);
    }

    @Test
    void reinitComputationManagerNominal() {
        // GIVEN
        when(appData.getShortTimeExecutionComputationManager()).thenReturn(shortTimeExecutionComputationManager);
        when(appData.getLongTimeExecutionComputationManager()).thenReturn(longTimeExecutionComputationManager);
        when(config.createLongTimeExecutionComputationManager()).thenReturn(longTimeExecutionComputationManager);
        when(config.createShortTimeExecutionComputationManager()).thenReturn(shortTimeExecutionComputationManager);
        // WHEN
        appDataBeanUnderTest.reinitComputationManager(false);
        // THEN
        verify(longTimeExecutionComputationManager, times(1)).close();
        verify(shortTimeExecutionComputationManager, times(1)).close();
        try (
            com.powsybl.computation.ComputationManager ignored = verify(config, times(1)).createShortTimeExecutionComputationManager();
            com.powsybl.computation.ComputationManager ignored1 = verify(config, times(1)).createLongTimeExecutionComputationManager()
        ) {
            verify(appData, times(1)).setLongTimeExecutionComputationManager(longTimeExecutionComputationManager);
            verify(appData, times(1)).setShortTimeExecutionComputationManager(shortTimeExecutionComputationManager);
            verifyNoMoreInteractions(config);
        }
    }

    @Test
    void reinitComputationManagerManagerExceptionLogged() {
        // GIVEN
        when(appData.getShortTimeExecutionComputationManager()).thenReturn(shortTimeExecutionComputationManager);
        when(appData.getLongTimeExecutionComputationManager()).thenReturn(longTimeExecutionComputationManager);
        when(config.createLongTimeExecutionComputationManager()).thenReturn(longTimeExecutionComputationManager);
        when(config.createShortTimeExecutionComputationManager()).thenReturn(shortTimeExecutionComputationManager);
        doThrow(new Exception("SHORT")).when(shortTimeExecutionComputationManager).close();
        doThrow(new Exception("LONG")).when(longTimeExecutionComputationManager).close();
        // WHEN
        appDataBeanUnderTest.reinitComputationManager(false);
        // THEN
        verify(longTimeExecutionComputationManager, times(1)).close();
        verify(shortTimeExecutionComputationManager, times(1)).close();
        try (
            com.powsybl.computation.ComputationManager ignored = verify(config, times(1)).createShortTimeExecutionComputationManager();
            com.powsybl.computation.ComputationManager ignored1 = verify(config, times(1)).createLongTimeExecutionComputationManager()
        ) {
            verifyNoMoreInteractions(config);
        }
    }

    @Test
    void reinitComputationManagerManagerThrowsExceptionShortTime() {
        // GIVEN
        when(appData.getShortTimeExecutionComputationManager()).thenReturn(shortTimeExecutionComputationManager);
        when(appData.getLongTimeExecutionComputationManager()).thenReturn(longTimeExecutionComputationManager);
        doThrow(new Exception("SHORT")).when(shortTimeExecutionComputationManager).close();
        // WHEN
        PowsyblException exception = assertThrows(PowsyblException.class, () -> appDataBeanUnderTest.reinitComputationManager(true));
        // THEN
        verify(longTimeExecutionComputationManager, times(0)).close();
        verify(shortTimeExecutionComputationManager, times(1)).close();
        try (
            com.powsybl.computation.ComputationManager ignored = verify(config, times(0)).createShortTimeExecutionComputationManager();
            com.powsybl.computation.ComputationManager ignored1 = verify(config, times(0)).createLongTimeExecutionComputationManager()
        ) {
            verifyNoMoreInteractions(config);
        }
        assertEquals("Error while closing existing connection to the short-time execution computation manager", exception.getMessage());
    }

    @Test
    void reinitComputationManagerManagerThrowsExceptionLongTime() {
        // GIVEN
        when(appData.getShortTimeExecutionComputationManager()).thenReturn(shortTimeExecutionComputationManager);
        when(appData.getLongTimeExecutionComputationManager()).thenReturn(longTimeExecutionComputationManager);
        doThrow(new Exception("LONG")).when(longTimeExecutionComputationManager).close();
        // WHEN
        PowsyblException exception = assertThrows(PowsyblException.class, () -> appDataBeanUnderTest.reinitComputationManager(true));
        // THEN
        verify(longTimeExecutionComputationManager, times(1)).close();
        verify(shortTimeExecutionComputationManager, times(1)).close();
        try (
            com.powsybl.computation.ComputationManager ignored = verify(config, times(0)).createShortTimeExecutionComputationManager();
            com.powsybl.computation.ComputationManager ignored1 = verify(config, times(0)).createLongTimeExecutionComputationManager()
        ) {
            verifyNoMoreInteractions(config);
        }
        assertEquals("Error while closing existing connection to the long-time execution computation manager", exception.getMessage());
    }

    @Test
    void reinitComputationManagerManagerNull() {
        // GIVEN
        when(appData.getShortTimeExecutionComputationManager()).thenReturn(null);
        when(appData.getLongTimeExecutionComputationManager()).thenReturn(null);
        // WHEN
        appDataBeanUnderTest.reinitComputationManager(false);
        // THEN
        verify(longTimeExecutionComputationManager, times(0)).close();
        verify(shortTimeExecutionComputationManager, times(0)).close();
        try (
            com.powsybl.computation.ComputationManager ignored = verify(config, times(1)).createShortTimeExecutionComputationManager();
            com.powsybl.computation.ComputationManager ignored1 = verify(config, times(1)).createLongTimeExecutionComputationManager()
        ) {
            verifyNoMoreInteractions(config);
        }
    }

    @Test
    void reinitComputationWithConfigNull() {
        // GIVEN
        when(appData.getShortTimeExecutionComputationManager()).thenReturn(null);
        when(appData.getLongTimeExecutionComputationManager()).thenReturn(null);
        appDataBeanUnderTest.config = null;
        // WHEN
        appDataBeanUnderTest.reinitComputationManager(false);
        // THEN
        verify(longTimeExecutionComputationManager, times(0)).close();
        verify(shortTimeExecutionComputationManager, times(0)).close();
        assertNotNull(appDataBeanUnderTest.config);
        verifyNoMoreInteractions(config);
    }

    @Test
    void reInit() {
        // GIVEN
        appDataBeanUnderTest.config = null;
        appDataBeanUnderTest.appData = null;
        // WHEN
        appDataBeanUnderTest.init();
        // THEN
        assertNotNull(appDataBeanUnderTest.config);
        assertNotNull(appDataBeanUnderTest.appData);
    }

    @Test
    void cleanCase1() {
        // GIVEN
        when(appData.getShortTimeExecutionComputationManager()).thenReturn(shortTimeExecutionComputationManager);
        when(appData.getLongTimeExecutionComputationManager()).thenReturn(longTimeExecutionComputationManager);
        // WHEN
        appDataBeanUnderTest.clean();
        // THEN
        verify(appData, times(1)).close();
        verify(longTimeExecutionComputationManager, times(1)).close();
        verify(shortTimeExecutionComputationManager, times(1)).close();
        verifyNoMoreInteractions(config);
    }

    @Test
    void cleanCase2() {
        // GIVEN
        when(appData.getShortTimeExecutionComputationManager()).thenReturn(shortTimeExecutionComputationManager);
        when(appData.getLongTimeExecutionComputationManager()).thenReturn(null);
        // WHEN
        appDataBeanUnderTest.clean();
        // THEN
        verify(appData, times(1)).close();
        verify(longTimeExecutionComputationManager, times(0)).close();
        verify(shortTimeExecutionComputationManager, times(1)).close();
        verifyNoMoreInteractions(config);
    }
}
