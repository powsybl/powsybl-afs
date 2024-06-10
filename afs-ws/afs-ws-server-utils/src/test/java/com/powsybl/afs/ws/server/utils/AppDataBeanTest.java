package com.powsybl.afs.ws.server.utils;

import com.powsybl.commons.PowsyblException;
import com.powsybl.computation.*;
import org.junit.jupiter.api.*;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;

class AppDataBeanTest {

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
    void reinitComputationManagerManagerExceptionLogged() {
        // GIVEN
        appDataBeanUnderTest.longTimeExecutionComputationManager = longTimeExecutionComputationManager;
        appDataBeanUnderTest.shortTimeExecutionComputationManager = shortTimeExecutionComputationManager;
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
        appDataBeanUnderTest.longTimeExecutionComputationManager = longTimeExecutionComputationManager;
        appDataBeanUnderTest.shortTimeExecutionComputationManager = shortTimeExecutionComputationManager;
        doThrow(new Exception("SHORT")).when(shortTimeExecutionComputationManager).close();
        // WHEN
        PowsyblException exception = assertThrows(PowsyblException.class, () -> {
            appDataBeanUnderTest.reinitComputationManager(true);
        });
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
        appDataBeanUnderTest.longTimeExecutionComputationManager = longTimeExecutionComputationManager;
        appDataBeanUnderTest.shortTimeExecutionComputationManager = shortTimeExecutionComputationManager;
        doThrow(new Exception("LONG")).when(longTimeExecutionComputationManager).close();
        // WHEN
        PowsyblException exception = assertThrows(PowsyblException.class, () -> {
            appDataBeanUnderTest.reinitComputationManager(true);
        });
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
        appDataBeanUnderTest.longTimeExecutionComputationManager = null;
        appDataBeanUnderTest.shortTimeExecutionComputationManager = null;
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
}
