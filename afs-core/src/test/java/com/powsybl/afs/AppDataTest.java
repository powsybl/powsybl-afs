package com.powsybl.afs;

import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class AppDataTest {
    @Mock
    private ComputationManager shortTimeExecutionComputationManager;
    @Mock
    private ComputationManager longTimeExecutionComputationManager;
    private AppData appDataUnderTest;

    @BeforeEach
    void init() {
        shortTimeExecutionComputationManager = mock(ComputationManager.class);
        longTimeExecutionComputationManager = mock(ComputationManager.class);
        appDataUnderTest = new AppData(shortTimeExecutionComputationManager, longTimeExecutionComputationManager);
    }

    @Test
    void setLongTimeExecutionComputationManager() {
        // GIVEN
        appDataUnderTest.setLongTimeExecutionComputationManager(null);
        Assertions.assertEquals(shortTimeExecutionComputationManager, appDataUnderTest.getLongTimeExecutionComputationManager());
        // WHEN
        appDataUnderTest.setLongTimeExecutionComputationManager(longTimeExecutionComputationManager);
        // THEN
        assertNotNull(appDataUnderTest.getLongTimeExecutionComputationManager());
    }

    @Test
    void setShortTimeExecutionComputationManager() {
        // GIVEN
        appDataUnderTest.setShortTimeExecutionComputationManager(null);
        Assertions.assertNull(appDataUnderTest.getShortTimeExecutionComputationManager());
        // WHEN
        appDataUnderTest.setShortTimeExecutionComputationManager(shortTimeExecutionComputationManager);
        // THEN
        assertNotNull(appDataUnderTest.getShortTimeExecutionComputationManager());
    }
}
