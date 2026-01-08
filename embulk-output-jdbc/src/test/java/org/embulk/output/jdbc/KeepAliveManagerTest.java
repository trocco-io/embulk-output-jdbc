package org.embulk.output.jdbc;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for KeepAliveManager.
 *
 * These tests verify that:
 * 1. KeepAliveManager starts and executes keep-alive queries
 * 2. KeepAliveManager stops properly when stop() is called
 * 3. KeepAliveManager handles connection errors gracefully
 * 4. pause() blocks until in-progress queries complete
 * 5. resume() allows queries to continue
 */
public class KeepAliveManagerTest {

    private Connection mockConnection;
    private Statement mockStatement;
    private ResultSet mockResultSet;

    @Before
    public void setUp() throws SQLException {
        mockConnection = mock(Connection.class);
        mockStatement = mock(Statement.class);
        mockResultSet = mock(ResultSet.class);

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery("SELECT 1")).thenReturn(mockResultSet);
    }

    @After
    public void tearDown() throws SQLException {
        // Clean up mocks
        reset(mockConnection, mockStatement, mockResultSet);
    }

    @Test
    public void testStartAndStop() throws Exception {
        // Start keep-alive with a short interval for testing
        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 100L);

        assertNotNull("KeepAliveManager should be created", manager);

        // Wait for at least one keep-alive query to be executed
        Thread.sleep(250);

        // Stop the manager
        manager.stop();

        // Verify that at least one keep-alive query was executed
        verify(mockConnection, atLeastOnce()).createStatement();
        verify(mockStatement, atLeastOnce()).executeQuery("SELECT 1");
    }

    @Test
    public void testStopPreventsMoreQueries() throws Exception {
        AtomicInteger queryCount = new AtomicInteger(0);

        // Create a mock that counts queries
        when(mockStatement.executeQuery("SELECT 1")).thenAnswer(invocation -> {
            queryCount.incrementAndGet();
            return mockResultSet;
        });

        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 50L);

        // Wait for some queries to execute
        Thread.sleep(150);

        // Stop the manager
        manager.stop();

        int countAfterStop = queryCount.get();

        // Wait a bit more
        Thread.sleep(150);

        // Count should not have increased significantly after stop
        int countAfterWait = queryCount.get();

        // The count should be the same or at most 1 more (if a query was in flight)
        assertTrue("Queries should stop after stop() is called",
                countAfterWait <= countAfterStop + 1);
    }

    @Test
    public void testHandlesSQLException() throws Exception {
        // Simulate a SQL exception on the first query, success on subsequent queries
        AtomicInteger callCount = new AtomicInteger(0);
        when(mockStatement.executeQuery("SELECT 1")).thenAnswer(invocation -> {
            if (callCount.incrementAndGet() == 1) {
                throw new SQLException("Connection lost");
            }
            return mockResultSet;
        });

        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 50L);

        // Wait for multiple query attempts
        Thread.sleep(200);

        // Stop the manager - should not throw
        manager.stop();

        // Verify that multiple queries were attempted despite the exception
        verify(mockStatement, atLeast(2)).executeQuery("SELECT 1");
    }

    @Test
    public void testDefaultInterval() throws Exception {
        // Test that default interval is used (30 seconds)
        // We just verify that start() without interval works
        KeepAliveManager manager = KeepAliveManager.start(mockConnection);

        assertNotNull("KeepAliveManager should be created with default interval", manager);

        // Immediately stop - we don't want to wait 30 seconds in a test
        manager.stop();
    }

    @Test
    public void testMultipleStopCalls() throws Exception {
        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 100L);

        // Stop multiple times should not throw
        manager.stop();
        manager.stop();
        manager.stop();

        // If we get here without exception, the test passes
    }

    @Test
    public void testClosedStatementAfterQuery() throws Exception {
        // Verify that Statement is closed after each query (try-with-resources behavior)
        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 100L);

        Thread.sleep(250);

        manager.stop();

        // Statement.close() should have been called
        verify(mockStatement, atLeastOnce()).close();
    }

    @Test
    public void testPausePreventsQueries() throws Exception {
        AtomicInteger queryCount = new AtomicInteger(0);

        when(mockStatement.executeQuery("SELECT 1")).thenAnswer(invocation -> {
            queryCount.incrementAndGet();
            return mockResultSet;
        });

        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 50L);

        // Wait for at least one query
        Thread.sleep(100);

        // Pause the manager
        manager.pause();
        int countAfterPause = queryCount.get();

        // Wait while paused
        Thread.sleep(150);

        // Count should not have increased while paused
        int countWhilePaused = queryCount.get();
        assertEquals("No queries should execute while paused", countAfterPause, countWhilePaused);

        manager.stop();
    }

    @Test
    public void testResumeAllowsQueries() throws Exception {
        AtomicInteger queryCount = new AtomicInteger(0);

        when(mockStatement.executeQuery("SELECT 1")).thenAnswer(invocation -> {
            queryCount.incrementAndGet();
            return mockResultSet;
        });

        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 50L);

        // Wait for at least one query
        Thread.sleep(100);

        // Pause
        manager.pause();
        int countAfterPause = queryCount.get();

        // Wait while paused
        Thread.sleep(100);

        // Resume
        manager.resume();

        // Wait for more queries after resume
        Thread.sleep(150);

        int countAfterResume = queryCount.get();

        // Queries should have resumed
        assertTrue("Queries should resume after resume() is called",
                countAfterResume > countAfterPause);

        manager.stop();
    }

    @Test
    public void testPauseWaitsForInProgressQuery() throws Exception {
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch queryCanComplete = new CountDownLatch(1);
        AtomicBoolean queryCompleted = new AtomicBoolean(false);

        // Create a mock that blocks during query execution
        when(mockStatement.executeQuery("SELECT 1")).thenAnswer(invocation -> {
            queryStarted.countDown();
            // Wait until we're told to complete
            queryCanComplete.await(5, TimeUnit.SECONDS);
            queryCompleted.set(true);
            return mockResultSet;
        });

        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 50L);

        // Wait for query to start
        boolean started = queryStarted.await(1, TimeUnit.SECONDS);
        assertTrue("Query should have started", started);

        // Start pause in a separate thread (it should block)
        AtomicBoolean pauseCompleted = new AtomicBoolean(false);
        Thread pauseThread = new Thread(() -> {
            manager.pause();
            pauseCompleted.set(true);
        });
        pauseThread.start();

        // Give pause thread time to start
        Thread.sleep(50);

        // pause() should not have completed yet (query is still running)
        assertFalse("pause() should block while query is in progress", pauseCompleted.get());

        // Allow the query to complete
        queryCanComplete.countDown();

        // Wait for pause to complete
        pauseThread.join(1000);

        // Now pause should be completed
        assertTrue("pause() should complete after query finishes", pauseCompleted.get());
        assertTrue("Query should have completed", queryCompleted.get());

        manager.stop();
    }

    @Test
    public void testMultiplePauseCalls() throws Exception {
        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 100L);

        // Multiple pause calls should not throw
        manager.pause();
        manager.pause();
        manager.pause();

        manager.stop();
    }

    @Test
    public void testMultipleResumeCalls() throws Exception {
        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 100L);

        manager.pause();

        // Multiple resume calls should not throw
        manager.resume();
        manager.resume();
        manager.resume();

        manager.stop();
    }

    @Test
    public void testPauseResumeDoesNotAffectStop() throws Exception {
        KeepAliveManager manager = KeepAliveManager.start(mockConnection, 100L);

        manager.pause();
        manager.resume();
        manager.stop();

        // After stop, pause and resume should be safe to call
        manager.pause();
        manager.resume();

        // If we get here without exception, the test passes
    }
}
