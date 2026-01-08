package org.embulk.output.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle of a keep-alive task that periodically executes a simple query
 * (e.g., "SELECT 1") to prevent the database connection from being closed due to idle timeout.
 *
 * <p>This is particularly useful when the output connection is established before input processing,
 * and the input processing takes a long time (e.g., fetching data from external APIs).
 * Without keep-alive, intermediate network devices (like AWS Gateway with ~350 second timeout)
 * may terminate idle connections.
 *
 * <p>Thread Safety:
 * This class provides pause() and resume() methods to safely suspend keep-alive queries
 * during batch operations, preventing any concurrent access to the JDBC connection.
 *
 * <p>Usage:
 * <pre>
 * KeepAliveManager keepAlive = KeepAliveManager.start(connection, 30000L);
 * try {
 *     // ... long running operation ...
 *
 *     // Before batch execution:
 *     keepAlive.pause();  // Waits for any in-progress query to complete
 *     try {
 *         batch.executeBatch();
 *     } finally {
 *         keepAlive.resume();
 *     }
 * } finally {
 *     keepAlive.stop();
 * }
 * </pre>
 */
public class KeepAliveManager {
    private static final Logger logger = LoggerFactory.getLogger(KeepAliveManager.class);
    private static final String KEEP_ALIVE_QUERY = "SELECT 1";
    private static final long DEFAULT_INTERVAL_MILLIS = 30000L; // 30 seconds

    private final ExecutorService executor;
    private final Future<?> task;
    private final AtomicBoolean stopped;
    private final AtomicBoolean paused;
    private final ReentrantLock queryLock;

    private KeepAliveManager(ExecutorService executor, Future<?> task, AtomicBoolean stopped,
                             AtomicBoolean paused, ReentrantLock queryLock) {
        this.executor = executor;
        this.task = task;
        this.stopped = stopped;
        this.paused = paused;
        this.queryLock = queryLock;
    }

    /**
     * Starts a background keep-alive task with the default interval (30 seconds).
     *
     * @param connection The JDBC connection to keep alive
     * @return A KeepAliveManager instance to control the keep-alive task
     */
    public static KeepAliveManager start(Connection connection) {
        return start(connection, DEFAULT_INTERVAL_MILLIS);
    }

    /**
     * Starts a background keep-alive task that periodically executes a lightweight query
     * to keep the database connection active.
     *
     * @param connection The JDBC connection to keep alive
     * @param intervalMillis The interval in milliseconds between each keep-alive query execution
     * @return A KeepAliveManager instance to control the keep-alive task
     */
    public static KeepAliveManager start(Connection connection, long intervalMillis) {
        AtomicBoolean stopped = new AtomicBoolean(false);
        AtomicBoolean paused = new AtomicBoolean(false);
        ReentrantLock queryLock = new ReentrantLock();

        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "jdbc-output-keepalive");
            t.setDaemon(true);
            return t;
        });

        Future<?> task = executor.submit(() -> {
            logger.info("Keep-alive task started (interval: {}ms)", intervalMillis);
            try {
                while (!Thread.currentThread().isInterrupted() && !stopped.get()) {
                    Thread.sleep(intervalMillis);
                    if (stopped.get()) {
                        break;
                    }
                    // Skip if paused, but don't block
                    if (paused.get()) {
                        continue;
                    }
                    // Acquire lock before executing query to ensure thread safety
                    queryLock.lock();
                    try {
                        // Double-check after acquiring lock
                        if (stopped.get() || paused.get()) {
                            continue;
                        }
                        try (Statement stmt = connection.createStatement()) {
                            stmt.executeQuery(KEEP_ALIVE_QUERY);
                        } catch (SQLException e) {
                            if (!stopped.get()) {
                                logger.warn("Keep-alive query failed: {}", e.getMessage());
                            }
                        }
                    } finally {
                        queryLock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            logger.info("Keep-alive task stopped");
        });

        return new KeepAliveManager(executor, task, stopped, paused, queryLock);
    }

    /**
     * Pauses the keep-alive task. This method blocks until any in-progress keep-alive query
     * completes, ensuring no concurrent access to the connection after this method returns.
     *
     * <p>Call this method before executing batch operations to prevent concurrent connection access.
     * Always call {@link #resume()} after the batch operation completes.
     *
     * <p>This method is idempotent and can be called multiple times safely.
     */
    public void pause() {
        paused.set(true);
        // Acquire and immediately release the lock to wait for any in-progress query to complete
        queryLock.lock();
        queryLock.unlock();
    }

    /**
     * Resumes the keep-alive task after it was paused.
     *
     * <p>This method is idempotent and can be called multiple times safely.
     */
    public void resume() {
        paused.set(false);
    }

    /**
     * Stops the keep-alive task permanently by cancelling the running thread and
     * shutting down the executor.
     *
     * <p>This method is idempotent and can be called multiple times safely.
     */
    public void stop() {
        stopped.set(true);
        paused.set(true);  // Also set paused to prevent any new queries
        task.cancel(true);
        executor.shutdownNow();
    }
}
