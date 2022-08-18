package ua.tn;

import org.postgresql.ds.common.BaseDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class PooledDataSource extends BaseDataSource implements DataSource {
    private final Queue<Connection> connectionQueue;
    private final Object lock = new Object();

    public PooledDataSource(String url, String user, String password) throws SQLException {
        super.setURL(url);
        var initialCapacity = 10;
        connectionQueue = new ArrayBlockingQueue<>(initialCapacity);
        for (int i = 0; i < initialCapacity; i++) {
            var connection = super.getConnection(user, password);
            connectionQueue.add(new ConnectionProxy(connection, connectionQueue));
        }
    }

    @Override
    public Connection getConnection() {
        synchronized (this.lock) {
            while (true) {
                if (!connectionQueue.isEmpty()) {
                    return connectionQueue.poll();
                } else {
                    try {
                        System.out.println("Pool is empty, waiting for available connection..");
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public String getDescription() {
        return "Pooling DataSource from PostgreSQL JDBC Driver";
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(this.getClass())) {
            return iface.cast(this);
        } else {
            throw new SQLException("Cannot unwrap to " + iface.getName());
        }
    }
}
