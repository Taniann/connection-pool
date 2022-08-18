package ua.tn;

import lombok.SneakyThrows;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

public class App {

    @SneakyThrows
    public static void main(String[] args) {
        DataSource dataSource = initializePolledDataSource();

        for (int j = 0; j < 5; j++) {
            new Thread(() ->
            {
                try {
                    var startTime = System.currentTimeMillis();
                    var total = getTotal(dataSource);
                    System.out.println(total);
                    System.out.println("execution time: " + (System.currentTimeMillis() - startTime) + " ms");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            ).start();
        }
    }

    private static Double getTotal(DataSource dataSource) throws SQLException {
        var total = 0.0;
        for (int i = 0; i < 500; i++) {
            try (var connection = dataSource.getConnection()) {
                try (var statement = connection.createStatement()) {
                    connection.setAutoCommit(false);
                    var resultSet = statement.executeQuery(
                            "select random() from products");
                    resultSet.next();
                    total += resultSet.getDouble(1);

                }
                connection.rollback();
            }
        }
        return total;
    }


    private static DataSource initializeDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL("jdbc:postgresql://localhost:5432/postgres");
        dataSource.setUser("postgres");
        return dataSource;
    }

    @SneakyThrows
    private static DataSource initializePolledDataSource() {
        return new PooledDataSource("jdbc:postgresql://localhost:5432/postgres", "postgres", "");
    }

}
