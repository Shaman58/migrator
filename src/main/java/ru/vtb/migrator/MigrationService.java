package ru.vtb.migrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.vtb.migrator.dto.ColumnMetaData;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class MigrationService {

    private final DataSource mysqlDataSource;
    private final DataSource postgresDataSource;

    public void migrate(String tableFilePath) throws Exception {
        List<String> tableNames = loadTableNames(tableFilePath);

        try (Connection mysqlConn = mysqlDataSource.getConnection();
             Connection pgConn = postgresDataSource.getConnection()) {

            for (String tableName : tableNames) {
                List<ColumnMetaData> columns = getTableMetaData(mysqlConn, tableName).get(tableName);

                // создание целевых таблиц можно закоментить если не нужно
//                createTables(tableName, columns, pgConn);
                //--------------------------------------------------------


                migrateTable(tableName, mysqlConn, pgConn, columns);
            }
        }
    }

    private Map<String, List<ColumnMetaData>> getTableMetaData(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, null, tableName, null);

        Map<String, List<ColumnMetaData>> tableMeta = new HashMap<>();
        List<ColumnMetaData> columnList = new ArrayList<>();

        while (columns.next()) {
            columnList.add(new ColumnMetaData(
                    columns.getString("COLUMN_NAME"),
                    columns.getString("TYPE_NAME")
            ));
        }
        tableMeta.put(tableName, columnList);
        return tableMeta;
    }

    private List<String> loadTableNames(String filePath) throws IOException {
        return Files.readAllLines(Paths.get(filePath));
    }

    private String generateInsertSQL(String tableName, List<ColumnMetaData> columns) {
        String columnNames = columns.stream().map(ColumnMetaData::getName).collect(Collectors.joining(", "));
        String valuePlaceholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + tableName + " (" + columnNames + ") VALUES (" + valuePlaceholders + ")";
    }

    public void migrateTable(String tableName, Connection mysqlConnection, Connection postgresConnection, List<ColumnMetaData> columns) throws SQLException {
        int minId = getMinId(tableName, mysqlConnection);
        int maxId = getMaxId(tableName, mysqlConnection);
        String insertSQL = generateInsertSQL(tableName, columns);

        log.info("Начало миграции таблицы {}: ID от {} до {}", tableName, minId, maxId);

        for (int currentId = minId; currentId <= maxId; currentId++) {
            log.info("Ищем по ID: {}", currentId);

            // чтение строки из mysql
            String selectSQL = "SELECT * FROM " + tableName + " WHERE id = " + currentId;

            try (Statement selectStmt = mysqlConnection.createStatement();
                 ResultSet rs = selectStmt.executeQuery(selectSQL)) {

                if (rs.next()) { //есть ли чо с этим id?
                    try (PreparedStatement insertStmt = postgresConnection.prepareStatement(insertSQL)) {
                        // вносим данные для вставки
                        for (int i = 0; i < columns.size(); i++) {
                            Object value = rs.getObject(columns.get(i).getName());
                            insertStmt.setObject(i + 1, value);
                        }
                        // вставка
                        insertStmt.executeUpdate();
                    }
                }
            } catch (SQLException e) {
                log.error("Ошибка с ID {}. message: {}", currentId, e.getMessage());
            }
        }

        log.info("Миграция таблицы {} завершена.", tableName);
    }

    private int getMinId(String tableName, Connection mysqlConnection) throws SQLException {
        String sql = "SELECT MIN(id) FROM " + tableName;
        try (Statement stmt = mysqlConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        throw new SQLException("Не удалось получить минимальный ID из таблицы " + tableName);
    }

    private int getMaxId(String tableName, Connection mysqlConnection) throws SQLException {
        String sql = "SELECT MAX(id) FROM " + tableName;
        try (Statement stmt = mysqlConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        throw new SQLException("Не удалось получить максимальный ID из таблицы " + tableName);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    // создание таблыц
    private void createTables(String tableName, List<ColumnMetaData> columns, Connection pgConn) throws SQLException {
        String createTableSQL = generateCreateTableSQL(tableName, columns);
        pgConn.createStatement().execute(createTableSQL);
    }

    // создание запроса на создание таблы
    private String generateCreateTableSQL(String tableName, List<ColumnMetaData> columns) {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        for (ColumnMetaData column : columns) {
            sql.append(column.getName())
                    .append(" ")
                    .append(mapToPostgresType(column.getType()))
                    .append(", ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(");");
        return sql.toString();
    }

    //маппинг расширить если планируем создавать таблы в авто режиме
    private String mapToPostgresType(String mysqlType) {
        return switch (mysqlType.toUpperCase()) {
            case "VARCHAR", "TEXT" -> "VARCHAR";
            case "INT", "INTEGER" -> "INTEGER";
            case "BIGINT" -> "BIGINT";
            case "DOUBLE" -> "DOUBLE PRECISION";
            case "TIMESTAMP" -> "TIMESTAMP";
            case "BLOB" -> "BYTEA";
            case "BIT" -> "BOOLEAN";
            default -> "TEXT";
        };
    }
}