package com.simpledb;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleDb {

    private String url;
    private String user;
    private String password;
    private String database;
    private boolean devMode;
    private boolean autoCommit;
    private Connection conn;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public SimpleDb(String url, String user, String password, String database) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.database = database;
        autoCommit = true;
    }

    private synchronized void connectDb() {
        try {
            conn = DriverManager.getConnection("jdbc:mysql://%s/%s?user=%s&password=%s".formatted(url, database, user, password));
            conn.setAutoCommit(autoCommit);
            if (devMode) {
                logConnected();
            }
        } catch (SQLException e) {
            if (devMode) {
                logSqlExceptionMessage(e);
            }
        }
    }

    /**
     * SQL문을 실행한다
     */
    public void run(final String statement) {
        Statement stmt = null;
        try {
            connectDb();
            stmt = conn.createStatement();
            stmt.execute(statement);
        } catch (SQLException e) {
            logSqlExceptionMessage(e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                    conn.close();
                } catch (SQLException e) {
                    if (devMode) {
                        logger.warning("Failed to close PreparedStatement: " + e.getMessage());
                    }
                }
            }
        }
    }

    public void run(final String statement, final Object... objects) {
        PreparedStatement psmt = null;
        try {
            connectDb();
            psmt = conn.prepareStatement(statement);
            for(int i = 0; i < objects.length; i++) {
                psmt.setObject(i + 1, objects[i]);
            }
            psmt.execute();
        } catch (SQLException e) {
            logSqlExceptionMessage(e);
        } finally {
            if (psmt != null) {
                try {
                    psmt.close();
                    conn.close();
                } catch (SQLException e) {
                    if (devMode) {
                        logger.warning("Failed to close PreparedStatement: " + e.getMessage());
                    }
                }
            }
        }
    }

    public void setDevMode(final boolean devMode) {
        this.devMode = devMode;
    }

    public Sql genSql() {
        connectDb();
        return new Sql(conn);
    }

    public void startTransaction() {
        autoCommit = false;
    }

    public void commit() {
        try {
            conn.commit();
            autoCommit = true;
        } catch (SQLException e) {
            if (devMode) {
                logSqlExceptionMessage(e);
            }
        }
    }

    public void rollback() {
        try {
            conn.rollback();
            autoCommit = true;
        } catch (SQLException e) {
            if (devMode) {
                logSqlExceptionMessage(e);
            }
        }
    }

    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            if (devMode) {
                logSqlExceptionMessage(e);
            }
        }
    }

    private void logConnected() {
        logger.info("Connect to Database : " + database);
    }

    private void logSqlExceptionMessage(SQLException e) {
        logger.warning("SQLException: " + e.getMessage());
        logger.warning("SQLState: " + e.getSQLState());
        logger.warning("VendorError: " + e.getErrorCode());
    }
}
