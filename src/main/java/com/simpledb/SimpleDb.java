package com.simpledb;

import java.sql.*;
import java.util.logging.Logger;

public class SimpleDb {

    private String url;
    private String user;
    private String password;
    private String database;
    private boolean devMode;
    private Connection conn;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public SimpleDb(String url, String user, String password, String database) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.database = database;
    }

    private void connectDb() {
        try {
            conn = DriverManager.getConnection("jdbc:mysql://%s/%s?user=%s&password=%s".formatted(url, database, user, password));
        } catch (SQLException ex) {
            logger.warning("SQLException: " + ex.getMessage());
            logger.warning("SQLState: " + ex.getSQLState());
            logger.warning("VendorError: " + ex.getErrorCode());
        }
    }

    /**
     * SQL문을 실행한다
     */
    public void run(String statement) {
        Statement stmt = null;
        try {
            connectDb();
            stmt = conn.createStatement();
            stmt.execute(statement);
        } catch (SQLException ex) {
            logger.warning("SQLException: " + ex.getMessage());
            logger.warning("SQLState: " + ex.getSQLState());
            logger.warning("VendorError: " + ex.getErrorCode());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                    conn.close();
                } catch (SQLException ex) {
                    logger.warning("Failed to close PreparedStatement: " + ex.getMessage());
                }
            }
        }
    }

    public void run(String statement, Object... objects) {
        PreparedStatement psmt = null;
        try {
            connectDb();
            psmt = conn.prepareStatement(statement);
            for(int i = 0; i < objects.length; i++) {
                psmt.setObject(i + 1, objects[i]);
            }
            psmt.execute();
        } catch (SQLException ex) {
            logger.warning("SQLException: " + ex.getMessage());
            logger.warning("SQLState: " + ex.getSQLState());
            logger.warning("VendorError: " + ex.getErrorCode());
        } finally {
            if (psmt != null) {
                try {
                    psmt.close();
                    conn.close();
                } catch (SQLException ex) {
                    logger.warning("Failed to close PreparedStatement: " + ex.getMessage());
                }
            }
        }
    }

    public void setDevMode(boolean devMode) {
        this.devMode = devMode;
    }

    public Sql genSql() {
        connectDb();
        return new Sql(conn);
    }

    public void startTransaction() {
    }

    public void commit() {
    }

    public void rollback() {
    }

    public void close() {
    }
}
