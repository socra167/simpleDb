package com.simpledb;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Sql {

    private final Connection conn;
    private final StringBuilder statementBuilder;
    private final List<Object> params;
    private final Logger logger = Logger.getLogger(this.getClass().getName());

    public Sql(final Connection conn) {
        this.conn = conn;
        this.statementBuilder = new StringBuilder();
        params = new ArrayList<>();
    }

    public Sql append(final String statement) {
        statementBuilder.append(statement);
        statementBuilder.append(' ');
        return this;
    }

    /**
     * SQL문의 Statement, 인자값 추가
     *
     * @param statement SQL statement
     * @param objects   values
     */
    public Sql append(final String statement, final Object... objects) {
        statementBuilder.append(statement);
        statementBuilder.append(' ');
        for (Object object : objects) {
            params.add(object);
        }
        return this;
    }

    public Sql appendIn(final String statement, final Object... objects) {
        StringBuilder tmpStatementBuilder = new StringBuilder(statement);
        int placeHolderIndex = tmpStatementBuilder.indexOf("?");
        for (int i = 0; i < objects.length - 1; i++) {
            tmpStatementBuilder.insert(placeHolderIndex, "?, ");
        }
        statementBuilder.append(tmpStatementBuilder);
        for (Object object : objects) {
            params.add(object);
        }
        return this;
    }

    /**
     * INSERT문 실행
     *
     * @return AUTO_INCREMENT 에 의해서 생성된 주키
     */
    public long insert() {
        try {
            final PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString(), Statement.RETURN_GENERATED_KEYS);
            setObjectsToStatement(psmt);
            psmt.executeUpdate();
            try (ResultSet generatedKeys = psmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    // 자동 생성된 키 값 (AUTO_INCREMENT id)
                    long generatedId = generatedKeys.getLong(1);
                    return generatedId;
                }
            }
        } catch (SQLException e) {
            logger.warning("Failed to execute INSERT query : " + e.getMessage());
        }
        return 0;
    }

    /**
     * UPDATE문 실행
     *
     * @return 수정된 row 개수
     */
    public int update() {
        try {
            final PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            final int affectedRow = psmt.executeUpdate();
            close(psmt);
            return affectedRow;
        } catch (SQLException e) {
            logger.warning("Failed to execute UPDATE query : " + e.getMessage());
            return 0;
        }
    }

    /**
     * DELETE문 실행
     *
     * @return 삭제된 row 개수
     */
    public int delete() {
        try {
            final PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            final int affectedRow = psmt.executeUpdate();
            close(psmt);
            return affectedRow;
        } catch (SQLException e) {
            logger.warning("Failed to execute DELETE query : " + e.getMessage());
            return 0;
        }
    }

    /**
     * 하나의 Row에 대한 SELECT문 실행
     *
     * @return SELECT 결과 Map
     */
    public Map<String, Object> selectRow() {
        final Map<String, Object> resultMap = new HashMap<>();
        final ResultSet rs;
        try {
            final PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            final ResultSetMetaData rsmd = rs.getMetaData();
            final int columnSize = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 0; i < columnSize; i++) {
                    resultMap.put(rsmd.getColumnName(i + 1), rs.getObject(i + 1));
                }
            }
            close(rs, psmt);
            return resultMap;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return resultMap;
    }

    /**
     * 여러 Row에 대한 SELECT문 실행
     *
     * @return SELECT 결과 Map의 List
     */
    public List<Map<String, Object>> selectRows() {
        final List<Map<String, Object>> resultList = new ArrayList<>();
        final ResultSet rs;
        Map<String, Object> resultMap;
        try {
            final PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            final ResultSetMetaData rsmd = rs.getMetaData();
            final int columnSize = rsmd.getColumnCount();
            while (rs.next()) {
                resultMap = new HashMap<>();
                for (int i = 0; i < columnSize; i++) {
                    resultMap.put(rsmd.getColumnName(i + 1), rs.getObject(i + 1));
                }
                resultList.add(resultMap);
            }
            close(rs, psmt);
            return resultList;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return resultList;
    }

    /**
     * 하나의 Row에 대한 SELECT문 실행
     *
     * @param clazz SELECT 하려는 entity의 클래스
     * @return SELECT 결과 entity 객체
     */
    public <T> T selectRow(final Class<T> clazz) {
        T resultObject = null;
        final ResultSet rs;

        try {
            final PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            rs = psmt.executeQuery();
            final ResultSetMetaData rsmd = rs.getMetaData();
            int columnSize = rsmd.getColumnCount();

            rs.next();
            resultObject = getInstanceFromResult(clazz, columnSize, rsmd, rs);

            close(rs, psmt);
            return resultObject;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        } catch (InvocationTargetException | IllegalAccessException | InstantiationException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return resultObject;
    }

    private <T> T getInstanceFromResult(Class<T> clazz, int columnSize, ResultSetMetaData rsmd, ResultSet rs) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final Map<String, Object> resultMap = new HashMap<>();
        // ResultMap에 조회된 데이터를 추가
        for (int i = 0; i < columnSize; i++) {
            resultMap.put(rsmd.getColumnName(i + 1), rs.getObject(i + 1));
        }

        // 인스턴스로 만든 후 setter 메소드로 필드값 저장
        T resultObject = clazz.getDeclaredConstructor().newInstance();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            String setterMethodName = "set" + capitalize(field.getName());
            Method setterMethod = clazz.getMethod(setterMethodName, field.getType());
            setterMethod.invoke(resultObject, resultMap.get(field.getName()));
        }
        return resultObject;
    }

    /**
     * 여러 Row에 대한 SELECT문 실행
     *
     * @param clazz SELECT 하려는 entity의 클래스
     * @return SELECT 결과 entity 객체들의 list
     */
    public <T> List<T> selectRows(Class<T> clazz) {
        final List<T> resultList = new ArrayList<>();
        final ResultSet rs;
        T resultObject;

        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            rs = psmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnSize = rsmd.getColumnCount();

            while (rs.next()) {
                resultObject = getInstanceFromResult(clazz, columnSize, rsmd, rs);
                resultList.add(resultObject);
            }
            close(rs, psmt);
            return resultList;

        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        } catch (InvocationTargetException | IllegalAccessException | InstantiationException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return resultList;
    }

    /**
     * 결과값이 Long인 SELECT문 실행
     *
     * @return SELECT 결과
     */
    public Long selectLong() {
        Long result = 0L;
        ResultSet rs;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            rs.next();
            result = rs.getLong(1);
            close(rs, psmt);
            return result;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return result;
    }

    public String selectString() {
        String result = "";
        ResultSet rs;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            rs.next();
            result = rs.getString(1);
            close(rs, psmt);
            return result;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return result;
    }

    public Boolean selectBoolean() {
        final ResultSet rs;
        Boolean result = false;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            rs.next();
            result = rs.getBoolean(1);
            close(rs, psmt);
            return result;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return result;
    }

    public List<Long> selectLongs() {
        final List<Long> resultList = new ArrayList<>();
        final ResultSet rs;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            while (rs.next()) {
                resultList.add(rs.getLong(1));
            }
            close(rs, psmt);
            return resultList;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return resultList;
    }

    public LocalDateTime selectDatetime() {
        final ResultSet rs;
        Timestamp timeStamp;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            rs = psmt.executeQuery();
            rs.next();
            timeStamp = rs.getTimestamp(1);
            close(rs, psmt);
            return timeStamp.toLocalDateTime();
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        throw new RuntimeException("DateTime 조회에 실패했습니다");
    }

    private void setObjectsToStatement(PreparedStatement psmt) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            psmt.setObject(i + 1, params.get(i));
        }
    }

    private String capitalize(String string) {
        if (string == null || string.isEmpty()) {
            return string;
        }
        return string.substring(0, 1).toUpperCase() + string.substring(1);
    }

    private void close(PreparedStatement psmt) throws SQLException {
        psmt.close();
    }

    private void close(final ResultSet rs, final PreparedStatement psmt) throws SQLException {
        rs.close();
        close(psmt);
    }
}
