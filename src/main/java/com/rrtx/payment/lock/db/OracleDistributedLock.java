package com.rrtx.payment.lock.db;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

/**
 * oracle分布式锁的实现
 */
public class OracleDistributedLock extends AbstractDbDistributedLockTemple {
    /**创建分布式锁相关表*/
    static final String CREATE_TABLE_SQL = "DECLARE\n" +
            "  NUM NUMBER;\n" +
            "BEGIN\n" +
            "  SELECT COUNT(1)\n" +
            "    INTO NUM\n" +
            "    FROM USER_TABLES\n" +
            "   WHERE TABLE_NAME = UPPER('PUB_DISTRIBUTED_LOCK');\n" +
            "  IF NUM < 1 THEN\n" +
            "    EXECUTE IMMEDIATE 'CREATE TABLE PUB_DISTRIBUTED_LOCK(\n" +
            "                                  NAME_SPACE VARCHAR2(225) NOT NULL,\n" +
            "                                  KEY VARCHAR2(50) NOT NULL,\n" +
            "                                  LOCK_TIME NUMBER(15),\n" +
            "                                  PRIMARY KEY (NAME_SPACE,KEY)\n" +
            "                                  )';\n" +
            "  END IF;\n" +
            "END;\n";
    /**插入锁的时间戳*/
    static final String INSERT_TIMESTAMP_SQL = "INSERT INTO PUB_DISTRIBUTED_LOCK (NAME_SPACE,KEY,LOCK_TIME) VALUES(?,?,?)";
    /**获取锁的时间戳*/
    static final String GET_TIMESTAMP_SQL = "SELECT LOCK_TIME FROM PUB_DISTRIBUTED_LOCK WHERE KEY = ? AND NAME_SPACE = ?";
    static final String COUNT_BY_ID_SQL = "SELECT COUNT(*) TOTAL FROM PUB_DISTRIBUTED_LOCK WHERE KEY = ? AND NAME_SPACE = ?";
    /**利用数据库行锁更新时间戳*/
    static final String TIMESTAM_UPDATE_BY_OLDTIMESTAMP_SQL = "UPDATE PUB_DISTRIBUTED_LOCK SET LOCK_TIME = ? WHERE LOCK_TIME = ? AND KEY = ? AND NAME_SPACE = ?";
    /**释放锁*/
    static final String TIMESTAM_DELETE_SQL = "DELETE FROM PUB_DISTRIBUTED_LOCK WHERE KEY = ? AND NAME_SPACE = ?";

    protected OracleDistributedLock(long expireTime, TimeUnit unit) {
        super(expireTime, unit);
    }

    @Override
    protected void init() {
        super.init();
    }

    /**
     * 静态工厂方法创建oracle实现分布锁
     * @param expireTime
     * @param unit
     * @param dataSource
     * @param prefix
     * @return
     */
    public static OracleDistributedLock newInstance(long expireTime, TimeUnit unit,DataSource dataSource,String prefix){
        OracleDistributedLock lock = new OracleDistributedLock(expireTime,unit);
        lock.setPrefix(prefix);
        lock.setDataSource(dataSource);
        lock.init();
        return lock;
    }

    protected String getCreateTableSql() {
        return CREATE_TABLE_SQL;
    }

    protected String getInsertTimestampSql() {
        return INSERT_TIMESTAMP_SQL;
    }

    protected String getGetTimestampSql() {
        return GET_TIMESTAMP_SQL;
    }

    @Override
    protected String getCountByIdSql() {
        return COUNT_BY_ID_SQL;
    }

    @Override
    protected String getTimestamUpdateByOldTimestampSql() {
        return TIMESTAM_UPDATE_BY_OLDTIMESTAMP_SQL;
    }

    protected String getTimestamDeleteSql() {
        return TIMESTAM_DELETE_SQL;
    }
}
