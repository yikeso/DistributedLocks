package com.rrtx.payment.lock.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * oracle分布式锁的实现
 */
public class MysqlDistributedLock extends AbstractDbDistributedLockTemple {

    final static Logger log = LoggerFactory.getLogger(AbstractDbDistributedLockTemple.class);
    /**创建分布式锁相关表*/
    static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS PUB_DISTRIBUTED_LOCK(\n" +
                                            "NAME_SPACE VARCHAR(225) NOT NULL,\n" +
                                            "KEY VARCHAR(50) NOT NULL,\n" +
                                            "LOCK_TIME BIGINT,\n" +
                                            "PRIMARY KEY (NAME_SPACE,KEY)\n" +
                                            ")ENGINE=InnoDB DEFAULT CHARSET=utf8";
    /**插入锁的时间戳*/
    static final String INSERT_TIMESTAMP_SQL = "INSERT INTO PUB_DISTRIBUTED_LOCK (NAME_SPACE,KEY,LOCK_TIME) VALUES(?,?,?)";
    /**获取锁的时间戳*/
    static final String GET_TIMESTAMP_SQL = "SELECT LOCK_TIME FROM PUB_DISTRIBUTED_LOCK WHERE NAME_SPACE = ? AND KEY = ? ";
    /**countById*/
    static final String COUNT_BY_ID_SQL = "SELECT COUNT(*) TOTAL FROM PUB_DISTRIBUTED_LOCK WHERE NAME_SPACE = ? AND KEY = ? ";
    /**更新锁的时间戳*/
    static final String TIMESTAM_UPDATE_SQL = "UPDATE PUB_DISTRIBUTED_LOCK SET LOCK_TIME = ? WHERE NAME_SPACE = ? AND KEY = ? AND LOCK_TIME = ? ";
    /**释放锁*/
    static final String TIMESTAM_DELETE_SQL = "DELETE FROM PUB_DISTRIBUTED_LOCK WHERE NAME_SPACE = ? AND KEY = ? ";

    protected MysqlDistributedLock(long expireTime, TimeUnit unit) {
        super(expireTime, unit);
    }

    @Override
    protected void init() {
        super.init();
    }

    @Override
    protected long get(String channelId, String trxId) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            ps = connection.prepareStatement(getGetTimestampSql());
            ps.setNString(1,prefix + channelId);
            ps.setNString(2,trxId);
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            long ret = resultSet.getLong("LOCK_TIME");
            resultSet.close();
            return ret;
        }catch (Exception e){
            return System.currentTimeMillis();
        }finally {
            if(null != ps){
                try {
                    ps.close();
                } catch (SQLException e) {
                    log.info("关闭PreparedStatement异常",e);
                }
            }
            if(null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.info("关闭数据库连接异常",e);
                }
            }
        }
    }

    @Override
    protected boolean exsit(String channelId, String trxId) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            ps = connection.prepareStatement(getCountByIdSql());
            ps.setNString(1,prefix + channelId);
            ps.setNString(2,trxId);
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            int ret = resultSet.getInt("TOTAL");
            resultSet.close();
            return ret > 0;
        }catch (Exception e){
            return false;
        }finally {
            if(null != ps){
                try {
                    ps.close();
                } catch (SQLException e) {
                    log.info("关闭PreparedStatement异常",e);
                }
            }
            if(null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.info("关闭数据库连接异常",e);
                }
            }
        }
    }

    @Override
    protected boolean updateLock(String namespace, String lockid, long oldTimestamp) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(true);
            ps = connection.prepareStatement(getTimestamUpdateByOldTimestampSql());
            Long timeStamp = Long.valueOf(System.currentTimeMillis());
            ps.setLong(1,timeStamp);
            ps.setNString(2,prefix + namespace);
            ps.setNString(3,lockid);
            ps.setLong(4,oldTimestamp);
            return ps.executeUpdate() > 0;
        }catch (Exception e){
            if (null != connection) {
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                    log.info("数据库事务回滚异常", e);
                }
            }
            return false;
        }finally {
            if(null != ps){
                try {
                    ps.close();
                } catch (SQLException e) {
                    log.info("关闭PreparedStatement异常",e);
                }
            }
            if(null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.info("关闭数据库连接异常",e);
                }
            }
        }
    }

    @Override
    protected void delete(LockProp lock) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(true);
            ps = connection.prepareStatement(getTimestamDeleteSql());
            ps.setNString(1,prefix + lock.getChannelId());
            ps.setNString(2,lock.getTrxId());
            ps.execute();
        }catch (Exception e){
            log.info("释放锁异常",e);
        }finally {
            if(null != ps){
                try {
                    ps.close();
                } catch (SQLException e) {
                    log.info("关闭PreparedStatement异常",e);
                }
            }
            if(null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.info("关闭数据库连接异常",e);
                }
            }
        }
    }

    /**
     * 静态工厂方法创建oracle实现分布锁
     * @param expireTime
     * @param unit
     * @param dataSource
     * @param prefix
     * @return
     */
    public static MysqlDistributedLock newInstance(long expireTime, TimeUnit unit, DataSource dataSource, String prefix){
        MysqlDistributedLock lock = new MysqlDistributedLock(expireTime,unit);
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
        return TIMESTAM_UPDATE_SQL;
    }

    protected String getTimestamDeleteSql() {
        return TIMESTAM_DELETE_SQL;
    }
}
