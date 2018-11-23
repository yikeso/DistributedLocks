package com.rrtx.payment.lock.db;

import com.rrtx.payment.lock.core.AbstractDistributeLockTemple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public abstract class AbstractDbDistributedLockTemple extends AbstractDistributeLockTemple {

    final static Logger log = LoggerFactory.getLogger(AbstractDbDistributedLockTemple.class);

    /**命名空间前缀*/
    String prefix = "";
    DataSource dataSource;

    protected AbstractDbDistributedLockTemple(long expireTime, TimeUnit unit) {
        super(expireTime, unit);
    }

    protected void init() {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(true);
            ps = connection.prepareStatement(getCreateTableSql());
            ps.execute();
        }catch (Exception e){
            throw new RuntimeException("分布式锁初始化失败",e);
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
    protected boolean setNx(String channelId, String trxId) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(true);
            ps = connection.prepareStatement(getInsertTimestampSql());
            ps.setNString(1,prefix + channelId);
            ps.setNString(2,trxId);
            long timestamp = System.currentTimeMillis();
            ps.setLong(3,timestamp);
            ps.execute();
            return true;
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
    protected long get(String channelId, String trxId) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            ps = connection.prepareStatement(getGetTimestampSql());
            ps.setNString(1,trxId);
            ps.setNString(2,prefix + channelId);
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
            ps.setNString(1,trxId);
            ps.setNString(2,prefix + channelId);
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            int ret = resultSet.getInt("TOTAL");
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
            ps.setLong(2,oldTimestamp);
            ps.setNString(3,lockid);
            ps.setNString(4,prefix + namespace);
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
    public boolean updateLock(String namespace, String lockid) {
        long oldTimestamp = get(namespace,lockid);
        return updateLock(namespace,lockid,oldTimestamp);
    }

    @Override
    protected void delete(LockProp lock) {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(true);
            ps = connection.prepareStatement(getTimestamDeleteSql());
            ps.setNString(1,lock.getTrxId());
            ps.setNString(2,prefix + lock.getChannelId());
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

    protected String getPrefix() {
        return prefix;
    }

    protected DataSource getDataSource() {
        return dataSource;
    }

    protected void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    protected void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected abstract String getCreateTableSql();

    protected abstract String getInsertTimestampSql();

    protected abstract String getCountByIdSql();

    protected abstract String getGetTimestampSql();

    protected abstract String getTimestamUpdateByOldTimestampSql();

    protected abstract String getTimestamDeleteSql();
}
