package com.rrtx.payment.lock.db;

import com.rrtx.payment.lock.ex.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class DbDistributedLockFactory {

    final static Logger log = LoggerFactory.getLogger(AbstractDbDistributedLockTemple.class);

    long expireTime = 1;
    TimeUnit unit = TimeUnit.SECONDS;
    DataSource dataSource;
    String prefix = "";

    public DbDistributedLockFactory setExpireTime(long expireTime) {
        this.expireTime = expireTime;
        return this;
    }

    public DbDistributedLockFactory setUnit(TimeUnit unit) {
        this.unit = unit;
        return this;
    }

    public DbDistributedLockFactory setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public DbDistributedLockFactory setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public AbstractDbDistributedLockTemple buildDbDistributedLock(){
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            String driverName = connection.getMetaData().getDriverName();
            driverName = driverName.toLowerCase();
            AbstractDbDistributedLockTemple distributedLockTemple = null;
            if(driverName.indexOf("mysql") > -1){
                distributedLockTemple = MysqlDistributedLock.newInstance(this.expireTime,this.unit,this.dataSource,this.prefix);
            }else if(driverName.indexOf("oracle") > -1){
                distributedLockTemple = OracleDistributedLock.newInstance(this.expireTime,this.unit,this.dataSource,this.prefix);
            }else {
                throw new LockException("暂不支持的数据库类型");
            }
            return distributedLockTemple;
        } catch (SQLException e) {
            throw new LockException("初始化锁失败",e);
        }finally {
            if(null != connection){
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.info("释放数据库连接异常",e);
                }
            }
        }
    }

}
