package com.rrtx.payment.lock.core;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁的模板
 */
public abstract class AbstractDistributeLockTemple implements ILock {

    final static Logger log = LoggerFactory.getLogger(AbstractDistributeLockTemple.class);

    /**锁默认国建时间1000ms*/
    long DEFAULT_EXPIRE_TIME = 1000L;

    protected AbstractDistributeLockTemple(long expireTime, TimeUnit unit) {
        this.DEFAULT_EXPIRE_TIME = TimeUnit.MILLISECONDS.convert(expireTime,unit);
    }

    @Override
    public boolean tryLock(String channelId, String trxId) {
        if(StringUtils.isEmpty(channelId) || StringUtils.isEmpty(trxId)){
            return true;
        }
        try {
            if(setNx(channelId,trxId)){
                return true;
            }
            long timestamp = get(channelId,trxId);
            if(System.currentTimeMillis() - timestamp < DEFAULT_EXPIRE_TIME){
                return false;
            }
            return updateLock(channelId,trxId,timestamp);
        }catch (Exception e){
            log.info("获取锁：",e);
            return false;
        }
    }

    /**
     * 尝试获取锁
     * @param channelId
     * @param trxId
     * @return
     */
    protected abstract boolean setNx(String channelId, String trxId);

    /**
     * 获取 锁的被占用的时间戳
     * @param channelId
     * @param trxId
     * @return
     */
    protected abstract long get(String channelId, String trxId);

    /**
     * 更新锁
     * @param namespace
     * @param lockid
     * @param oldTimestamp
     * @return
     */
    protected abstract boolean updateLock(String namespace, String lockid,long oldTimestamp);

    /**
     * 判断锁是否有效
     * @param namespace
     * @param lockid
     * @return
     */
    @Override
    public boolean isLocked(String namespace, String lockid) {
        if(StringUtils.isEmpty(namespace) || StringUtils.isEmpty(lockid)){
            return false;
        }
        if(!exsit(namespace,lockid)){
            return false;
        }
        long timestamp = get(namespace,lockid);
        if(System.currentTimeMillis() - timestamp < DEFAULT_EXPIRE_TIME){
            return true;
        }
        return false;
    }

    /**
     * 尝试获取锁
     * @param channelId
     * @param trxId
     * @return
     */
    protected abstract boolean exsit(String channelId, String trxId);

    /**
     * 释放锁
     * @param channelId
     * @param trxId
     * @return
     */
    public void release(String channelId, String trxId) {
        if(StringUtils.isEmpty(channelId) || StringUtils.isEmpty(channelId)){
            return;
        }
        delete(new LockProp(channelId, trxId));
    }

    /**
     * 释放锁
     * @return
     */
    protected abstract void delete(LockProp lock);

    protected static class LockProp{

        String channelId;
        String trxId;

        public LockProp(String channelId, String trxId) {
            this.channelId = channelId;
            this.trxId = trxId;
        }

        public String getChannelId() {
            return channelId;
        }

        public String getTrxId() {
            return trxId;
        }
    }
}
