package com.rrtx.payment.lock.core;

public interface ILock {

    /**
     * 分布式锁接口
     * @param namespace 渠道id
     * @param lockid 交易流水号
     * @return
     */
    boolean tryLock(String namespace,String lockid);

    /**
     * 是否上锁
     * @param namespace
     * @param lockid
     * @return
     */
    boolean isLocked(String namespace,String lockid);

    boolean updateLock(String namespace,String lockid);

    /**
     * 释放锁
     * @param namespace
     * @param lockid
     */
    void release(String namespace,String lockid);

}
