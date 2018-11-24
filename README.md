# DistributedLocks
基于db的分布式锁，实现，只支持Oracle和mysql

......

......



DbDistributedLockFactory lockFactory = new DbDistributedLockFactory();

lockFactory.setDataSource(dataSource);

lockFactory.setExpireTime(5);

lockFactory.setPrefix("SFT_");

AbstractDbDistributedLockTemple lock = lockFactory.buildDbDistributedLock();



.......

lock.tryLock(namespace,lockid);

lock.isLocked(namespace,lockid);

lock.updateLock(namespace,lockid);

lock.isLocked(namespace,lockid);
