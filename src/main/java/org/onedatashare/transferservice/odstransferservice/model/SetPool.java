package org.onedatashare.transferservice.odstransferservice.model;

import org.apache.commons.pool2.ObjectPool;

public interface SetPool {

    public <T> void setConnectionBag(ObjectPool<T> pool);

}
