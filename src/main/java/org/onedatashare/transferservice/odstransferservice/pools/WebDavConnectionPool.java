package org.onedatashare.transferservice.odstransferservice.pools;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.pool2.ObjectPool;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import java.util.*;

import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

public class WebDavConnectionPool implements ObjectPool<HttpClient> {
    AccountEndpointCredential credential;
    String uri;
    LinkedBlockingQueue<HttpClient> connectionPool;
    public void WebDavConnectionPool(AccountEndpointCredential credential, URI uri){
        this.credential = credential;
        this.uri = credential.getUri();
    }

    @Override
    public void addObject() throws Exception, IllegalStateException, UnsupportedOperationException {
        HostConfiguration hostConfig = new HostConfiguration();
        hostConfig.setHost(this.uri);
        HttpClient client = new HttpClient();
        Credentials creds = new UsernamePasswordCredentials(credential.getUsername(), credential.getSecret());
        client.getState().setCredentials(AuthScope.ANY, creds);
        connectionPool.add(client);
    }

    @Override
    public HttpClient borrowObject() throws Exception, NoSuchElementException, IllegalStateException {
        return this.connectionPool.take();
    }

    @Override
    public void clear() throws Exception, UnsupportedOperationException {
        this.connectionPool.clear();
    }

    @Override
    public void close() {
        for(HttpClient client: this.connectionPool) {
            this.connectionPool.remove(client);
        }
    }

    @Override
    public int getNumActive() {
        return 0;
    }

    @Override
    public int getNumIdle() {
        return 0;
    }

    @Override
    public void invalidateObject(HttpClient obj) throws Exception {
        this.connectionPool.remove(obj);
    }

    @Override
    public void returnObject(HttpClient obj) throws Exception {
        this.connectionPool.add(obj);
    }
}
