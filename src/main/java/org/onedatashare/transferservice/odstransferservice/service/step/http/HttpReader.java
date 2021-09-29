package org.onedatashare.transferservice.odstransferservice.service.step.http;

import com.fasterxml.jackson.databind.util.ClassUtil;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.pools.HttpConnectionPool;
import org.onedatashare.transferservice.odstransferservice.service.FilePartitioner;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import javax.swing.text.html.parser.Entity;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;

public class HttpReader<T> extends AbstractItemCountingItemStreamItemReader<DataChunk> {

    HttpClient client;
    HttpConnectionPool httpConnectionPool;
    EntityInfo fileInfo;
    FilePartitioner filePartitioner;
    int chunkSize;
    ByteBuffer buffer;


    public HttpReader(EntityInfo fileInfo, int chunkSize) {
        this.setExecutionContextName(ClassUtils.getShortName(HttpReader.class));
        this.fileInfo = fileInfo;
        this.filePartitioner = new FilePartitioner(chunkSize);
        this.chunkSize = chunkSize;
        buffer = ByteBuffer.allocate(this.chunkSize);
    }


    @Override
    protected DataChunk doRead() throws Exception {
        return null;
    }

    @Override
    protected void doOpen() throws Exception {
        this.client = this.httpConnectionPool.borrowObject();
        HttpRequest request = HttpRequest.newBuilder()
                                .GET()
                                .uri(URI.create("https://" + fileInfo.getPath() + fileInfo.getId()))
                                .setHeader("User-Agent", "Java11 Http")
                                .build();
        HttpResponse<String> response = this.client.send(request, HttpResponse.BodyHandlers.ofString());
        String header = response.body();
    }

    @Override
    protected void doClose() throws Exception {

    }
}
