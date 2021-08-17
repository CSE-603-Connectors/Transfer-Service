package org.onedatashare.transferservice.odstransferservice.model;

import com.box.sdk.BoxFileUploadSession;
import com.box.sdk.BoxFileUploadSessionPart;
import lombok.Data;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Data
public class BoxMultiPartUpload {
    public String fileName;
    public List<BoxFileUploadSessionPart> parts;
    public MessageDigest digest;
    private long size;

    public BoxMultiPartUpload(){
        parts = new ArrayList<>();
    }

    public void prepare(long size){
        try {
            this.digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        this.size = size;
    }

    public void addPart(BoxFileUploadSession session, DataChunk dataChunk){
        BoxFileUploadSessionPart uploadSessionPart = session.uploadPart(dataChunk.getData(), dataChunk.getStartPosition(), Long.valueOf(dataChunk.getSize()).intValue(), this.size);
        this.parts.add(uploadSessionPart);
        digest.update(dataChunk.getData());
    }

    public void finishUpload(BoxFileUploadSession session){
        session.commit(Base64.getEncoder().encodeToString(this.digest.digest()), this.parts, null, null, null);
    }
}
