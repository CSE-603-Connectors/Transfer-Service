package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpUtility {

    static Logger logger = LoggerFactory.getLogger(SftpUtility.class);


    public static Session createJschSession(JSch jsch, AccountEndpointCredential credential) {
        String noTypeUri = credential.getUri().replaceFirst("sftp://", "");
        String[] destCredUri = noTypeUri.split(":");
        boolean connected = false;
        Session jschSession = null;
        logger.info(credential.toString());
        try {
            logger.info("Doing private key auth");
            return authenticateWithUserAndPrivateKey(credential, jsch, destCredUri);
        } catch (JSchException ignored) {
        }
        try {
            logger.info("doing user pass auth");
            return authenticateWithUserPass(credential, jsch, destCredUri);
        } catch (JSchException ignored) {
        }
        return null;
    }

    public static Session authenticateWithUserAndPrivateKey(AccountEndpointCredential credential, JSch jsch, String[] destCredUri) throws JSchException {
        jsch.addIdentity("randomName", credential.getSecret().getBytes(), null, null);
        Session jschSession = jsch.getSession(credential.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1]));
        jschSession.setConfig("StrictHostKeyChecking", "no");
        jschSession.connect();
        return jschSession;
    }

    public static Session authenticateWithUserPass(AccountEndpointCredential credential, JSch jsch, String[] destCredUri) throws JSchException {
        Session jschSession = jsch.getSession(credential.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1]));
        jschSession.setConfig("StrictHostKeyChecking", "no");
        jschSession.setPassword(credential.getSecret());
        jschSession.connect();
        return jschSession;
    }
}
