package org.onedatashare.transferservice.odstransferservice.service.step.sftp;

import lombok.NoArgsConstructor;
import net.schmizz.sshj.Config;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.Connection;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.UserAuthException;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.session.ClientSession;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.security.*;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;

@Lazy
@NoArgsConstructor
@Component
public class SshConnectionCreator {

    Logger logger = LoggerFactory.getLogger(SshConnectionCreator.class);

    KeyFactory kf;

    public ClientSession authenticateClientSession(SshClient sshClient, AccountEndpointCredential credential) {
        String[] destCredUri = accountCredentialURIFormated(credential);
        try {
            ClientSession clientSession = sshClient.connect(credential.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1])).verify().getSession();
            logger.info("Doing basic auth sftp -- connected the remote ssh server");
            clientSession.addPasswordIdentity(credential.getSecret());
            clientSession.auth().verify();
//            clientSession.switchToNoneCipher();
            return clientSession;
        } catch (IOException e) {
            logger.debug("sshd failed to do basic auth with the account endpoint credential");
        }
        try {
            ClientSession clientSession = sshClient.connect(credential.getUsername(), destCredUri[0], Integer.parseInt(destCredUri[1])).verify().getSession();
            logger.info("Doing pem key auth sftp -- connected the remote ssh server");
            clientSession.getIoSession().setAttribute("SO_KEEPALIVE", true);
            KeyPair keyPair = createKeyPair(credential.getSecret());
            clientSession.addPublicKeyIdentity(keyPair);
            clientSession.auth().verify();
//            clientSession.switchToNoneCipher();
            if (clientSession.isOpen() && clientSession.isAuthenticated())
                logger.info("Authenticated a session using pem key");
            return clientSession;
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
            logger.error("sshd failed to do pem auth with the account endpoint credential");
            e.printStackTrace();
        }
        return null;
    }

    public KeyPair createKeyPair(String secret) throws InvalidKeySpecException, NoSuchAlgorithmException, IOException {
        PrivateKey privateKey = convertStringToKeyPair(secret);
        PublicKey publicKey = publicKeyFromPrivateKey(privateKey);
        return new KeyPair(publicKey, privateKey);
    }

    public PrivateKey convertStringToKeyPair(String secret) throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        StringBuilder pkcs8Lines = new StringBuilder();

        String[] strArr = secret.split("\\n");
        for (int i = 0; i < strArr.length; i++) { //skipping the first two lines that contain the words BEGIN and END
            String cur = strArr[i];
            if (cur.contains("BEGIN")) continue;
            if (cur.contains("END")) continue;
            pkcs8Lines.append(strArr[i]);
        }
        // Remove the "BEGIN" and "END" lines, as well as any whitespace
        String pkcs8Pem = pkcs8Lines.toString();
        pkcs8Pem = pkcs8Pem.replaceAll("\\s+", "");

        // Base64 decode the result
        byte[] pkcs8EncodedBytes = Base64.getMimeDecoder().decode(pkcs8Pem);
        // extract the private key

        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
        this.kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }

    public PublicKey publicKeyFromPrivateKey(PrivateKey privateKey) throws InvalidKeySpecException {
        RSAPrivateCrtKey privk = (RSAPrivateCrtKey) privateKey;
        RSAPublicKeySpec publicKeySpec = new java.security.spec.RSAPublicKeySpec(privk.getModulus(), privk.getPublicExponent());
        return this.kf.generatePublic(publicKeySpec);
    }

    public SSHClient createConnectedAndAuthenticatedSSHClient(AccountEndpointCredential credential, long size) throws IOException {
        String[] destCredUri = accountCredentialURIFormated(credential);
        SSHClient sshClient = new SSHClient();
        logger.info("The SSHClient window size: {}, timeout: {}", sshClient.getConnection().getWindowSize(), sshClient.getConnection().getTimeoutMs());
        sshClient.addHostKeyVerifier(new PromiscuousVerifier());
        sshClient.connect(destCredUri[0], Integer.parseInt(destCredUri[1]));
        Connection conn = sshClient.getConnection();
        conn.setMaxPacketSize(Math.toIntExact(size)+1);
        conn.setWindowSize(size);
        conn.setTimeoutMs(10000);
        logger.info("The SSHClient window size: {}, timeout: {}", sshClient.getConnection().getWindowSize(), sshClient.getConnection().getTimeoutMs());
        try {
            logger.info("Doing basic auth sftp");
            sshClient.authPassword(credential.getUsername(), credential.getSecret());
            if (sshClient.isAuthenticated() && sshClient.isConnected()) logger.info("Connected & authenticated client");
            return sshClient;
        } catch (UserAuthException | TransportException e) {
            logger.debug("sshj failed to do basic auth with the account endpoint credential");
        }
        try {
            logger.info("Doing pem key auth sftp");
            KeyPair kp = this.createKeyPair(credential.getSecret());
            KeyProvider provider = sshClient.loadKeys(kp);
            sshClient.authPublickey(credential.getUsername(), provider);
            if (sshClient.isAuthenticated() && sshClient.isConnected()) logger.info("Connected & authenticated client");
            return sshClient;
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            logger.error("sshj failed to do pem auth with the account endpoint credential");
            e.printStackTrace();
        }
        return null;
    }

    public String[] accountCredentialURIFormated(AccountEndpointCredential credential) {
        String noTypeUri = credential.getUri().replaceFirst("sftp://", "");
        return noTypeUri.split(":");
    }

}
