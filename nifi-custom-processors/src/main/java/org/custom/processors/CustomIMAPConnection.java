package org.custom.processors;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;

import javax.mail.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class CustomIMAPConnection {
    private final AtomicReference<Store> storeRef = new AtomicReference<>();
    private final AtomicReference<Folder> folderRef = new AtomicReference<>();
    private final ComponentLog logger;
    private long lastReconnectAttempt = 0;

    private static final String AUTH_PASSWORD = "PASSWORD";
    private static final String AUTH_OAUTH2 = "OAUTH2";

    public CustomIMAPConnection(ComponentLog logger) {
        this.logger = logger;
    }

    public synchronized boolean connect(String host, int port, String user, String password,
                                        String authMode, String folder,
                                        boolean useSsl, boolean useTls,
                                        boolean markRead, boolean deleteMessages,
                                        boolean partialFetch, long fetchBufferSize,
                                        long connectionTimeoutMs,
                                        OAuth2AccessTokenProvider tokenProvider) {

        try {
            Properties props = getProperties(host, port, useSsl, useTls,
                    partialFetch, fetchBufferSize,
                    connectionTimeoutMs, authMode);

            Session session = Session.getInstance(props);
            if (logger.isDebugEnabled()) session.setDebug(true);

            String protocol = useSsl ? "imaps" : "imap";
            Store store = session.getStore(protocol);

            if (AUTH_PASSWORD.equals(authMode)) {
                store.connect(host, port, user, password);
            } else {
                if (tokenProvider == null) {
                    throw new MessagingException("OAuth2 provider not configured");
                }
                String token = tokenProvider.getAccessDetails().getAccessToken();
                store.connect(host, port, user, token);
            }

            Folder imapFolder = store.getFolder(folder);
            int mode = (markRead || deleteMessages) ? Folder.READ_WRITE : Folder.READ_ONLY;
            imapFolder.open(mode);

            storeRef.set(store);
            folderRef.set(imapFolder);

            logger.info("Connected to {}:{} folder {}", host, port, folder);
            return true;

        } catch (MessagingException e) {
            logger.error("Connection failed", e);
            disconnect();
            return false;
        }
    }

    public synchronized void disconnect() {
        logger.info("Disconnecting IMAP connection");

        Folder f = folderRef.getAndSet(null);
        Store s = storeRef.getAndSet(null);

        if (s != null && s instanceof com.sun.mail.imap.IMAPStore) {
            try {
                com.sun.mail.imap.IMAPStore imapStore = (com.sun.mail.imap.IMAPStore) s;
                imapStore.close();
                logger.debug("Closed IMAPStore");
            } catch (Exception e) {
                logger.debug("Error closing IMAPStore", e);
            }
        }

        if (f != null) {
            try {
                if (f.isOpen()) {
                    f.close(false);
                }
            } catch (Exception e) {
                logger.debug("Error closing folder", e);
            }
        }

        if (s != null) {
            try {
                if (s.isConnected()) {
                    s.close();
                }
            } catch (Exception e) {
                logger.debug("Error closing store", e);
            }
        }

        logger.info("Disconnect completed");
    }

    public synchronized boolean ensureConnected(String host, int port, String user, String password,
                                                String authMode, String folder,
                                                boolean useSsl, boolean useTls,
                                                boolean markRead, boolean deleteMessages,
                                                boolean partialFetch, long fetchBufferSize,
                                                long connectionTimeoutMs, long reconnectIntervalMs,
                                                OAuth2AccessTokenProvider tokenProvider) {

        Store store = storeRef.get();
        Folder imapFolder = folderRef.get();

        if (store != null && store.isConnected() && imapFolder != null && imapFolder.isOpen()) {
            try {
                imapFolder.getMessageCount();
                return true;
            } catch (MessagingException e) {
                logger.warn("Existing connection is stale, will reconnect", e);
                disconnect();
            }
        }

        long now = System.currentTimeMillis();
        if (now - lastReconnectAttempt < reconnectIntervalMs) {
            logger.debug("In cooldown period after last reconnect attempt");
            return false;
        }

        lastReconnectAttempt = now;

        return connect(host, port, user, password, authMode, folder,
                useSsl, useTls, markRead, deleteMessages,
                partialFetch, fetchBufferSize, connectionTimeoutMs,
                tokenProvider);
    }

    private Properties getProperties(String host, int port, boolean useSsl, boolean useTls,
                                     boolean partialFetch, long fetchBufferSize,
                                     long connectionTimeoutMs, String authMode) {
        Properties props = new Properties();
        String protocol = useSsl ? "imaps" : "imap";

        props.setProperty("mail.store.protocol", protocol);
        props.setProperty("mail." + protocol + ".host", host);
        props.setProperty("mail." + protocol + ".port", String.valueOf(port));
        props.setProperty("mail." + protocol + ".connectiontimeout", String.valueOf(connectionTimeoutMs));
        props.setProperty("mail." + protocol + ".timeout", String.valueOf(connectionTimeoutMs));
        props.setProperty("mail." + protocol + ".writetimeout", String.valueOf(connectionTimeoutMs));
        props.setProperty("mail." + protocol + ".uid", "true");
        props.setProperty("mail." + protocol + ".fetchuid", "true");
        props.setProperty("mail." + protocol + ".partialfetch", String.valueOf(partialFetch));
        props.setProperty("mail." + protocol + ".fetchsize", String.valueOf(fetchBufferSize));
        props.setProperty("mail." + protocol + ".peek", "true");

        if (useSsl) {
            props.setProperty("mail." + protocol + ".ssl.enable", "true");
            props.setProperty("mail." + protocol + ".ssl.checkserveridentity", "true");
            props.setProperty("mail." + protocol + ".starttls.enable", "false");
        } else if (useTls) {
            props.setProperty("mail." + protocol + ".starttls.enable", "true");
            props.setProperty("mail." + protocol + ".starttls.required", "true");
            props.setProperty("mail." + protocol + ".ssl.enable", "false");
        }

        props.setProperty("mail." + protocol + ".auth", "true");

        if (AUTH_OAUTH2.equals(authMode)) {
            props.setProperty("mail." + protocol + ".sasl.enable", "true");
            props.setProperty("mail." + protocol + ".sasl.mechanisms", "XOAUTH2");
            props.setProperty("mail." + protocol + ".auth.login.disable", "true");
            props.setProperty("mail." + protocol + ".auth.plain.disable", "true");
            props.setProperty("mail." + protocol + ".auth.xoauth2.disable", "false");
            props.setProperty("mail." + protocol + ".sasl.xoauth2.force", "true");
        } else {
            props.setProperty("mail." + protocol + ".auth.mechanisms", "LOGIN PLAIN");
        }

        props.setProperty("mail." + protocol + ".connectionpoolsize", "5");
        props.setProperty("mail." + protocol + ".connectionpooltimeout", String.valueOf(connectionTimeoutMs));

        return props;
    }

    public Folder getFolder() { return folderRef.get(); }
    public Store getStore() { return storeRef.get(); }
}