package org.custom.processors;

import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.mail.*;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CustomIMAPTest {

    private TestRunner testRunner;
    private CustomIMAP processor;

    @TempDir
    Path tempDir;

    private static final String SIMPLE_EMAIL_FILE = "simple_email.txt";
    private static final String EMAIL_WITH_ATTACHMENT_FILE = "email_with_attachment.txt";
    private static final String MULTIPART_EMAIL_FILE = "multipart_email.txt";

    @BeforeEach
    public void setUp() throws Exception {
        processor = new CustomIMAP();
        testRunner = TestRunners.newTestRunner(processor);
        createTestEmailFiles();
    }

    //Creates test email files if they don't exist
    private void createTestEmailFiles() throws IOException {
        Path resourceDir = Path.of("src/test/resources");
        if (!Files.exists(resourceDir)) {
            Files.createDirectories(resourceDir);
        }

        Path simpleEmailPath = resourceDir.resolve(SIMPLE_EMAIL_FILE);
        if (!Files.exists(simpleEmailPath)) {
            String simpleEmail = "From: sender@example.com\n" +
                    "To: recipient@example.com\n" +
                    "Subject: Test Meeting\n" +
                    "Date: Mon, 15 Jan 2024 10:00:00 +0000\n" +
                    "Message-ID: <12345@example.com>\n" +
                    "\n" +
                    "Dear team,\n" +
                    "\n" +
                    "This is a test meeting invitation for tomorrow at 2 PM.\n" +
                    "\n" +
                    "Best regards,\n" +
                    "John";
            Files.writeString(simpleEmailPath, simpleEmail);
        }

        Path attachmentEmailPath = resourceDir.resolve(EMAIL_WITH_ATTACHMENT_FILE);
        if (!Files.exists(attachmentEmailPath)) {
            String attachmentEmail = "From: support@company.com\n" +
                    "To: user@example.com\n" +
                    "Subject: Invoice attached\n" +
                    "Date: Tue, 16 Jan 2024 14:30:00 +0000\n" +
                    "Message-ID: <67890@company.com>\n" +
                    "MIME-Version: 1.0\n" +
                    "Content-Type: multipart/mixed; boundary=\"boundary123\"\n" +
                    "\n" +
                    "--boundary123\n" +
                    "Content-Type: text/plain; charset=UTF-8\n" +
                    "\n" +
                    "Dear customer,\n" +
                    "\n" +
                    "Please find your invoice attached.\n" +
                    "\n" +
                    "--boundary123\n" +
                    "Content-Type: text/plain; charset=UTF-8\n" +
                    "Content-Disposition: attachment; filename=\"invoice.txt\"\n" +
                    "\n" +
                    "Invoice #INV-2024-001\n" +
                    "Amount: $100.00\n" +
                    "Due date: 2024-02-15\n" +
                    "\n" +
                    "--boundary123--";
            Files.writeString(attachmentEmailPath, attachmentEmail);
        }

        Path multipartEmailPath = resourceDir.resolve(MULTIPART_EMAIL_FILE);
        if (!Files.exists(multipartEmailPath)) {
            String multipartEmail = "From: newsletter@example.com\n" +
                    "To: subscriber@example.com\n" +
                    "Subject: Monthly Newsletter - January 2024\n" +
                    "Date: Wed, 17 Jan 2024 09:00:00 +0000\n" +
                    "Message-ID: <news-2024-01@example.com>\n" +
                    "MIME-Version: 1.0\n" +
                    "Content-Type: multipart/alternative; boundary=\"boundary456\"\n" +
                    "\n" +
                    "--boundary456\n" +
                    "Content-Type: text/plain; charset=UTF-8\n" +
                    "\n" +
                    "This is the plain text version of the newsletter.\n" +
                    "- News item 1\n" +
                    "- News item 2\n" +
                    "\n" +
                    "--boundary456\n" +
                    "Content-Type: text/html; charset=UTF-8\n" +
                    "\n" +
                    "<html>\n" +
                    "<body>\n" +
                    "<h1>Monthly Newsletter - January 2024</h1>\n" +
                    "<ul>\n" +
                    "<li>News item 1</li>\n" +
                    "<li>News item 2</li>\n" +
                    "</ul>\n" +
                    "</body>\n" +
                    "</html>\n" +
                    "\n" +
                    "--boundary456--";
            Files.writeString(multipartEmailPath, multipartEmail);
        }
    }

    private String readEmailFromFile(String filename) throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream(filename);
        if (is == null) {
            Path filePath = Path.of("src/test/resources", filename);
            if (Files.exists(filePath)) {
                return Files.readString(filePath);
            }
            throw new RuntimeException("File not found: " + filename);
        }
        return new String(is.readAllBytes());
    }

    private void setupBaseProperties() {
        testRunner.setProperty(CustomIMAP.HOST, "imap.example.com");
        testRunner.setProperty(CustomIMAP.PORT, "993");
        testRunner.setProperty(CustomIMAP.USERNAME, "test@example.com");
        testRunner.setProperty(CustomIMAP.PASSWORD, "password");
        testRunner.setProperty(CustomIMAP.FOLDER, "INBOX");
        testRunner.setProperty(CustomIMAP.USE_SSL, "true");
        testRunner.setProperty(CustomIMAP.USE_TLS, "false");
        testRunner.setProperty(CustomIMAP.AUTH_MODE, "PASSWORD");
        testRunner.setProperty(CustomIMAP.BATCH_SIZE, "10");
        testRunner.setProperty(CustomIMAP.CONNECTION_TIMEOUT, "30 secs");
        testRunner.setProperty(CustomIMAP.RECONNECT_INTERVAL, "10 secs");
        testRunner.setProperty(CustomIMAP.INCLUDE_ATTACHMENTS, "false");
        testRunner.setProperty(CustomIMAP.INCLUDE_HEADERS, "true");
        testRunner.setProperty(CustomIMAP.MARK_READ, "false");
        testRunner.setProperty(CustomIMAP.DELETE_MESSAGES, "false");
        testRunner.setProperty(CustomIMAP.FETCH_MODE, "FULL_MESSAGE");
        testRunner.setProperty(CustomIMAP.PARTIAL_FETCH, "true");
        testRunner.setProperty(CustomIMAP.FETCH_BUFFER_SIZE, "100 KB");
        testRunner.setProperty(CustomIMAP.MAX_MESSAGE_SIZE, "10 MB");
        testRunner.setProperty(CustomIMAP.MAX_RETRIES, "3");
        testRunner.setProperty(CustomIMAP.RETRY_BACKOFF, "10 secs");
        testRunner.setProperty(CustomIMAP.INCREMENTAL_FETCH, "false");
    }

    // Validation tests
    @Test
    public void testValidation() {
        setupBaseProperties();
        testRunner.assertValid();

        testRunner.setProperty(CustomIMAP.PORT, "99999");
        testRunner.assertNotValid();
        testRunner.setProperty(CustomIMAP.PORT, "993");

        testRunner.setProperty(CustomIMAP.USE_SSL, "true");
        testRunner.setProperty(CustomIMAP.USE_TLS, "true");
        testRunner.assertNotValid();
        testRunner.setProperty(CustomIMAP.USE_TLS, "false");
    }

    @Test
    public void testAttachmentsValidation() {
        testRunner.setValidateExpressionUsage(false);
        testRunner = TestRunners.newTestRunner(CustomIMAP.class);

        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCLUDE_ATTACHMENTS, "true");
        testRunner.setProperty(CustomIMAP.MAX_ATTACHMENT_SIZE, "5 MB");
        testRunner.setProperty(CustomIMAP.MAX_ATTACHMENTS_PER_MESSAGE, "10");
        testRunner.assertValid();

        testRunner.setProperty(CustomIMAP.MAX_MESSAGE_SIZE, "1 MB");
        testRunner.assertNotValid();
    }

    @Test
    public void testOAuth2Configuration() throws InitializationException {
        OAuth2AccessTokenProvider tokenProvider = mock(OAuth2AccessTokenProvider.class);
        AccessToken mockAccessToken = mock(AccessToken.class);

        when(tokenProvider.getIdentifier()).thenReturn("token-provider");
        when(mockAccessToken.getAccessToken()).thenReturn("mock-access-token");
        when(tokenProvider.getAccessDetails()).thenReturn(mockAccessToken);

        testRunner.addControllerService("token-provider", tokenProvider);
        testRunner.enableControllerService(tokenProvider);

        testRunner.setProperty(CustomIMAP.AUTH_MODE, "OAUTH2");
        testRunner.setProperty(CustomIMAP.OAUTH2_TOKEN_PROVIDER, "token-provider");
        testRunner.removeProperty(CustomIMAP.PASSWORD);

        testRunner.setProperty(CustomIMAP.HOST, "imap.example.com");
        testRunner.setProperty(CustomIMAP.USERNAME, "test@example.com");
        testRunner.setProperty(CustomIMAP.FOLDER, "INBOX");
        testRunner.setProperty(CustomIMAP.FETCH_BUFFER_SIZE, "100 KB");

        testRunner.assertValid();
    }

    @Test
    public void testIMAPFilterParser() {
        assertNotNull(IMAPFilterParser.parse("FROM \"user@example.com\"", 0, 10));
        assertNotNull(IMAPFilterParser.parse("SUBJECT \"Meeting\"", 0, 10));
        assertNotNull(IMAPFilterParser.parse("SINCE 01-Jan-2024", 0, 10));
        assertThrows(IllegalArgumentException.class,
                () -> IMAPFilterParser.parse("BODY \"text\"", 0, 10));
    }

    @Test
    public void testCanReadSimpleEmailFile() throws Exception {
        String content = readEmailFromFile(SIMPLE_EMAIL_FILE);
        assertNotNull(content);
        assertTrue(content.contains("From: sender@example.com"));
        assertTrue(content.contains("Subject: Test Meeting"));
    }

    @Test
    public void testCanReadEmailWithAttachmentFile() throws Exception {
        String content = readEmailFromFile(EMAIL_WITH_ATTACHMENT_FILE);
        assertNotNull(content);
        assertTrue(content.contains("Content-Disposition: attachment"));
        assertTrue(content.contains("filename=\"invoice.txt\""));
    }

    @Test
    public void testCanReadMultipartEmailFile() throws Exception {
        String content = readEmailFromFile(MULTIPART_EMAIL_FILE);
        assertNotNull(content);
        assertTrue(content.contains("Content-Type: multipart/alternative"));
    }

    @Test
    public void testParseSimpleEmail() throws Exception {
        String content = readEmailFromFile(SIMPLE_EMAIL_FILE);
        Properties props = new Properties();
        Session session = Session.getInstance(props);
        MimeMessage message = new MimeMessage(session, new ByteArrayInputStream(content.getBytes()));

        assertEquals("Test Meeting", message.getSubject());
        assertNotNull(message.getContent());
    }

    @Test
    public void testParseEmailWithAttachment() throws Exception {
        String content = readEmailFromFile(EMAIL_WITH_ATTACHMENT_FILE);
        Properties props = new Properties();
        Session session = Session.getInstance(props);
        MimeMessage message = new MimeMessage(session, new ByteArrayInputStream(content.getBytes()));

        assertEquals("Invoice attached", message.getSubject());
        assertTrue(message.getContentType().contains("multipart/mixed"));

        Object msgContent = message.getContent();
        assertTrue(msgContent instanceof Multipart);

        Multipart multipart = (Multipart) msgContent;
        assertEquals(2, multipart.getCount());

        BodyPart attachment = multipart.getBodyPart(1);
        assertEquals("invoice.txt", attachment.getFileName());
        assertTrue(Part.ATTACHMENT.equalsIgnoreCase(attachment.getDisposition()));
    }

    @Test
    public void testParseMultipartEmail() throws Exception {
        String content = readEmailFromFile(MULTIPART_EMAIL_FILE);
        Properties props = new Properties();
        Session session = Session.getInstance(props);
        MimeMessage message = new MimeMessage(session, new ByteArrayInputStream(content.getBytes()));

        assertEquals("Monthly Newsletter - January 2024", message.getSubject());
        assertTrue(message.getContentType().contains("multipart/alternative"));

        Object msgContent = message.getContent();
        assertTrue(msgContent instanceof Multipart);

        Multipart multipart = (Multipart) msgContent;
        assertEquals(2, multipart.getCount());
    }

    @Test
    public void testAddEmailAttributes() throws Exception {
        String content = readEmailFromFile(SIMPLE_EMAIL_FILE);
        Properties props = new Properties();
        Session session = Session.getInstance(props);
        MimeMessage message = new MimeMessage(session, new ByteArrayInputStream(content.getBytes()));

        MockProcessSession processSession = (MockProcessSession) testRunner.getProcessSessionFactory().createSession();
        MockFlowFile flowFile = processSession.create();

        CustomIMAPMessageProcessor msgProcessor = new CustomIMAPMessageProcessor(testRunner.getLogger());
        flowFile = (MockFlowFile) msgProcessor.addEmailAttributes(flowFile, message, processSession, "test-batch-123");

        assertNotNull(flowFile.getAttribute("email.subject"));
        assertEquals("Test Meeting", flowFile.getAttribute("email.subject"));
        assertNotNull(flowFile.getAttribute("email.from"));
        assertNotNull(flowFile.getAttribute("email.batch.id"));
        assertEquals("test-batch-123", flowFile.getAttribute("email.batch.id"));
    }

    @Test
    public void testProcessSingleMessageStreaming() throws Exception {
        String content = readEmailFromFile(SIMPLE_EMAIL_FILE);
        Properties props = new Properties();
        Session session = Session.getInstance(props);
        MimeMessage message = new MimeMessage(session, new ByteArrayInputStream(content.getBytes()));

        MockProcessSession processSession = (MockProcessSession) testRunner.getProcessSessionFactory().createSession();
        CustomIMAPMessageProcessor msgProcessor = new CustomIMAPMessageProcessor(testRunner.getLogger());

        boolean result = msgProcessor.processSingleMessageStreaming(
                message, "FULL_MESSAGE", true, false,
                1024 * 1024, 10,
                processSession, "test-batch-123",
                12345, "INBOX"
        );

        assertTrue(result);
    }

    @Test
    public void testMissingRequiredProperties() {
        testRunner.removeProperty(CustomIMAP.HOST);
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.HOST, "imap.example.com");
        testRunner.removeProperty(CustomIMAP.USERNAME);
        testRunner.assertNotValid();
    }

    @Test
    public void testMissingPasswordWhenAuthModePassword() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.AUTH_MODE, "PASSWORD");
        testRunner.removeProperty(CustomIMAP.PASSWORD);
        testRunner.assertNotValid();
    }

    @Test
    public void testPasswordSetWhenAuthModeOAuth2() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.AUTH_MODE, "OAUTH2");
        testRunner.setProperty(CustomIMAP.PASSWORD, "password");
        testRunner.assertNotValid();
    }

    @Test
    public void testInvalidRecursionDepth() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.MAX_RECURSION_DEPTH, "0");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.MAX_RECURSION_DEPTH, "-2");
        testRunner.assertNotValid();
    }

    @Test
    public void testInvalidBatchSize() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.BATCH_SIZE, "0");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.BATCH_SIZE, "-5");
        testRunner.assertNotValid();
    }

    @Test
    public void testMaxMessageSizeZero() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.MAX_MESSAGE_SIZE, "0 MB");
        testRunner.assertNotValid();
    }

    @Test
    public void testMaxMessageSizeNegative() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.MAX_MESSAGE_SIZE, "-1 MB");
        testRunner.assertNotValid();
    }

    @Test
    public void testMaxAttachmentSizeZero() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCLUDE_ATTACHMENTS, "true");
        testRunner.setProperty(CustomIMAP.MAX_ATTACHMENT_SIZE, "0 MB");
        testRunner.assertNotValid();
    }

    @Test
    public void testMaxAttachmentsPerMessageValidation() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCLUDE_ATTACHMENTS, "true");
        testRunner.setProperty(CustomIMAP.MAX_MESSAGE_SIZE, "50 MB");
        testRunner.setProperty(CustomIMAP.MAX_ATTACHMENT_SIZE, "10 MB");

        testRunner.setProperty(CustomIMAP.MAX_ATTACHMENTS_PER_MESSAGE, "0");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.MAX_ATTACHMENTS_PER_MESSAGE, "-5");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.MAX_ATTACHMENTS_PER_MESSAGE, "10");
        testRunner.assertValid();
    }

    @Test
    public void testInvalidTimePeriods() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.CONNECTION_TIMEOUT, "0 secs");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.RECONNECT_INTERVAL, "-10 secs");
        testRunner.assertNotValid();
    }

    @Test
    public void testRetryBackoffValidation() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.RETRY_BACKOFF, "0 secs");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.RETRY_BACKOFF, "-10 secs");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.RETRY_BACKOFF, "10 secs");
        testRunner.assertValid();
    }

    @Test
    public void testMaxRetriesValidation() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.MAX_RETRIES, "-1");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.MAX_RETRIES, "0");
        testRunner.assertNotValid();

        testRunner.setProperty(CustomIMAP.MAX_RETRIES, "3");
        testRunner.assertValid();
    }

    @Test
    public void testPortWithSSLDefault() {
        testRunner.setProperty(CustomIMAP.HOST, "imap.example.com");
        testRunner.setProperty(CustomIMAP.USERNAME, "test@example.com");
        testRunner.setProperty(CustomIMAP.PASSWORD, "password");
        testRunner.setProperty(CustomIMAP.FOLDER, "INBOX");
        testRunner.setProperty(CustomIMAP.AUTH_MODE, "PASSWORD");
        testRunner.setProperty(CustomIMAP.FETCH_BUFFER_SIZE, "100 KB");

        testRunner.setProperty(CustomIMAP.USE_SSL, "true");
        testRunner.setProperty(CustomIMAP.USE_TLS, "false");
        testRunner.removeProperty(CustomIMAP.PORT);
        testRunner.assertValid();
    }

    @Test
    public void testPortWithTLSDefault() {
        testRunner.setProperty(CustomIMAP.HOST, "imap.example.com");
        testRunner.setProperty(CustomIMAP.USERNAME, "test@example.com");
        testRunner.setProperty(CustomIMAP.PASSWORD, "password");
        testRunner.setProperty(CustomIMAP.FOLDER, "INBOX");
        testRunner.setProperty(CustomIMAP.AUTH_MODE, "PASSWORD");
        testRunner.setProperty(CustomIMAP.FETCH_BUFFER_SIZE, "100 KB");

        testRunner.setProperty(CustomIMAP.USE_SSL, "false");
        testRunner.setProperty(CustomIMAP.USE_TLS, "true");
        testRunner.removeProperty(CustomIMAP.PORT);
        testRunner.assertValid();
    }

    @Test
    public void testFetchModeAttachmentsOnlyWithoutIncludeAttachments() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.FETCH_MODE, "ATTACHMENTS_ONLY");
        testRunner.setProperty(CustomIMAP.INCLUDE_ATTACHMENTS, "false");
        testRunner.assertNotValid();
    }

    @Test
    public void testFetchModeFullMessage() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.FETCH_MODE, "FULL_MESSAGE");
        testRunner.assertValid();
    }

    @Test
    public void testFetchModeHeadersOnly() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.FETCH_MODE, "HEADERS_ONLY");
        testRunner.assertValid();
    }

    @Test
    public void testPartialFetchDisabledNoBufferSize() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.PARTIAL_FETCH, "false");
        testRunner.removeProperty(CustomIMAP.FETCH_BUFFER_SIZE);
        testRunner.assertValid();
    }

    @Test
    public void testPartialFetchEnabledRequiresBufferSize() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.PARTIAL_FETCH, "true");
        testRunner.setProperty(CustomIMAP.FETCH_BUFFER_SIZE, "");
        testRunner.assertNotValid();
    }

    @Test
    public void testIncrementalFetchWithoutUIDFilter() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCREMENTAL_FETCH, "true");
        testRunner.setProperty(CustomIMAP.IMAP_FILTER, "FROM \"test@example.com\"");
        testRunner.assertValid();
    }

    @Test
    public void testIncrementalFetchWithEmptyFilter() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCREMENTAL_FETCH, "true");
        testRunner.removeProperty(CustomIMAP.IMAP_FILTER);
        testRunner.assertValid();
    }

    @Test
    public void testIncrementalFetchWithUIDFilter() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCREMENTAL_FETCH, "true");
        testRunner.setProperty(CustomIMAP.IMAP_FILTER, "UID 1:*");
        testRunner.assertValid();
    }

    @Test
    public void testIncrementalFetchWithLastUIDVariable() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCREMENTAL_FETCH, "true");
        testRunner.setProperty(CustomIMAP.IMAP_FILTER, "UID ${lastUID}:* FROM \"test@example.com\"");
        testRunner.assertValid();
    }

    @Test
    public void testStateFolderDefault() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCREMENTAL_FETCH, "true");
        testRunner.setProperty(CustomIMAP.IMAP_FILTER, "UID 1:*");
        testRunner.assertValid();
    }

    @Test
    public void testStateFolderCustom() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.INCREMENTAL_FETCH, "true");
        testRunner.setProperty(CustomIMAP.IMAP_FILTER, "UID 1:*");
        testRunner.setProperty(CustomIMAP.STATE_FOLDER, "CUSTOMER_123");
        testRunner.assertValid();
    }

    @Test
    public void testIMAPFilterTooLong() {
        setupBaseProperties();
        String longFilter = "A".repeat(5001);
        testRunner.setProperty(CustomIMAP.IMAP_FILTER, longFilter);
        testRunner.assertNotValid();
    }

    @Test
    public void testIMAPFilterInvalidSyntax() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.IMAP_FILTER, "FROM \"unclosed quote");
        testRunner.assertNotValid();
    }

    @Test
    public void testIMAPFilterEmpty() {
        setupBaseProperties();
        testRunner.removeProperty(CustomIMAP.IMAP_FILTER);
        testRunner.assertValid();
    }

    @Test
    public void testRecursiveFolderSearchValid() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.RECURSIVE_FOLDER_SEARCH, "true");
        testRunner.setProperty(CustomIMAP.MAX_RECURSION_DEPTH, "5");
        testRunner.assertValid();
    }

    @Test
    public void testRecursiveFolderSearchInvalidDepth() {
        setupBaseProperties();
        testRunner.setProperty(CustomIMAP.RECURSIVE_FOLDER_SEARCH, "true");
        testRunner.setProperty(CustomIMAP.MAX_RECURSION_DEPTH, "0");
        testRunner.assertNotValid();
    }

    @Test
    public void testDynamicConfigValidate() {
        CustomIMAP.ImapConfig config = processor.new ImapConfig();
        config.host = "imap.example.com";
        config.user = "test@example.com";
        config.folder = "INBOX";
        config.port = 993;
        config.batchSize = 10;
        config.authMode = "PASSWORD";
        config.password = "password";
        config.maxMessageSize = 10 * 1024 * 1024;
        config.reconnectIntervalMs = 10000;
        config.connectionTimeoutMs = 30000;
        config.maxRetries = 3;
        config.retryBackoffMs = 10000;
        config.useSsl = true;
        config.useTls = false;

        MockProcessSession session = (MockProcessSession) testRunner.getProcessSessionFactory().createSession();
        assertTrue(processor.dynamicConfigValidate(config, null, session));
    }

    @Test
    public void testStateKeyGeneration() {
        CustomIMAP.ImapConfig config = processor.new ImapConfig();
        config.stateFolder = "CUSTOMER_123";
        config.folder = "INBOX";

        String stateKey = processor.getStateKeyForFolder(config, "INBOX/Subfolder");
        assertEquals("lastUID_CUSTOMER_123_INBOX_Subfolder", stateKey);
    }
}