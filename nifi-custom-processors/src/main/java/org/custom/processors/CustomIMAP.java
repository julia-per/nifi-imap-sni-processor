/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.custom.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.*;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import javax.mail.*;
import javax.mail.search.SearchTerm;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SupportsBatching
@Tags({"imap", "email", "consume", "fetch", "filter", "incremental", "streaming", "recursive"})
@CapabilityDescription("""
        Streaming IMAP consumer with server-side filtering. Processes messages in small batches without loading all into memory.
        Supports SSL/TLS, OAuth2, and dynamic configuration. Features incremental UID-based fetching combined with IMAP filters.
        Partial streaming from IMAP server with size-limited processing. Includes recursive folder search and retry logic.""")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="email.subject", description="Subject of the email"),
        @WritesAttribute(attribute="email.from", description="Sender of the email"),
        @WritesAttribute(attribute="email.date", description="Date the email was received"),
        @WritesAttribute(attribute="email.size", description="Size of the email in bytes"),
        @WritesAttribute(attribute="email.flags", description="Flags of the email (SEEN, FLAGGED, etc)"),
        @WritesAttribute(attribute="email.uid", description="UID of the message on the server"),
        @WritesAttribute(attribute="email.batch.id", description="Unique ID for the batch this message belongs to"),
        @WritesAttribute(attribute="email.parent.message.id", description="Message-ID of parent email for attachment"),
        @WritesAttribute(attribute="email.folder", description="Folder where the message was found"),
        @WritesAttribute(attribute="attachment.filename", description="Name of extracted attachment"),
        @WritesAttribute(attribute="attachment.size", description="Size of extracted attachment"),
        @WritesAttribute(attribute="attachment.mime.type", description="MIME type of attachment"),
        @WritesAttribute(attribute="error.type", description="Type of error when processing fails"),
        @WritesAttribute(attribute="error.message", description="Error message details"),
        @WritesAttribute(attribute="error.filter", description="The IMAP filter that caused an error"),
        @WritesAttribute(attribute="retry.count", description="Number of retry attempts for this FlowFile")})
@Stateful(description = "Stores the last processed UID per folder to enable incremental fetching", scopes = {Scope.CLUSTER})
public class CustomIMAP extends AbstractProcessor {

    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("imap-host")
            .displayName("Host Name")
            .description("Network address of Email server (e.g., imap.gmail.com, imap.mail.yahoo.com)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNI_HOSTNAME = new PropertyDescriptor.Builder()
            .name("sni-hostname")
            .displayName("SNI Hostname")
            .description("""
                Hostname to send in TLS SNI extension.
                If not specified, the connection hostname will be used.
                Useful when connecting through proxies or load balancers.""")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("imap-port")
            .displayName("Port")
            .description("Numeric value identifying Port of Email server (e.g., 993 for SSL, 143 for STARTTLS)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("993")
            .build();

    public static final AllowableValue AUTH_PASSWORD = new AllowableValue("PASSWORD", "Use Password",
            "Regular authentication with username and password");
    public static final AllowableValue AUTH_OAUTH2 = new AllowableValue("OAUTH2", "Use OAuth2",
            "Modern authentication using OAuth2 access tokens");

    public static final PropertyDescriptor AUTH_MODE = new PropertyDescriptor.Builder()
            .name("authorization-mode")
            .displayName("Authorization Mode")
            .description("How to authenticate with the email server")
            .required(true)
            .allowableValues(AUTH_PASSWORD, AUTH_OAUTH2)
            .defaultValue(AUTH_PASSWORD.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor OAUTH2_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("oauth2-access-token-provider")
            .displayName("OAuth2 Access Token Provider")
            .description("Controller service that provides OAuth2 access tokens (only used with OAuth2 mode)")
            .required(false)
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .dependsOn(AUTH_MODE, AUTH_OAUTH2)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("imap-username")
            .displayName("Username")
            .description("Email account username (usually full email address)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("imap-password")
            .displayName("Password")
            .description("""
                    Password used for authentication and authorization with Email server
                    (only used with Password mode)""")
            .required(false)
            .dependsOn(AUTH_MODE, AUTH_PASSWORD)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("imap-folder")
            .displayName("Folder")
            .description("Folder to read messages from (e.g., INBOX, INBOX/Spam, Archive)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("INBOX")
            .build();

    public static final PropertyDescriptor RECURSIVE_FOLDER_SEARCH = new PropertyDescriptor.Builder()
            .name("recursive-folder-search")
            .displayName("Recursive Folder Search")
            .description("Search for messages in subfolders recursively (this may be slow on large mailboxes)")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_RECURSION_DEPTH = new PropertyDescriptor.Builder()
            .name("max-recursion-depth")
            .displayName("Max Recursion Depth")
            .description("Maximum depth for recursive folder traversal (positive integers only or -1 = unlimited)")
            .required(true)
            .defaultValue("5")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("""
                    Number of messages to process in each batch.
                    Messages are streamed one batch at a time to manage memory.""")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final AllowableValue FETCH_MODE_HEADERS_ONLY = new AllowableValue("HEADERS_ONLY",
            "Headers Only", "Fetch only message headers as attributes, no content (fastest, minimal data)");
    public static final AllowableValue FETCH_MODE_FULL_MESSAGE = new AllowableValue("FULL_MESSAGE",
            "Full Message", "Fetch complete MIME message including body and attachments");
    public static final AllowableValue FETCH_MODE_ATTACHMENTS_ONLY = new AllowableValue("ATTACHMENTS_ONLY",
            "Attachments Only", """
            Fetch only attachments, discard message body
            (The message body is still loaded into memory to extract attachments,
            but attachments are streamed separately.)""");

    public static final PropertyDescriptor FETCH_MODE = new PropertyDescriptor.Builder()
            .name("fetch-mode")
            .displayName("Fetch Mode")
            .description("Determines what content to fetch from the message")
            .required(true)
            .allowableValues(FETCH_MODE_HEADERS_ONLY, FETCH_MODE_FULL_MESSAGE, FETCH_MODE_ATTACHMENTS_ONLY)
            .defaultValue(FETCH_MODE_FULL_MESSAGE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PARTIAL_FETCH = new PropertyDescriptor.Builder()
            .name("partial-fetch")
            .displayName("Partial Fetch")
            .description("If true, fetch message content in chunks to reduce memory usage. Critical for streaming large messages.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FETCH_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("fetch-buffer-size")
            .displayName("Fetch Buffer Size")
            .description("Size of the buffer for fetching message content in bytes. Used with partial fetch for streaming large messages.")
            .required(true)
            .defaultValue("100 KB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(PARTIAL_FETCH, "true")
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connection-timeout")
            .displayName("Connection Timeout")
            .description("Time to wait for connection to the email server (e.g., 30 secs, 1 min)")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RECONNECT_INTERVAL = new PropertyDescriptor.Builder()
            .name("reconnect-interval")
            .displayName("Reconnect Interval")
            .description("Time to wait before reconnecting after a failure (e.g., 10 secs, 1 min)")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MARK_READ = new PropertyDescriptor.Builder()
            .name("mark-messages-as-read")
            .displayName("Mark Messages as Read")
            .description("Whether to mark messages as read after retrieval")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DELETE_MESSAGES = new PropertyDescriptor.Builder()
            .name("delete-messages")
            .displayName("Delete Messages")
            .description("Whether to delete messages from the server after retrieval")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor INCLUDE_HEADERS = new PropertyDescriptor.Builder()
            .name("include-headers")
            .displayName("Include Headers")
            .description("Add message headers (From, To, Subject, Date) as FlowFile attributes")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor INCLUDE_ATTACHMENTS = new PropertyDescriptor.Builder()
            .name("include-attachments")
            .displayName("Include Attachments")
            .description("Extract attachments as separate FlowFiles")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("max-message-size")
            .displayName("Max Message Size")
            .description("Maximum size of a single message in bytes. Messages larger than this will be routed to parse error relationship.")
            .required(true)
            .defaultValue("100 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_ATTACHMENT_SIZE = new PropertyDescriptor.Builder()
            .name("max-attachment-size")
            .displayName("Max Attachment Size")
            .description("Maximum size of a single attachment in bytes. Attachments larger than this will be skipped.")
            .required(true)
            .defaultValue("50 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(INCLUDE_ATTACHMENTS, "true")
            .build();

    public static final PropertyDescriptor MAX_ATTACHMENTS_PER_MESSAGE = new PropertyDescriptor.Builder()
            .name("max-attachments-per-message")
            .displayName("Max Attachments per Message")
            .description("Maximum number of attachments to extract from a single message. Additional attachments are skipped.")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(INCLUDE_ATTACHMENTS, "true")
            .build();

    public static final PropertyDescriptor USE_SSL = new PropertyDescriptor.Builder()
            .name("use-ssl")
            .displayName("Use SSL")
            .description("Use SSL/TLS (IMAPS) connection - typically port 993")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor USE_TLS = new PropertyDescriptor.Builder()
            .name("use-tls")
            .displayName("Use TLS / STARTTLS")
            .description("Use STARTTLS - connection starts as plain text then upgrades to TLS (port 143)")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IMAP_FILTER = new PropertyDescriptor.Builder()
            .name("imap-filter")
            .displayName("IMAP Filter")
            .description("""
                IMAP search filter string. Use the same syntax as your email client.
                For incremental fetching, use UID filter with state variable: UID ${lastUID}:*""")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCREMENTAL_FETCH = new PropertyDescriptor.Builder()
            .name("incremental-fetch")
            .displayName("Incremental Fetch")
            .description("""
                Enable incremental fetching based on UIDs. When enabled, the processor stores the last
                successfully processed UID per folder and only fetches messages with higher UIDs on subsequent polls.
                The UID filter is combined with your IMAP filter using AND if needed.
                """)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor STATE_FOLDER = new PropertyDescriptor.Builder()
            .name("state-folder")
            .displayName("State Folder Identifier")
            .description("Unique identifier for this folder's state (used when processing multiple folders with same processor)")
            .required(false)
            .defaultValue("INBOX")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_RETRIES = new PropertyDescriptor.Builder()
            .name("max-retries")
            .displayName("Maximum Retries")
            .description("Maximum number of retry attempts for failed messages before routing to failure")
            .required(true)
            .defaultValue("3")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RETRY_BACKOFF = new PropertyDescriptor.Builder()
            .name("retry-backoff")
            .displayName("Retry Backoff")
            .description("Base time to wait between retries (exponential backoff: wait * 2^retryCount)")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final String LAST_UID_PREFIX = "lastUID_";
    private static final Pattern UID_PATTERN = Pattern.compile("\\bUID\\b", Pattern.CASE_INSENSITIVE);

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All messages successfully received from Email server are routed here")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Messages that could not be processed due to critical issues are routed here")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original triggering FlowFile (if any) is routed here after processing")
            .build();

    public static final Relationship REL_PARSE_ERROR = new Relationship.Builder()
            .name("parse-error")
            .description("Messages that were received but could not be parsed are routed here")
            .build();

    public static final Relationship REL_ATTACHMENTS = new Relationship.Builder()
            .name("attachments")
            .description("All attachments extracted from email messages are routed here as separate FlowFiles")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("Messages that failed due to temporary issues are routed here for later retry")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private volatile boolean stopped = false;

    final AtomicReference<Store> storeRef = new AtomicReference<>();
    final AtomicReference<Folder> folderRef = new AtomicReference<>();
    private final AtomicReference<ImapConfig> currentConfigRef = new AtomicReference<>();
    private long lastReconnectAttempt = 0;

    CustomIMAPConnection connectionManager;
    CustomIMAPMessageProcessor messageProcessor;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        getLogger().info("Initializing CustomIMAP processor");

        this.connectionManager = new CustomIMAPConnection(getLogger());
        this.messageProcessor = new CustomIMAPMessageProcessor(getLogger());

        descriptors = getSupportedPropertyDescriptors();
        relationships = Set.of(
                REL_SUCCESS,
                REL_FAILURE,
                REL_ORIGINAL,
                REL_PARSE_ERROR,
                REL_ATTACHMENTS,
                REL_RETRY
        );
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOST);
        descriptors.add(SNI_HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(AUTH_MODE);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(OAUTH2_TOKEN_PROVIDER);
        descriptors.add(FOLDER);
        descriptors.add(RECURSIVE_FOLDER_SEARCH);
        descriptors.add(MAX_RECURSION_DEPTH);
        descriptors.add(BATCH_SIZE);
        descriptors.add(FETCH_MODE);
        descriptors.add(PARTIAL_FETCH);
        descriptors.add(FETCH_BUFFER_SIZE);
        descriptors.add(CONNECTION_TIMEOUT);
        descriptors.add(RECONNECT_INTERVAL);
        descriptors.add(MARK_READ);
        descriptors.add(DELETE_MESSAGES);
        descriptors.add(INCLUDE_HEADERS);
        descriptors.add(INCLUDE_ATTACHMENTS);
        descriptors.add(MAX_MESSAGE_SIZE);
        descriptors.add(MAX_ATTACHMENT_SIZE);
        descriptors.add(MAX_ATTACHMENTS_PER_MESSAGE);
        descriptors.add(USE_SSL);
        descriptors.add(USE_TLS);
        descriptors.add(IMAP_FILTER);
        descriptors.add(INCREMENTAL_FETCH);
        descriptors.add(STATE_FOLDER);
        descriptors.add(MAX_RETRIES);
        descriptors.add(RETRY_BACKOFF);
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        Collection<ValidationResult> results = new ArrayList<>();

        validateHostAndSni(context, results);
        validateSecuritySettings(context, results);
        validateAuthSettings(context, results);
        validateSizeSettings(context, results);
        validateFilterSettings(context, results);
        validateRecursionSettings(context, results);
        validateRetrySettings(context, results);
        validateBatchSettings(context, results);
        validateTimeoutSettings(context, results);
        validatePartialFetchSettings(context, results);

        return results;
    }

    private boolean containsUIDCriteria(String filter) {
        if (filter == null) return false;
        Matcher m = UID_PATTERN.matcher(filter);
        return m.find() && !filter.contains("${lastUID}");
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getLogger().info("Processor scheduled, will connect on first trigger");
        stopped = false;
    }

    @OnStopped
    public void onStopped() {
        getLogger().info("Processor stopped, disconnecting...");
        stopped = true;
        connectionManager.disconnect();
    }

    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        if (stopped) {
            getLogger().info("Processor is stopped, exiting");
            return;
        }

        FlowFile originalFlowFile = session.get();
        String batchId = UUID.randomUUID().toString();

        if (handleRetryFlowFile(context, session, originalFlowFile)) {
            return;
        }

        try {
            ImapConfig config = readAndValidateConfig(context, session, originalFlowFile);
            if (config == null) return;

            if (!ensureConnection(context, session, config, originalFlowFile)) {
                return;
            }

            Store store = connectionManager.getStore();
            Folder mainFolder = connectionManager.getFolder();

            List<FolderInfo> foldersToProcess = getFoldersToProcess(store, config);
            if (foldersToProcess.isEmpty()) {
                handleNoFolders(context, session, originalFlowFile);
                return;
            }

            ProcessingResult result = processAllFolders(context, session, config, batchId, foldersToProcess, mainFolder);

            updateState(context, config, result.highestUIDs, result.highestSeqNums);

            getLogger().info("Processed total of {} messages across {} folders",
                    result.totalProcessed, foldersToProcess.size());

            if (!result.hadMessages) {
                getLogger().debug("No messages found, yielding");
                context.yield();
            }

            if (originalFlowFile != null && !stopped) {
                session.transfer(originalFlowFile, REL_ORIGINAL);
            }

        } catch (Exception e) {
            handleException(session, originalFlowFile, e);
            context.yield();
        }
    }

    private boolean handleRetryFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        if (flowFile == null) return false;

        String retryCountStr = flowFile.getAttribute("retry.count");
        if (retryCountStr == null) return false;

        int retryCount = Integer.parseInt(retryCountStr);
        long backoffMs = getTimePeriodValue(context, RETRY_BACKOFF, flowFile, 10000L);
        long waitTime = backoffMs * (long) Math.pow(2, retryCount - 1);

        String lastRetryStr = flowFile.getAttribute("retry.last.time");
        if (lastRetryStr != null) {
            long lastRetry = Long.parseLong(lastRetryStr);
            if (System.currentTimeMillis() - lastRetry < waitTime) {
                session.transfer(flowFile, REL_RETRY);
                context.yield();
                return true;
            }
        }
        return false;
    }

    private ImapConfig readAndValidateConfig(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        ImapConfig config = readConfig(context, flowFile);
        getLogger().debug("Configuration loaded - Host: {}, SNI: {}, Port: {}, User: {}, Folder: {}, Filter: {}, Recursive: {}",
                config.host, config.sniHostname, config.port, config.user, config.folder, config.imapFilter, config.recursive);

        currentConfigRef.set(config);

        if (!dynamicConfigValidate(config, flowFile, session)) {
            return null;
        }

        parseImapFilter(config);

        if (stopped) {
            if (flowFile != null) session.transfer(flowFile, REL_ORIGINAL);
            return null;
        }

        return config;
    }

    private void parseImapFilter(ImapConfig config) {
        if (config.imapFilter == null || config.imapFilter.trim().isEmpty()) return;

        try {
            int depthLimit = config.maxRecursionDepth <= 0 ? Integer.MAX_VALUE : config.maxRecursionDepth;
            config.searchTerm = IMAPFilterParser.parse(config.imapFilter, 0, depthLimit);
            getLogger().debug("Successfully parsed IMAP filter: {}", config.imapFilter);
        } catch (IllegalArgumentException e) {
            config.filterParseError = e.getMessage();
        }
    }

    private boolean ensureConnection(ProcessContext context, ProcessSession session,
                                     ImapConfig config, FlowFile flowFile) {
        OAuth2AccessTokenProvider tokenProvider = context.getProperty(OAUTH2_TOKEN_PROVIDER)
                .asControllerService(OAuth2AccessTokenProvider.class);

        if (!ensureConnected(config, tokenProvider)) {
            handleRetry(session, flowFile, "CONNECTION_ERROR", "Failed to connect to IMAP server", config);
            context.yield();
            return false;
        }

        Store store = connectionManager.getStore();
        if (store == null || !store.isConnected()) {
            handleRetry(session, flowFile, "CONNECTION_ERROR", "Store is not connected", config);
            context.yield();
            return false;
        }

        if (stopped) {
            if (flowFile != null) session.transfer(flowFile, REL_ORIGINAL);
            return false;
        }

        return true;
    }

    private void handleNoFolders(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        getLogger().warn("No folders found to process");
        if (flowFile != null) session.transfer(flowFile, REL_ORIGINAL);
        context.yield();
    }

    private ProcessingResult processAllFolders(ProcessContext context, ProcessSession session,
                                               ImapConfig config, String batchId,
                                               List<FolderInfo> foldersToProcess, Folder mainFolder) throws MessagingException {
        long totalProcessed = 0;
        Map<String, Long> highestUIDs = new HashMap<>();
        Map<String, Long> highestSeqNums = new HashMap<>();
        boolean hadMessages = false;

        for (FolderInfo folderInfo : foldersToProcess) {
            if (stopped) {
                getLogger().info("Processor stopped before folder: {}", folderInfo.path);
                break;
            }

            FolderResult folderResult = processSingleFolder(context, session, config, batchId,
                    folderInfo, mainFolder);

            if (folderResult != null) {
                hadMessages = hadMessages || folderResult.hadMessages;
                totalProcessed += folderResult.processedCount;
                highestUIDs.putAll(folderResult.highestUIDs);
                highestSeqNums.putAll(folderResult.highestSeqNums);
            }
        }

        return new ProcessingResult(totalProcessed, highestUIDs, highestSeqNums, hadMessages);
    }

    private FolderResult processSingleFolder(ProcessContext context, ProcessSession session,
                                             ImapConfig config, String batchId,
                                             FolderInfo folderInfo, Folder mainFolder) throws MessagingException {
        Folder currentFolder = null;
        try {
            currentFolder = folderInfo.folder;

            if (!currentFolder.isOpen()) {
                int folderMode = (config.markRead || config.deleteMessages) ? Folder.READ_WRITE : Folder.READ_ONLY;
                currentFolder.open(folderMode);
            }

            Message[] messages = getFilteredMessages(currentFolder, config);
            if (messages.length == 0) {
                return null;
            }

            FolderUIDInfo uidInfo = checkFolderUIDSupport(currentFolder, config);
            if (uidInfo.canUseUID) {
                FetchProfile fp = new FetchProfile();
                fp.add(UIDFolder.FetchProfileItem.UID);
                fp.add(FetchProfile.Item.ENVELOPE);
                fp.add(FetchProfile.Item.FLAGS);
                currentFolder.fetch(messages, fp);
            }

            long lastId = getLastIdFromState(context, config, folderInfo.path, uidInfo.canUseUID);

            MessageFilterResult filterResult = filterMessagesByLastId(
                    messages, uidInfo, lastId, config.incrementalFetch, folderInfo.path);

            if (filterResult.messagesToProcess.length == 0) {
                return null;
            }

            long folderHighestUID = messageProcessor.processMessageStream(
                    context, session,
                    filterResult.messagesToProcess, uidInfo.uidFolder,
                    config.fetchMode, config.includeHeaders, config.includeAttachments,
                    config.maxMessageSize, config.maxAttachmentSize,
                    config.maxAttachmentsPerMessage,
                    config.markRead, config.deleteMessages,
                    batchId, folderInfo.path,
                    config.batchSize, currentFolder,
                    uidInfo.uidFolder
            );

            Map<String, Long> highestUIDs = new HashMap<>();
            Map<String, Long> highestSeqNums = new HashMap<>();

            if (folderHighestUID > 0) {
                if (uidInfo.canUseUID && uidInfo.uidFolder != null) {
                    highestUIDs.put(folderInfo.path, folderHighestUID);
                } else {
                    highestSeqNums.put(folderInfo.path, folderHighestUID);
                }
            }

            return new FolderResult(true, filterResult.messagesToProcess.length,
                    highestUIDs, highestSeqNums);

        } finally {
            if (currentFolder != null && currentFolder != mainFolder && currentFolder.isOpen()) {
                try {
                    currentFolder.close(false);
                } catch (MessagingException e) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Error closing temporary folder", e);
                    }
                }
            }
        }
    }

    private FolderUIDInfo checkFolderUIDSupport(Folder folder, ImapConfig config) {
        UIDFolder uidFolder = null;
        boolean canUseUID = false;

        if (folder instanceof UIDFolder) {
            uidFolder = (UIDFolder) folder;
            canUseUID = true;
        }

        return new FolderUIDInfo(uidFolder, canUseUID);
    }

    private long getLastIdFromState(ProcessContext context, ImapConfig config,
                                    String folderPath, boolean canUseUID) {
        if (!config.incrementalFetch) return -1;

        try {
            StateManager stateManager = context.getStateManager();
            StateMap stateMap = stateManager.getState(Scope.CLUSTER);

            if (canUseUID) {
                String stateKey = getStateKeyForFolder(config, folderPath);
                String lastUIDStr = stateMap.get(stateKey);
                if (lastUIDStr != null) {
                    try {
                        long lastId = Long.parseLong(lastUIDStr);
                        getLogger().debug("Folder {} - Last processed UID: {}", folderPath, lastId);
                        return lastId;
                    } catch (NumberFormatException e) {
                        getLogger().warn("Folder {} - Invalid last UID", folderPath);
                    }
                }
            } else {
                String seqKey = getStateKeyForFolder(config, folderPath) + "_SEQ";
                String lastSeqStr = stateMap.get(seqKey);
                if (lastSeqStr != null) {
                    try {
                        long lastId = Long.parseLong(lastSeqStr);
                        getLogger().debug("Folder {} - Last processed message number: {}", folderPath, lastId);
                        return lastId;
                    } catch (NumberFormatException e) {
                        getLogger().warn("Folder {} - Invalid last sequence number", folderPath);
                    }
                }
            }
        } catch (IOException e) {
            getLogger().warn("Failed to get state for folder {}", folderPath, e);
        }

        return -1;
    }

    private Message[] getFilteredMessages(Folder folder, ImapConfig config) throws MessagingException {
        Message[] messages;
        if (config.searchTerm != null) {
            messages = folder.search(config.searchTerm);
            getLogger().debug("Folder {} - Filter returned {} messages", folder.getFullName(), messages.length);
        } else {
            messages = folder.getMessages();
            getLogger().debug("Folder {} - Processing all {} messages", folder.getFullName(), messages.length);
        }
        return messages;
    }

    private void fetchUIDs(Folder folder, Message[] messages) {
        try {
            FetchProfile fp = new FetchProfile();
            fp.add(UIDFolder.FetchProfileItem.UID);
            fp.add(FetchProfile.Item.ENVELOPE);
            fp.add(FetchProfile.Item.FLAGS);
            folder.fetch(messages, fp);
            getLogger().debug("Fetched UIDs for {} messages in folder {}",
                    messages.length, folder.getFullName());
        } catch (MessagingException e) {
            getLogger().warn("Failed to fetch UIDs: {}", e.getMessage());
        }
    }

    private MessageFilterResult filterMessagesByLastId(Message[] messages, FolderUIDInfo uidInfo,
                                                       long lastId, boolean incrementalFetch, String folderPath) {
        if (!incrementalFetch || lastId < 0) {
            return new MessageFilterResult(messages, -1);
        }

        List<Message> filteredList = new ArrayList<>();
        long highestInBatch = -1;

        for (Message msg : messages) {
            if (stopped) break;

            long currentId;
            String idSource;

            if (uidInfo.canUseUID && uidInfo.uidFolder != null) {
                try {
                    currentId = uidInfo.uidFolder.getUID(msg);
                    if (currentId > 0) {
                        idSource = "UID";
                    } else {
                        currentId = msg.getMessageNumber();
                        idSource = "UID failed → MsgNum";
                    }
                } catch (MessagingException e) {
                    currentId = msg.getMessageNumber();
                    idSource = "UID error → MsgNum";
                }
            } else {
                currentId = msg.getMessageNumber();
                idSource = "MsgNum";
            }

            getLogger().debug("Folder {} - Message {}: {} = {}", folderPath,
                    msg.getMessageNumber(), idSource, currentId);

            if (currentId > lastId) {
                filteredList.add(msg);
                if (currentId > highestInBatch) {
                    highestInBatch = currentId;
                }
            }
        }

        Message[] result = filteredList.toArray(new Message[0]);
        getLogger().debug("Folder {} - After filter: {} messages, highest in batch: {}",
                folderPath, result.length, highestInBatch);

        return new MessageFilterResult(result, highestInBatch);
    }

    private void updateState(ProcessContext context, ImapConfig config,
                             Map<String, Long> highestUIDs, Map<String, Long> highestSeqNums) {
        if (!config.incrementalFetch || (highestUIDs.isEmpty() && highestSeqNums.isEmpty()) || stopped) {
            return;
        }

        try {
            StateManager stateManager = context.getStateManager();
            StateMap currentState = stateManager.getState(Scope.CLUSTER);
            Map<String, String> updatedState = new HashMap<>(currentState.toMap());

            for (Map.Entry<String, Long> entry : highestUIDs.entrySet()) {
                String stateKey = getStateKeyForFolder(config, entry.getKey());
                Long currentUID = entry.getValue();
                String existingUIDStr = updatedState.get(stateKey);
                if (existingUIDStr == null || currentUID > Long.parseLong(existingUIDStr)) {
                    updatedState.put(stateKey, String.valueOf(currentUID));
                    getLogger().debug("Updated UID state for folder {} with last UID: {}",
                            entry.getKey(), currentUID);
                }
            }

            for (Map.Entry<String, Long> entry : highestSeqNums.entrySet()) {
                String seqKey = getStateKeyForFolder(config, entry.getKey()) + "_SEQ";
                Long currentSeq = entry.getValue();
                String existingSeqStr = updatedState.get(seqKey);
                if (existingSeqStr == null || currentSeq > Long.parseLong(existingSeqStr)) {
                    updatedState.put(seqKey, String.valueOf(currentSeq));
                    getLogger().debug("Updated SEQ state for folder {} with last message number: {}",
                            entry.getKey(), currentSeq);
                }
            }

            stateManager.setState(updatedState, Scope.CLUSTER);
        } catch (IOException e) {
            getLogger().error("Failed to update state", e);
        }
    }

    private boolean ensureConnected(ImapConfig config, OAuth2AccessTokenProvider tokenProvider) {
        if (stopped) return false;
        return connectionManager.ensureConnected(
                config.host, config.sniHostname, config.port, config.user, config.password,
                config.authMode, config.folder, config.useSsl, config.useTls,
                config.markRead, config.deleteMessages, config.partialFetch,
                config.fetchBufferSize, config.connectionTimeoutMs,
                config.reconnectIntervalMs, tokenProvider);
    }

    private List<FolderInfo> getFoldersToProcess(Store store, ImapConfig config) throws MessagingException {
        List<FolderInfo> folders = new ArrayList<>();
        if (!config.recursive) {
            Folder folder = store.getFolder(config.folder);
            folders.add(new FolderInfo(folder, config.folder, 0));
        } else {
            int maxDepth = config.maxRecursionDepth < 0 ? Integer.MAX_VALUE : config.maxRecursionDepth;
            traverseFolders(store.getDefaultFolder(), config.folder, 0, maxDepth, folders);
        }
        return folders;
    }

    private void traverseFolders(Folder parent, String targetPath, int currentDepth, int maxDepth,
                                 List<FolderInfo> folders) throws MessagingException {
        if (currentDepth > maxDepth) return;

        if (parent.getFullName().equals(targetPath) ||
                (targetPath.endsWith("*") && parent.getFullName().startsWith(targetPath.substring(0, targetPath.length() - 1)))) {
            folders.add(new FolderInfo(parent, parent.getFullName(), currentDepth));
        }

        if (parent.getType() != Folder.HOLDS_FOLDERS) return;

        Folder[] subfolders = parent.list();
        for (Folder subfolder : subfolders) {
            traverseFolders(subfolder, targetPath, currentDepth + 1, maxDepth, folders);
        }
    }

    private void handleError(ProcessSession session, FlowFile flowFile, String errorType,
                             String errorMessage, Relationship destination) {
        if (flowFile != null) {
            flowFile = session.putAttribute(flowFile, "error.type", errorType);
            flowFile = session.putAttribute(flowFile, "error.message", errorMessage);
            session.transfer(flowFile, destination);
        }
    }

    private void handleRetry(ProcessSession session, FlowFile flowFile, String errorType,
                             String errorMessage, ImapConfig config) {
        if (flowFile != null) {
            int maxRetries = config.maxRetries;
            String retryCountStr = flowFile.getAttribute("retry.count");
            int retryCount = retryCountStr != null ? Integer.parseInt(retryCountStr) : 0;

            if (retryCount >= maxRetries) {
                getLogger().warn("FlowFile exceeded max retries ({}), routing to failure", maxRetries);
                flowFile = session.putAttribute(flowFile, "error.type", errorType);
                flowFile = session.putAttribute(flowFile, "error.message", errorMessage + " - Max retries exceeded");
                session.transfer(flowFile, REL_FAILURE);
            } else {
                flowFile = session.putAttribute(flowFile, "retry.count", String.valueOf(retryCount + 1));
                flowFile = session.putAttribute(flowFile, "retry.last.time", String.valueOf(System.currentTimeMillis()));
                flowFile = session.putAttribute(flowFile, "error.type", errorType);
                flowFile = session.putAttribute(flowFile, "error.message", errorMessage);
                session.transfer(flowFile, REL_RETRY);

                long backoffMs = config.retryBackoffMs * (long) Math.pow(2, retryCount);
                getLogger().debug("Scheduled retry #{} for FlowFile, backoff: {} ms", retryCount + 1, backoffMs);
            }
        }
    }

    private void handleException(ProcessSession session, FlowFile flowFile, Exception e) {
        if (e instanceof MessagingException) {
            getLogger().error("IMAP connection error", e);
            handleRetry(session, flowFile, "CONNECTION_ERROR", e.getMessage(),
                    currentConfigRef.get() != null ? currentConfigRef.get() : new ImapConfig());
        } else {
            getLogger().error("Unexpected error", e);
            handleError(session, flowFile, "UNEXPECTED_ERROR", e.getMessage(), REL_FAILURE);
        }
    }

    String getStateKeyForFolder(ImapConfig config, String folderPath) {
        String baseKey = config.stateFolder != null ? config.stateFolder : config.folder;
        return LAST_UID_PREFIX + baseKey + "_" + folderPath.replace('/', '_');
    }

    private ImapConfig readConfig(ProcessContext context, FlowFile flowFile) {
        ImapConfig config = new ImapConfig();

        config.host = getStringValue(context, HOST, flowFile);
        config.sniHostname = getStringValue(context, SNI_HOSTNAME, flowFile);
        config.user = getStringValue(context, USERNAME, flowFile);
        config.folder = getStringValue(context, FOLDER, flowFile);
        config.authMode = getStringValue(context, AUTH_MODE, flowFile);
        config.fetchMode = getStringValue(context, FETCH_MODE, flowFile);
        config.useSsl = getBooleanValue(context, USE_SSL, flowFile);
        config.useTls = getBooleanValue(context, USE_TLS, flowFile);
        config.recursive = getBooleanValue(context, RECURSIVE_FOLDER_SEARCH, flowFile);
        config.markRead = getBooleanValue(context, MARK_READ, flowFile);
        config.deleteMessages = getBooleanValue(context, DELETE_MESSAGES, flowFile);
        config.includeHeaders = getBooleanValue(context, INCLUDE_HEADERS, flowFile);
        config.includeAttachments = getBooleanValue(context, INCLUDE_ATTACHMENTS, flowFile);
        config.partialFetch = getBooleanValue(context, PARTIAL_FETCH, flowFile);
        config.incrementalFetch = getBooleanValue(context, INCREMENTAL_FETCH, flowFile);

        if (AUTH_PASSWORD.getValue().equals(config.authMode)) {
            config.password = getStringValue(context, PASSWORD, flowFile);
        } else {
            config.password = null;
        }

        config.imapFilter = getStringValue(context, IMAP_FILTER, flowFile);
        config.stateFolder = getStringValue(context, STATE_FOLDER, flowFile);

        String portStr = getStringValue(context, PORT, flowFile);
        if (portStr == null || portStr.trim().isEmpty()) {
            if (config.useSsl) config.port = 993;
            else if (config.useTls) config.port = 143;
            else config.port = 143;
        } else {
            try {
                config.port = Integer.parseInt(portStr.trim());
            } catch (NumberFormatException e) {
                config.port = config.useSsl ? 993 : 143;
                getLogger().warn("Invalid port format '{}', using default: {}", portStr, config.port);
            }
        }

        config.batchSize = getIntValue(context, BATCH_SIZE, flowFile, 10);
        config.maxMessageSize = getDataSizeValue(context, MAX_MESSAGE_SIZE, flowFile, 100L * 1024 * 1024);
        config.maxRecursionDepth = getIntValue(context, MAX_RECURSION_DEPTH, flowFile, 5);
        config.reconnectIntervalMs = getTimePeriodValue(context, RECONNECT_INTERVAL, flowFile, 10000L);
        config.connectionTimeoutMs = getTimePeriodValue(context, CONNECTION_TIMEOUT, flowFile, 30000L);
        config.maxRetries = getIntValue(context, MAX_RETRIES, flowFile, 3);
        config.retryBackoffMs = getTimePeriodValue(context, RETRY_BACKOFF, flowFile, 10000L);

        if (config.partialFetch) {
            config.fetchBufferSize = getDataSizeValue(context, FETCH_BUFFER_SIZE, flowFile, 102400L);
        } else {
            config.fetchBufferSize = 102400L;
        }

        if (config.includeAttachments) {
            config.maxAttachmentSize = getDataSizeValue(context, MAX_ATTACHMENT_SIZE, flowFile, 50L * 1024 * 1024);
            config.maxAttachmentsPerMessage = getIntValue(context, MAX_ATTACHMENTS_PER_MESSAGE, flowFile, 100);
        } else {
            config.maxAttachmentSize = Long.MAX_VALUE;
            config.maxAttachmentsPerMessage = Integer.MAX_VALUE;
        }

        return config;
    }

    private String getStringValue(ProcessContext context, PropertyDescriptor descriptor, FlowFile flowFile) {
        PropertyValue propertyValue = context.getProperty(descriptor);
        if (!descriptor.isExpressionLanguageSupported()) return propertyValue.getValue();
        try {
            String value;
            if (flowFile != null) value = propertyValue.evaluateAttributeExpressions(flowFile).getValue();
            else value = propertyValue.evaluateAttributeExpressions().getValue();
            if (value == null && descriptor.isRequired()) {
                getLogger().warn("Expression evaluated to null for required property: {}", descriptor.getName());
            }
            return value;
        } catch (Exception e) {
            getLogger().warn("Error evaluating expression for {}", descriptor.getName(), e);
            return null;
        }
    }

    private boolean getBooleanValue(ProcessContext context, PropertyDescriptor descriptor, FlowFile flowFile) {
        PropertyValue propertyValue = context.getProperty(descriptor);
        try {
            if (flowFile != null && descriptor.isExpressionLanguageSupported()) {
                return propertyValue.evaluateAttributeExpressions(flowFile).asBoolean();
            } else if (descriptor.isExpressionLanguageSupported()) {
                return propertyValue.evaluateAttributeExpressions().asBoolean();
            } else {
                return propertyValue.asBoolean();
            }
        } catch (Exception e) {
            getLogger().warn("Error evaluating boolean for {}", descriptor.getName(), e);
            return false;
        }
    }

    private int getIntValue(ProcessContext context, PropertyDescriptor descriptor, FlowFile flowFile, int defaultValue) {
        try {
            PropertyValue prop = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile);
            Integer val = prop.asInteger();
            return val != null ? val : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private long getDataSizeValue(ProcessContext context, PropertyDescriptor descriptor, FlowFile flowFile, long defaultValue) {
        try {
            PropertyValue prop = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile);
            Double val = prop.asDataSize(DataUnit.B);
            return val != null ? val.longValue() : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private long getTimePeriodValue(ProcessContext context, PropertyDescriptor descriptor, FlowFile flowFile, long defaultValue) {
        try {
            PropertyValue prop = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile);
            Long val = prop.asTimePeriod(TimeUnit.MILLISECONDS);
            return val != null ? val : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    boolean dynamicConfigValidate(ImapConfig config, FlowFile flowFile, ProcessSession session) {
        if (config.filterParseError != null) {
            handleError(session, flowFile, "FILTER_PARSE_ERROR", config.filterParseError, REL_FAILURE);
            return false;
        }
        if (!checkRequired(config.host, "host", session, flowFile)) return false;
        if (!checkRequired(config.user, "username", session, flowFile)) return false;
        if (!checkRequired(config.folder, "folder", session, flowFile)) return false;
        if (!checkPort(config.port, session, flowFile)) return false;
        if (!checkPositive(config.batchSize, "batch size", session, flowFile)) return false;
        if (AUTH_PASSWORD.getValue().equals(config.authMode)) {
            if (!checkRequired(config.password, "password", session, flowFile)) return false;
        }
        if (!checkMessageSize(config.maxMessageSize, session, flowFile)) return false;
        if (config.includeAttachments) {
            if (!checkAttachmentLimits(config, session, flowFile)) return false;
        }
        if (!checkPositive(config.reconnectIntervalMs, "reconnect interval", session, flowFile)) return false;
        if (!checkPositive(config.connectionTimeoutMs, "connection timeout", session, flowFile)) return false;
        if (!checkRetrySettings(config, session, flowFile)) return false;
        if (config.useSsl && config.useTls) {
            handleError(session, flowFile, "CONFLICTING_SECURITY", "Cannot use both SSL and TLS", REL_FAILURE);
            return false;
        }
        if (config.partialFetch && config.fetchBufferSize > config.maxMessageSize) {
            getLogger().warn("Fetch buffer size exceeds max message size, adjusting...");
            config.fetchBufferSize = config.maxMessageSize;
        }
        return true;
    }

    private void validateHostAndSni(ValidationContext context, Collection<ValidationResult> results) {
        String host = context.getProperty(HOST).getValue();
        String sni = context.getProperty(SNI_HOSTNAME).getValue();

        if (host == null || host.trim().isEmpty()) {
            results.add(new ValidationResult.Builder()
                    .subject("Host Name")
                    .valid(false)
                    .explanation("Host cannot be empty")
                    .build());
        }

        if (sni != null && !sni.trim().isEmpty() && isIpAddress(sni)) {
            results.add(new ValidationResult.Builder()
                    .subject("SNI Hostname")
                    .valid(true)
                    .explanation("SNI with IP address may not work - SNI typically requires a domain name")
                    .build());
        }

        if (isIpAddress(host) && (sni == null || sni.trim().isEmpty())) {
            results.add(new ValidationResult.Builder()
                    .subject("SNI Hostname")
                    .valid(true)
                    .explanation("Host is IP address but SNI not specified. Consider setting SNI hostname for proper TLS handshake")
                    .build());
        }
    }

    private void validateSecuritySettings(ValidationContext context, Collection<ValidationResult> results) {
        boolean useSsl = context.getProperty(USE_SSL).asBoolean();
        boolean useTls = context.getProperty(USE_TLS).asBoolean();
        int port = getPortOrDefault(context);

        if (useSsl && useTls) {
            results.add(new ValidationResult.Builder()
                    .subject("SSL/TLS Settings")
                    .valid(false)
                    .explanation("Cannot use both SSL and TLS/STARTTLS at the same time")
                    .build());
        }

        if (useSsl && port == 143) {
            results.add(new ValidationResult.Builder()
                    .subject("SSL/TLS Settings")
                    .valid(true)
                    .explanation("Using SSL (IMAPS) with port 143. Standard IMAPS port is 993.")
                    .build());
        }

        if (useTls && port == 993) {
            results.add(new ValidationResult.Builder()
                    .subject("SSL/TLS Settings")
                    .valid(true)
                    .explanation("Using TLS/STARTTLS with port 993. Standard port is 143.")
                    .build());
        }
    }

    private void validateAuthSettings(ValidationContext context, Collection<ValidationResult> results) {
        String authMode = context.getProperty(AUTH_MODE).getValue();

        if (AUTH_PASSWORD.getValue().equals(authMode)) {
            if (!context.getProperty(PASSWORD).isSet()) {
                results.add(new ValidationResult.Builder()
                        .subject("Password")
                        .valid(false)
                        .explanation("Password is required when Authorization Mode is 'Use Password'")
                        .build());
            }
        } else if (AUTH_OAUTH2.getValue().equals(authMode)) {
            if (!context.getProperty(OAUTH2_TOKEN_PROVIDER).isSet()) {
                results.add(new ValidationResult.Builder()
                        .subject("OAuth2 Access Token Provider")
                        .valid(false)
                        .explanation("OAuth2 Access Token Provider is required when using OAuth2")
                        .build());
            }
            if (context.getProperty(PASSWORD).isSet()) {
                results.add(new ValidationResult.Builder()
                        .subject("Password")
                        .valid(false)
                        .explanation("Password should not be set when using OAuth2 mode.")
                        .build());
            }
        }
    }

    private void validateSizeSettings(ValidationContext context, Collection<ValidationResult> results) {
        long maxMessageSize = context.getProperty(MAX_MESSAGE_SIZE).asDataSize(DataUnit.B).longValue();
        boolean includeAttachments = context.getProperty(INCLUDE_ATTACHMENTS).asBoolean();
        String fetchMode = context.getProperty(FETCH_MODE).getValue();

        if (FETCH_MODE_ATTACHMENTS_ONLY.getValue().equals(fetchMode) && !includeAttachments) {
            results.add(new ValidationResult.Builder()
                    .subject("Fetch Mode / Include Attachments")
                    .valid(false)
                    .explanation("When Fetch Mode is 'Attachments Only', 'Include Attachments' must be enabled")
                    .build());
        }

        if (maxMessageSize <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Max Message Size")
                    .valid(false)
                    .explanation("Max message size must be positive")
                    .build());
        }

        if (includeAttachments) {
            long maxAttachmentSize = context.getProperty(MAX_ATTACHMENT_SIZE).asDataSize(DataUnit.B).longValue();
            int maxAttachments = context.getProperty(MAX_ATTACHMENTS_PER_MESSAGE).asInteger();

            if (maxAttachmentSize <= 0) {
                results.add(new ValidationResult.Builder()
                        .subject("Max Attachment Size")
                        .valid(false)
                        .explanation("Max attachment size must be positive")
                        .build());
            }

            if (maxAttachmentSize > maxMessageSize) {
                results.add(new ValidationResult.Builder()
                        .subject("Max Attachment Size")
                        .valid(false)
                        .explanation("Max attachment size cannot exceed max message size")
                        .build());
            }

            if (maxAttachments <= 0) {
                results.add(new ValidationResult.Builder()
                        .subject("Max Attachments Per Message")
                        .valid(false)
                        .explanation("Max attachments per message must be positive")
                        .build());
            }
        }
    }

    private void validateFilterSettings(ValidationContext context, Collection<ValidationResult> results) {
        String imapFilter = context.getProperty(IMAP_FILTER).getValue();
        boolean incrementalFetch = context.getProperty(INCREMENTAL_FETCH).asBoolean();
        int maxDepth = context.getProperty(MAX_RECURSION_DEPTH).asInteger();

        if (imapFilter != null && !imapFilter.trim().isEmpty()) {
            if (imapFilter.length() > 5000) {
                results.add(new ValidationResult.Builder()
                        .subject("IMAP Filter")
                        .valid(false)
                        .explanation("Filter exceeds maximum length of 5000 characters")
                        .build());
            } else {
                try {
                    int depthLimit = maxDepth <= 0 ? Integer.MAX_VALUE : maxDepth;
                    IMAPFilterParser.parse(imapFilter, 0, depthLimit);
                } catch (IllegalArgumentException e) {
                    results.add(new ValidationResult.Builder()
                            .subject("IMAP Filter")
                            .valid(false)
                            .explanation("Invalid IMAP filter syntax: " + e.getMessage())
                            .build());
                }
            }
        }

        if (incrementalFetch) {
            if (imapFilter != null && !imapFilter.trim().isEmpty()) {
                if (!imapFilter.contains("${lastUID}") && containsUIDCriteria(imapFilter)) {
                    results.add(new ValidationResult.Builder()
                            .subject("Incremental Fetch")
                            .valid(true)
                            .explanation("Filter contains static UID. Consider using ${lastUID} for true incremental processing")
                            .build());
                }
            }
        }
    }

    private void validateRecursionSettings(ValidationContext context, Collection<ValidationResult> results) {
        int maxDepth = context.getProperty(MAX_RECURSION_DEPTH).asInteger();
        if (maxDepth < -1 || maxDepth == 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Max Recursion Depth")
                    .valid(false)
                    .explanation("Max recursion depth must be -1 (unlimited) or positive")
                    .build());
        }
    }

    private void validateRetrySettings(ValidationContext context, Collection<ValidationResult> results) {
        int maxRetries = context.getProperty(MAX_RETRIES).asInteger();
        long retryBackoff = context.getProperty(RETRY_BACKOFF).asTimePeriod(TimeUnit.MILLISECONDS);

        if (maxRetries < 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Max Retries")
                    .valid(false)
                    .explanation("Max retries must be non-negative")
                    .build());
        }

        if (retryBackoff <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Retry Backoff")
                    .valid(false)
                    .explanation("Retry backoff must be positive")
                    .build());
        }
    }

    private void validateBatchSettings(ValidationContext context, Collection<ValidationResult> results) {
        int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        if (batchSize <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Batch Size")
                    .valid(false)
                    .explanation("Batch size must be positive")
                    .build());
        }
    }

    private void validateTimeoutSettings(ValidationContext context, Collection<ValidationResult> results) {
        long connectionTimeout = context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        long reconnectInterval = context.getProperty(RECONNECT_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

        if (connectionTimeout <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Connection Timeout")
                    .valid(false)
                    .explanation("Connection timeout must be positive")
                    .build());
        }

        if (reconnectInterval <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Reconnect Interval")
                    .valid(false)
                    .explanation("Reconnect interval must be positive")
                    .build());
        }
    }

    private void validatePartialFetchSettings(ValidationContext context, Collection<ValidationResult> results) {
        boolean partialFetch = context.getProperty(PARTIAL_FETCH).asBoolean();
        if (partialFetch) {
            String bufferSize = context.getProperty(FETCH_BUFFER_SIZE).getValue();
            if (bufferSize == null || bufferSize.trim().isEmpty()) {
                results.add(new ValidationResult.Builder()
                        .subject("Fetch Buffer Size")
                        .valid(false)
                        .explanation("Fetch buffer size is required when partial fetch is enabled")
                        .build());
            } else {
                try {
                    long bufferSizeBytes = context.getProperty(FETCH_BUFFER_SIZE).asDataSize(DataUnit.B).longValue();
                    if (bufferSizeBytes <= 0) {
                        results.add(new ValidationResult.Builder()
                                .subject("Fetch Buffer Size")
                                .valid(false)
                                .explanation("Fetch buffer size must be positive")
                                .build());
                    }
                } catch (Exception e) {
                    results.add(new ValidationResult.Builder()
                            .subject("Fetch Buffer Size")
                            .valid(false)
                            .explanation("Invalid fetch buffer size format")
                            .build());
                }
            }
        }
    }

    private boolean checkRequired(String value, String fieldName, ProcessSession session, FlowFile flowFile) {
        if (value == null || value.trim().isEmpty()) {
            handleError(session, flowFile, "INVALID_" + fieldName.toUpperCase(), fieldName + " cannot be empty", REL_FAILURE);
            return false;
        }
        return true;
    }

    private boolean checkPort(int port, ProcessSession session, FlowFile flowFile) {
        if (port <= 0 || port > 65535) {
            handleError(session, flowFile, "INVALID_PORT", "Port must be between 1 and 65535", REL_FAILURE);
            return false;
        }
        return true;
    }

    private boolean checkPositive(long value, String fieldName, ProcessSession session, FlowFile flowFile) {
        if (value <= 0) {
            handleError(session, flowFile, "INVALID_" + fieldName.toUpperCase().replace(" ", "_"),
                    fieldName + " must be positive", REL_FAILURE);
            return false;
        }
        return true;
    }

    private boolean checkMessageSize(long maxMessageSize, ProcessSession session, FlowFile flowFile) {
        if (maxMessageSize <= 0) {
            handleError(session, flowFile, "INVALID_MAX_MESSAGE_SIZE", "Max message size must be positive", REL_FAILURE);
            return false;
        }
        return true;
    }

    private boolean checkAttachmentLimits(ImapConfig config, ProcessSession session, FlowFile flowFile) {
        if (config.maxAttachmentSize <= 0) {
            handleError(session, flowFile, "INVALID_MAX_ATTACHMENT_SIZE", "Max attachment size must be positive", REL_FAILURE);
            return false;
        }
        if (config.maxAttachmentSize > config.maxMessageSize) {
            handleError(session, flowFile, "INVALID_SIZE_LIMITS", "Max attachment size cannot exceed max message size", REL_FAILURE);
            return false;
        }
        if (config.maxAttachmentsPerMessage <= 0) {
            handleError(session, flowFile, "INVALID_MAX_ATTACHMENTS", "Max attachments per message must be positive", REL_FAILURE);
            return false;
        }
        return true;
    }

    private boolean checkRetrySettings(ImapConfig config, ProcessSession session, FlowFile flowFile) {
        if (config.maxRetries < 0) {
            handleError(session, flowFile, "INVALID_MAX_RETRIES", "Max retries must be non-negative", REL_FAILURE);
            return false;
        }
        if (config.retryBackoffMs <= 0) {
            handleError(session, flowFile, "INVALID_RETRY_BACKOFF", "Retry backoff must be positive", REL_FAILURE);
            return false;
        }
        return true;
    }

    private boolean isIpAddress(String host) {
        if (host == null || host.trim().isEmpty()) return false;

        String trimmed = host.trim();
        String checkHost = trimmed.replaceAll("^\\[|\\]$", "");

        try {
            InetAddress addr = InetAddress.getByName(checkHost);
            String hostAddress = addr.getHostAddress();

            String normalizedInput = checkHost;

            if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                normalizedInput = trimmed.substring(1, trimmed.length() - 1);
            }

            return hostAddress.equals(normalizedInput) ||
                    isIPv6CompressedForm(normalizedInput, hostAddress);

        } catch (UnknownHostException e) {
            return false;
        }
    }

    private boolean isIPv6CompressedForm(String input, String resolved) {
        if (!resolved.contains(":")) return false;

        try {
            InetAddress inputAddr = InetAddress.getByName(input);
            InetAddress resolvedAddr = InetAddress.getByName(resolved);
            return Arrays.equals(inputAddr.getAddress(), resolvedAddr.getAddress());
        } catch (UnknownHostException e) {
            return false;
        }
    }

    private int getPortOrDefault(ValidationContext context) {
        try {
            return context.getProperty(PORT).asInteger();
        } catch (NumberFormatException e) {
            return context.getProperty(USE_SSL).asBoolean() ? 993 : 143;
        }
    }

    private static class FolderUIDInfo {
        final UIDFolder uidFolder;
        final boolean canUseUID;

        FolderUIDInfo(UIDFolder uidFolder, boolean canUseUID) {
            this.uidFolder = uidFolder;
            this.canUseUID = canUseUID;
        }
    }

    private static class MessageFilterResult {
        final Message[] messagesToProcess;
        final long highestInBatch;

        MessageFilterResult(Message[] messagesToProcess, long highestInBatch) {
            this.messagesToProcess = messagesToProcess;
            this.highestInBatch = highestInBatch;
        }
    }

    private static class FolderResult {
        final boolean hadMessages;
        final int processedCount;
        final Map<String, Long> highestUIDs;
        final Map<String, Long> highestSeqNums;

        FolderResult(boolean hadMessages, int processedCount,
                     Map<String, Long> highestUIDs, Map<String, Long> highestSeqNums) {
            this.hadMessages = hadMessages;
            this.processedCount = processedCount;
            this.highestUIDs = highestUIDs;
            this.highestSeqNums = highestSeqNums;
        }
    }

    private static class ProcessingResult {
        final long totalProcessed;
        final Map<String, Long> highestUIDs;
        final Map<String, Long> highestSeqNums;
        final boolean hadMessages;

        ProcessingResult(long totalProcessed, Map<String, Long> highestUIDs,
                         Map<String, Long> highestSeqNums, boolean hadMessages) {
            this.totalProcessed = totalProcessed;
            this.highestUIDs = highestUIDs;
            this.highestSeqNums = highestSeqNums;
            this.hadMessages = hadMessages;
        }
    }

    protected class ImapConfig {
        String host;
        String sniHostname;
        int port;
        String user;
        String password;
        String folder;
        String stateFolder;
        boolean useSsl;
        boolean useTls;
        int batchSize;
        String fetchMode;
        boolean partialFetch;
        long fetchBufferSize;
        long maxMessageSize;
        long maxAttachmentSize;
        int maxAttachmentsPerMessage;
        boolean deleteMessages;
        boolean markRead;
        boolean includeHeaders;
        boolean includeAttachments;
        boolean recursive;
        int maxRecursionDepth;
        long reconnectIntervalMs;
        long connectionTimeoutMs;
        String authMode;
        String imapFilter;
        SearchTerm searchTerm;
        String filterParseError;
        boolean incrementalFetch;
        int maxRetries;
        long retryBackoffMs;
    }

    static class FolderInfo {
        final Folder folder;
        final String path;
        final int depth;

        FolderInfo(Folder folder, String path, int depth) {
            this.folder = folder;
            this.path = path;
            this.depth = depth;
        }
    }
}