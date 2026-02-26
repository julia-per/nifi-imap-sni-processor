package org.custom.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import javax.mail.*;
import javax.mail.internet.MimeUtility;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CustomIMAPMessageProcessor {
    private final ComponentLog logger;

    private static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();
    private static final Relationship REL_ATTACHMENTS = new Relationship.Builder().name("attachments").build();
    private static final Relationship REL_PARSE_ERROR = new Relationship.Builder().name("parse-error").build();

    public CustomIMAPMessageProcessor(ComponentLog logger) {
        this.logger = logger;
    }

    public long processMessageStream(ProcessContext context, ProcessSession session,
                                     Message[] messages, UIDFolder uidFolder,
                                     String fetchMode, boolean includeHeaders,
                                     boolean includeAttachments, long maxMessageSize,
                                     long maxAttachmentSize, int maxAttachmentsPerMessage,
                                     boolean markRead, boolean deleteMessages,
                                     String batchId, String folderPath, int batchSize,
                                     Folder currentFolder, UIDFolder uidFolderForFlags) throws MessagingException {  // ИЗМЕНЕНО: добавлен uidFolderForFlags

        if (messages.length == 0) {
            return -1;
        }

        logger.info("Folder {} - Streaming {} messages with batch size {}",
                folderPath, messages.length, batchSize);

        long highestSuccessUID = -1;
        int messagesProcessed = 0;
        List<Long> successUIDBatch = new ArrayList<>();  // ИЗМЕНЕНО: храним UID вместо Message объектов

        for (int i = 0; i < messages.length; i++) {
            if (Thread.currentThread().isInterrupted()) {
                logger.info("Processing interrupted for folder {}", folderPath);
                break;
            }

            Message message = messages[i];
            long messageUID = uidFolder != null ? uidFolder.getUID(message) : -1;

            try {
                int messageSizeInt = message.getSize();
                long messageSize = messageSizeInt > 0 ? messageSizeInt : 0;
                if (messageSize > maxMessageSize) {
                    logger.warn("Folder {} - Message UID {} exceeds max size, skipping", folderPath, messageUID);

                    FlowFile errorFF = session.create();
                    errorFF = session.putAttribute(errorFF, "error.type", "MESSAGE_TOO_LARGE");
                    errorFF = session.putAttribute(errorFF, "error.message",
                            String.format("Message size %d exceeds limit %d", messageSize, maxMessageSize));
                    errorFF = session.putAttribute(errorFF, "email.folder", folderPath);
                    if (messageUID > 0) {
                        errorFF = session.putAttribute(errorFF, "email.uid", String.valueOf(messageUID));
                    }
                    session.transfer(errorFF, REL_PARSE_ERROR);
                    continue;
                }

                Message messageForProcessing = message;
                if (uidFolder != null && messageUID > 0) {
                    messageForProcessing = uidFolder.getMessageByUID(messageUID);
                    if (messageForProcessing == null) {
                        logger.warn("Could not retrieve message by UID {}, skipping", messageUID);
                        continue;
                    }
                }

                boolean success = processSingleMessageStreaming(messageForProcessing, fetchMode, includeHeaders,
                        includeAttachments, maxAttachmentSize, maxAttachmentsPerMessage,
                        session, batchId, messageUID, folderPath);

                if (success) {
                    if (messageUID > 0) {
                        successUIDBatch.add(messageUID);
                        if (messageUID > highestSuccessUID) {
                            highestSuccessUID = messageUID;
                        }
                    }
                    messagesProcessed++;

                    if (successUIDBatch.size() >= batchSize) {
                        updateMessagesFlagsByUID(successUIDBatch, markRead, deleteMessages, currentFolder, uidFolderForFlags);
                        logger.debug("Folder {} - Processed batch of {} messages", folderPath, successUIDBatch.size());
                        successUIDBatch.clear();
                    }
                }

            } catch (Exception e) {
                logger.error("Folder {} - Error processing message UID: {}", folderPath, messageUID, e);

                FlowFile errorFF = session.create();
                errorFF = session.putAttribute(errorFF, "error.type", "MESSAGE_PROCESSING_ERROR");
                errorFF = session.putAttribute(errorFF, "error.message", e.getMessage());
                errorFF = session.putAttribute(errorFF, "email.folder", folderPath);
                if (messageUID > 0) {
                    errorFF = session.putAttribute(errorFF, "email.uid", String.valueOf(messageUID));
                }
                session.transfer(errorFF, REL_PARSE_ERROR);
            }

            if (messagesProcessed >= batchSize * 10) {
                logger.info("Folder {} - Processed {} messages, yielding for next trigger", folderPath, messagesProcessed);
                context.yield();
                break;
            }
        }

        if (!successUIDBatch.isEmpty()) {
            updateMessagesFlagsByUID(successUIDBatch, markRead, deleteMessages, currentFolder, uidFolderForFlags);
            logger.debug("Folder {} - Processed final batch of {} messages", folderPath, successUIDBatch.size());
        }

        if (deleteMessages && highestSuccessUID > 0) {
            try {
                if (currentFolder != null && currentFolder.isOpen()) {
                    currentFolder.expunge();
                }
            } catch (MessagingException e) {
                logger.warn("Folder {} - Failed to expunge messages", folderPath, e);
            }
        }

        logger.info("Folder {} - Processed {} messages, highest UID: {}", folderPath, messagesProcessed, highestSuccessUID);
        return highestSuccessUID;
    }

    public boolean processSingleMessageStreaming(Message message, String fetchMode,
                                                 boolean includeHeaders, boolean includeAttachments,
                                                 long maxAttachmentSize, int maxAttachmentsPerMessage,
                                                 ProcessSession session, String batchId,
                                                 long messageUID, String folderPath)
            throws MessagingException, IOException {

        FlowFile flowFile = null;
        boolean transferred = false;

        try {
            flowFile = session.create();

            if (includeHeaders) {
                flowFile = addEmailAttributes(flowFile, message, session, batchId);
            } else {
                flowFile = session.putAttribute(flowFile, "email.batch.id", batchId);
            }

            flowFile = session.putAttribute(flowFile, "email.folder", folderPath);

            if (messageUID > 0) {
                flowFile = session.putAttribute(flowFile, "email.uid", String.valueOf(messageUID));
            }

            if ("HEADERS_ONLY".equals(fetchMode)) {
                session.transfer(flowFile, REL_SUCCESS);
                transferred = true;
            } else if ("ATTACHMENTS_ONLY".equals(fetchMode)) {
                session.transfer(flowFile, REL_SUCCESS);
                transferred = true;
                try {
                    processAttachmentsStreaming(message, maxAttachmentSize, maxAttachmentsPerMessage,
                            session, batchId, folderPath);
                } catch (Exception e) {
                    logger.warn("Failed to extract attachments but message was transferred", e);
                }
            } else {
                try (InputStream is = message.getInputStream()) {
                    flowFile = session.importFrom(is, flowFile);
                }

                session.transfer(flowFile, REL_SUCCESS);
                transferred = true;

                if (includeAttachments) {
                    try {
                        processAttachmentsStreaming(message, maxAttachmentSize, maxAttachmentsPerMessage,
                                session, batchId, folderPath);
                    } catch (Exception e) {
                        logger.warn("Failed to extract attachments but message was transferred", e);
                    }
                }
            }

            return true;

        } finally {
            if (flowFile != null && !transferred) {
                session.remove(flowFile);
            }
        }
    }

    private void processAttachmentsStreaming(Message message, long maxAttachmentSize,
                                             int maxAttachmentsPerMessage,
                                             ProcessSession session, String batchId,
                                             String folderPath) {
        try {
            Object content = message.getContent();
            if (!(content instanceof Multipart)) {
                return;
            }

            Multipart multipart = (Multipart) content;
            processMultipartStreaming(multipart, message, maxAttachmentSize,
                    maxAttachmentsPerMessage, session, batchId, folderPath, 0);

        } catch (Exception e) {
            logger.warn("Unexpected error extracting attachments from message", e);
        }
    }

    private int processMultipartStreaming(Multipart multipart, Message message, long maxAttachmentSize,
                                          int maxAttachmentsPerMessage, ProcessSession session,
                                          String batchId, String folderPath, int currentCount)
            throws MessagingException, IOException {

        for (int i = 0; i < multipart.getCount(); i++) {
            if (currentCount >= maxAttachmentsPerMessage) {
                return currentCount;
            }

            BodyPart bodyPart = multipart.getBodyPart(i);

            if (bodyPart.isMimeType("multipart/*")) {
                currentCount = processMultipartStreaming((Multipart) bodyPart.getContent(), message,
                        maxAttachmentSize, maxAttachmentsPerMessage,
                        session, batchId, folderPath, currentCount);
                continue;
            }

            String disposition = bodyPart.getDisposition();
            boolean isAttachment = Part.ATTACHMENT.equalsIgnoreCase(disposition) ||
                    (disposition == null && bodyPart.getFileName() != null);

            if (!isAttachment) {
                continue;
            }

            int attachmentSizeInt = bodyPart.getSize();
            long attachmentSize = attachmentSizeInt > 0 ? attachmentSizeInt : 0;
            if (attachmentSize > maxAttachmentSize) {
                continue;
            }

            FlowFile attachmentFF = createAttachmentFlowFileStreaming(bodyPart, i, message,
                    session, batchId, folderPath);
            if (attachmentFF != null) {
                session.transfer(attachmentFF, REL_ATTACHMENTS);
                currentCount++;
            }
        }

        return currentCount;
    }

    private FlowFile createAttachmentFlowFileStreaming(BodyPart bodyPart, int index, Message message,
                                                       ProcessSession session, String batchId, String folderPath) throws MessagingException, IOException {
        FlowFile flowFile = null;
        boolean transferred = false;

        try {
            flowFile = session.create();

            String fileName;
            String mimeType;
            try {
                fileName = bodyPart.getFileName();
                if (fileName == null) {
                    fileName = "attachment_" + index;
                }
                fileName = decodeText(fileName);

                mimeType = bodyPart.getContentType();
                if (mimeType.contains(";")) {
                    mimeType = mimeType.substring(0, mimeType.indexOf(';')).trim();
                }
            } catch (MessagingException e) {
                fileName = "attachment_" + index + "_unknown";
                mimeType = "application/octet-stream";
            }

            try (InputStream is = bodyPart.getInputStream()) {
                flowFile = session.importFrom(is, flowFile);
            }

            flowFile = session.putAttribute(flowFile, "attachment.filename", fileName);
            flowFile = session.putAttribute(flowFile, "attachment.mime.type", mimeType);
            flowFile = session.putAttribute(flowFile, "email.folder", folderPath);

            try {
                int size = bodyPart.getSize();
                flowFile = session.putAttribute(flowFile, "attachment.size", size > 0 ? String.valueOf(size) : "unknown");
            } catch (MessagingException e) {
                flowFile = session.putAttribute(flowFile, "attachment.size", "unknown");
            }

            try {
                String subject = decodeText(message.getSubject() != null ? message.getSubject() : "");
                flowFile = session.putAttribute(flowFile, "email.subject", subject);

                String from = message.getFrom() != null && message.getFrom().length > 0 ?
                        decodeText(message.getFrom()[0].toString()) : "unknown";
                flowFile = session.putAttribute(flowFile, "email.from", from);

                String[] messageIdHeader = message.getHeader("Message-ID");
                String messageId = (messageIdHeader != null && messageIdHeader.length > 0) ? messageIdHeader[0] : "";
                flowFile = session.putAttribute(flowFile, "email.parent.message.id", messageId);

                flowFile = session.putAttribute(flowFile, "email.batch.id", batchId);

            } catch (MessagingException e) {
                flowFile = session.putAttribute(flowFile, "email.batch.id", batchId);
            }

            transferred = true;
            return flowFile;

        } finally {
            if (flowFile != null && !transferred) {
                session.remove(flowFile);
            }
        }
    }

    private void updateMessagesFlagsByUID(List<Long> messageUIDs, boolean markRead,
                                          boolean deleteMessages, Folder folder, UIDFolder uidFolder) {
        if (messageUIDs == null || messageUIDs.isEmpty()) return;
        if (!markRead && !deleteMessages) return;
        if (folder == null || !folder.isOpen() || folder.getMode() != Folder.READ_WRITE) {
            logger.warn("Cannot update message flags - folder not open or not writable");
            return;
        }
        if (uidFolder == null) {
            logger.warn("Cannot update message flags by UID - UIDFolder not available");
            return;
        }

        for (Long uid : messageUIDs) {
            try {
                Message msg = uidFolder.getMessageByUID(uid);
                if (msg != null) {
                    if (markRead) msg.setFlag(Flags.Flag.SEEN, true);
                    if (deleteMessages) msg.setFlag(Flags.Flag.DELETED, true);
                    logger.debug("Set flags for message UID: {}", uid);
                } else {
                    logger.warn("Could not find message by UID: {}", uid);
                }
            } catch (MessagingException e) {
                logger.warn("Failed to set flags for message UID: {}", uid, e);
            }
        }
    }

    FlowFile addEmailAttributes(FlowFile flowFile, Message message, ProcessSession session, String batchId) {
        try {
            flowFile = session.putAttribute(flowFile, "email.subject",
                    decodeText(message.getSubject() != null ? message.getSubject() : ""));

            Address[] from = message.getFrom();
            flowFile = session.putAttribute(flowFile, "email.from",
                    from != null && from.length > 0 ? decodeText(from[0].toString()) : "unknown");

            Address[] to = message.getRecipients(Message.RecipientType.TO);
            flowFile = session.putAttribute(flowFile, "email.to",
                    to != null && to.length > 0 ? decodeText(to[0].toString()) : "");

            Address[] cc = message.getRecipients(Message.RecipientType.CC);
            flowFile = session.putAttribute(flowFile, "email.cc",
                    cc != null && cc.length > 0 ? decodeText(cc[0].toString()) : "");

            Date receivedDate = message.getReceivedDate();
            flowFile = session.putAttribute(flowFile, "email.received.date",
                    receivedDate != null ? String.valueOf(receivedDate.getTime()) : "");

            Date sentDate = message.getSentDate();
            flowFile = session.putAttribute(flowFile, "email.sent.date",
                    sentDate != null ? String.valueOf(sentDate.getTime()) : "");

            Flags flags = message.getFlags();
            if (flags != null) {
                StringBuilder flagsStr = new StringBuilder();
                if (flags.contains(Flags.Flag.SEEN)) flagsStr.append("SEEN ");
                if (flags.contains(Flags.Flag.FLAGGED)) flagsStr.append("FLAGGED ");
                if (flags.contains(Flags.Flag.ANSWERED)) flagsStr.append("ANSWERED ");
                if (flags.contains(Flags.Flag.DRAFT)) flagsStr.append("DRAFT ");
                if (flags.contains(Flags.Flag.DELETED)) flagsStr.append("DELETED ");
                if (flags.contains(Flags.Flag.RECENT)) flagsStr.append("RECENT ");
                flowFile = session.putAttribute(flowFile, "email.flags", flagsStr.toString().trim());
            } else {
                flowFile = session.putAttribute(flowFile, "email.flags", "");
            }

            int size = message.getSize();
            flowFile = session.putAttribute(flowFile, "email.size", String.valueOf(Math.max(size, 0)));

            String[] messageIdHeader = message.getHeader("Message-ID");
            String messageId = (messageIdHeader != null && messageIdHeader.length > 0) ? messageIdHeader[0] : "";
            flowFile = session.putAttribute(flowFile, "email.message.id", messageId);

            String[] inReplyToHeader = message.getHeader("In-Reply-To");
            String inReplyTo = (inReplyToHeader != null && inReplyToHeader.length > 0) ? inReplyToHeader[0] : "";
            flowFile = session.putAttribute(flowFile, "email.in.reply.to", inReplyTo);

            flowFile = session.putAttribute(flowFile, "email.batch.id", batchId);

        } catch (MessagingException e) {
            flowFile = session.putAttribute(flowFile, "email.batch.id", batchId);
        }
        return flowFile;
    }

    private String decodeText(String text) {
        if (text == null) return "";
        try {
            return MimeUtility.decodeText(text);
        } catch (UnsupportedEncodingException e) {
            return text;
        }
    }
}