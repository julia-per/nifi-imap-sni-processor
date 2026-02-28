# Custom IMAP Processor for Apache NiFi

A production-ready, streaming IMAP processor for Apache NiFi that efficiently fetches and processes emails with minimal memory footprint. Designed for high-volume email ingestion pipelines.

## Features

Core Capabilities: 
Streaming Architecture - Processes emails in configurable batches without loading all messages into memory.
Incremental Fetching - Remembers last processed UIDs per folder, only new messages on subsequent polls.
Dual Authentication - Supports both password-based and OAuth2 (XOAUTH2) authentication.
Server-Side Filtering - IMAP search criteria combined with incremental UID tracking.
Recursive Folder Traversal - Search through subfolders with configurable depth limits.

Memory Management:
Partial Fetch - Streams large messages in chunks to prevent OOM errors.
Configurable Buffer Size - Control memory usage per message.
Size Limits - Enforce maximum message and attachment sizes.
Batch Processing - Process messages in small, controlled batches.

Error Handling:
Retry Logic - Exponential backoff for transient failures.
Dead Letter Queue - Route unprocessable messages to failure relationships.
Comprehensive Error Attributes - Detailed error information for debugging.

Attachment Processing:
Separate FlowFiles - Extract attachments as individual FlowFiles.
Configurable Limits - Control max attachment size and count per message.
MIME Type Detection - Preserves attachment metadata.
Parent Message Reference - Link attachments back to source email.

## How It Works

The processor maintains a persistent connection to the IMAP server and:

1. Connects to the specified folder or traverses recursively
2. Applies IMAP filters if configured
3. Filters messages by UID when incremental fetch is enabled
4. Streams messages in batches according to fetch mode
5. Updates state with last processed UID per folder
6. Handles attachments as separate FlowFiles when enabled
7. Manages reconnection with exponential backoff on failures
8. Supports expression language

State is stored cluster-wide, enabling distributed processing of the same mailbox.

## Requirements

Apache NiFi Version 2.7.0 / 
Java JDK 21 / 
Maven 3.6 or higher for building from source / 
IMAP Server any RFC-compliant IMAP server (Gmail, Outlook, Exchange, Dovecot, etc.)

## Installation

Option 1:
1. Download the latest NAR file [https://github.com/julia-per/nifi-processor/releases/tag/latest]
2. Copy the NAR file to NiFi's extensions directory:
   cp nifi-custom-nar-*.nar /path/to/nifi/extensions/
3. Restart NiFi:
   /path/to/nifi/bin/nifi.cmd start

Option 2:
1. git clone [https://github.com/julia-per/nifi-processor.git]
2. cd nifi-processor
3. mvn clean install

The NAR file will be in: nifi-custom-nar/target/nifi-custom-nar-*.nar

After installation, find "CustomIMAP" in the NiFi processor palette.

## Configuration

Basic Settings:
Host Name - IMAP server address (e.g., imap.gmail.com) - Required.
Port - Server port - Default 993 - Required.
Username - Email account username - Required.
Folder - Folder to read from - Default INBOX - Required.

Authentication:
Authorization Mode - Choose between Use Password or Use OAuth2 - Required.
Password - Password when using password mode - Required for password mode.
OAuth2 Access Token Provider - Controller service when using OAuth2 - Required for OAuth2 mode.

Fetch Options:
Fetch Mode - Headers Only, Full Message, or Attachments Only - Default Full Message.
Include Headers - Add email headers as attributes - Default true.
Include Attachments - Extract attachments as separate FlowFiles - Default false.
Batch Size - Messages per processing cycle - Default 10.

Advanced Settings:
Incremental Fetch - Enable UID-based incremental fetching - Default false.
IMAP Filter - Server-side search criteria e.g., SINCE 01-Jan-2024.
Recursive Folder Search - Search in subfolders - Default false.
Max Recursion Depth - Maximum depth for recursive search - Default 5.
Max Message Size - Skip oversized messages - Default 100 MB.
Partial Fetch - Stream large messages in chunks - Default true.
Fetch Buffer Size - Buffer size for partial fetch - Default 100 KB.
Mark Messages as Read - Set SEEN flag after processing - Default false.
Delete Messages - Set DELETED flag after processing - Default false.
Max Retries - Maximum retry attempts - Default 3.
Retry Backoff - Base time between retries - Default 10 secs.
Connection Timeout - Timeout for server connection - Default 30 secs.
Reconnect Interval - Wait time before reconnecting - Default 10 secs.
Max Attachment Size - Maximum size per attachment - Default 50 MB.
Max Attachments Per Message - Maximum attachments to extract - Default 100.
State Folder Identifier - Unique ID for folder state - Default INBOX.

## Relationships

success - Successfully processed messages or headers.
attachments - Extracted attachments when enabled.
parse-error - Messages that were received but couldnt be parsed.
retry - Messages that failed due to temporary issues.
failure - Messages with critical unrecoverable errors.
original - Original triggering FlowFile if any.

## Attributes

Email Attributes:
email.subject - Email subject line.
email.from - Sender address.
email.to - Primary recipient.
email.cc - CC recipient.
email.received.date - Timestamp when received.
email.sent.date - Timestamp when sent.
email.flags - Message flags (SEEN, FLAGGED, etc).
email.size - Message size in bytes.
email.uid - Server UID for incremental fetching.
email.message.id - Message-ID header.
email.folder - Source folder path.
email.batch.id - Unique batch identifier.

Attachment Attributes:
attachment.filename - Attachment file name.
attachment.size - Attachment size.
attachment.mime.type - MIME type.
email.parent.message.id - Parent email Message-ID.

Error Attributes:
error.type - Error classification.
error.message - Detailed error message.
retry.count - Number of retry attempts.

## Usage Examples

Basic Email Ingestion:
1. Host Name = imap.gmail.com
2. Port = 993
3. Username = user@gmail.com
4. Password = app-password
5. Folder = INBOX
6. Fetch Mode = Headers Only
7. Include Headers = true
8. Incremental Fetch = true

Incremental Fetch with Filtering:
1. IMAP Filter = SINCE "01-Jan-2024" UNSEEN
2. Incremental Fetch = true
3. State Folder Identifier = INBOX

The processor automatically combines: UID ${lastUID}:* SINCE 01-Jan-2024 UNSEEN

Attachment Extraction:
1. Fetch Mode = Attachments Only
2. Include Attachments = true
3. Max Attachment Size = 25 MB
4. Max Attachments Per Message = 10

Attachments go to attachments relationship, email metadata to success.

## Performance Tuning

For High-Volume Mailboxes:
Batch Size = 50.
Partial Fetch = true.
Fetch Buffer Size = 500 KB.
Max Message Size = 50 MB.
Incremental Fetch = true.

For Memory-Constrained Environments:
Batch Size = 5.
Partial Fetch = true.
Fetch Buffer Size = 100 KB.
Max Message Size = 10 MB.

For Real-Time Processing:
Reconnect Interval = 5 secs.
Connection Timeout = 10 secs.
Incremental Fetch = true.

## Troubleshooting

Common Issues:

Connection Refused:
Verify host and port.
Check network connectivity.
Ensure IMAP access is enabled.

Authentication Failed:
For Gmail use App Password not regular password.
For OAuth2 verify token provider configuration.
Check username format usually full email.

No Messages Found:
Verify folder name exists.
Check IMAP filter syntax.
Enable debug logging to see raw IMAP communication.

Memory Issues:
Enable Partial Fetch.
Reduce Batch Size.
Decrease Max Message Size.

Enable Debug Logging:
Add to conf/logback.xml:
<logger name="org.custom.processors.CustomIMAP" level="DEBUG"/>

## Building from Source

Clone repository:
git clone [https://github.com/julia-per/nifi-processor.git]
cd nifi-processor

Build with tests:
mvn clean install

Build without tests:
mvn clean install -DskipTests

Install to local Maven repository:
mvn clean install

Project structure:
nifi-custom-processors - Processor implementation
  src/main/java/org/custom/processors
    CustomIMAP.java
    CustomIMAPConnection.java
    CustomIMAPMessageProcessor.java
    IMAPFilterParser.java
  src/main/resources/META-INF/services/org.apache.nifi.processor.Processor
nifi-custom-imap-nar - NAR packaging

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Author

Created by Julia P. as part of own development portfolio.

Happy data flowing!
