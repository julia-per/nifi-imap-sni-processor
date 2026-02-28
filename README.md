# Custom IMAP Processor for Apache NiFi

A production-ready, streaming IMAP processor for Apache NiFi that efficiently fetches and processes emails with minimal memory footprint. Designed for high-volume email ingestion pipelines.

## Features

Core Capabilities:
<br>Streaming Architecture - Processes emails in configurable batches without loading all messages into memory
<br>Incremental Fetching - Remembers last processed UIDs per folder, only new messages on subsequent polls
<br>Dual Authentication - Supports both password-based and OAuth2 (XOAUTH2) authentication
<br>Server-Side Filtering - IMAP search criteria combined with incremental UID tracking
<br>Recursive Folder Traversal - Search through subfolders with configurable depth limits

Memory Management:
<br>Partial Fetch - Streams large messages in chunks to prevent OOM errors
<br>Configurable Buffer Size - Control memory usage per message
<br>Size Limits - Enforce maximum message and attachment sizes
<br>Batch Processing - Process messages in small, controlled batches

Error Handling:
<br>Retry Logic - Exponential backoff for transient failures
<br>Dead Letter Queue - Route unprocessable messages to failure relationships
<br>Comprehensive Error Attributes - Detailed error information for debugging

Attachment Processing:
<br>Separate FlowFiles - Extract attachments as individual FlowFiles
<br>Configurable Limits - Control max attachment size and count per message
<br>MIME Type Detection - Preserves attachment metadata
<br>Parent Message Reference - Link attachments back to source email

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

Apache NiFi Version 2.7.0
<br>Java JDK 21
<br>Maven 3.6 or higher for building from source
<br>IMAP Server any RFC-compliant IMAP server (Gmail, Outlook, Exchange, Dovecot, etc.)

## Installation

Option 1:
1. Download the latest NAR file [https://github.com/julia-per/nifi--imap-sni-processor/releases/tag/latest]
2. Copy the NAR file to NiFi's extensions directory:
   cp nifi-custom-nar-*.nar /path/to/nifi/extensions/
3. Restart NiFi:
   /path/to/nifi/bin/nifi.cmd start

Option 2:
1. git clone [https://github.com/julia-per/nifi-imap-sni-processor.git]
2. cd nifi-processor
3. mvn clean install

The NAR file will be in: nifi-custom-nar/target/nifi-custom-nar-*.nar

After installation, find "CustomIMAP" in the NiFi processor palette.

## Configuration

Basic Settings:
<br>Host Name - IMAP server address (supports domains, IPv4, IPv6) - Required
<br>SNI Hostname - Optional server name for TLS extension (supports domains, IPv4, IPv6)
<br>Port - Server port - Default 993 - Required
<br>Username - Email account username - Required
<br>Folder - Folder to read from - Default INBOX - Required

Authentication:
<br>Authorization Mode - Choose between Use Password or Use OAuth2 - Required
<br>Password - Password when using password mode - Required for password mode
<br>OAuth2 Access Token Provider - Controller service when using OAuth2 - Required for OAuth2 mode

Fetch Options:
<br>Fetch Mode - Headers Only, Full Message, or Attachments Only - Default Full Message
<br>Include Headers - Add email headers as attributes - Default true
<br>Include Attachments - Extract attachments as separate FlowFiles - Default false
<br>Batch Size - Messages per processing cycle - Default 10

Advanced Settings:
<br>Incremental Fetch - Enable UID-based incremental fetching - Default false
<br>IMAP Filter - Server-side search criteria e.g., SINCE 01-Jan-2024
<br>Recursive Folder Search - Search in subfolders - Default false
<br> Recursion Depth - Maximum depth for recursive search - Default 5
<br>Max Message Size - Skip oversized messages - Default 100 MB
<br> Fetch - Stream large messages in chunks - Default true
<br>Fetch Buffer Size - Buffer size for partial fetch - Default 100 KB
<br>Mark Messages as Read - Set SEEN flag after processing - Default false
<br>Delete Messages - Set DELETED flag after processing - Default false
<br>Max Retries - Maximum retry attempts - Default 3
<br>Retry Backoff - Base time between retries - Default 10 secs
<br>Connection Timeout - Timeout for server connection - Default 30 secs
<br>Reconnect Interval - Wait time before reconnecting - Default 10 secs
<br>Max Attachment Size - Maximum size per attachment - Default 50 MB
<br>Max Attachments Per Message - Maximum attachments to extract - Default 100
<br>State Folder Identifier - Unique ID for folder state - Default INBOX

## Relationships

success - Successfully processed messages or headers
<br>attachments - Extracted attachments when enabled
<br>parse-error - Messages that were received but couldn't be parsed
<br>retry - Messages that failed due to temporary issues
<br>failure - Messages with critical unrecoverable errors
<br>original - Original triggering FlowFile if any

## Attributes

Email Attributes:
<br>email.subject - Email subject line
<br>email.from - Sender address
<br>email.to - Primary recipient
<br>email.cc - CC recipient
<br>email.received.date - Timestamp when received
<br>email.sent.date - Timestamp when sent
<br>email.flags - Message flags (SEEN, FLAGGED, etc)
<br>email.size - Message size in bytes
<br>email.uid - Server UID for incremental fetching
<br>email.message.id - Message-ID header
<br>email.folder - Source folder path
<br>email.batch.id - Unique batch identifier

Attachment Attributes:
<br>attachment.filename - Attachment file name
<br>attachment.size - Attachment size
<br>attachment.mime.type - MIME type
<br>email.parent.message.id - Parent email Message-ID

Error Attributes:
<br>error.type - Error classification
<br>error.message - Detailed error message
<br>retry.count - Number of retry attempts

## Usage Examples

Basic Email Ingestion:
<br>Host Name = imap.gmail.com
<br>Port = 993
<br>Username = user@gmail.com
<br>Password = app-password
<br>Folder = INBOX
<br>Fetch Mode = Headers Only
<br>Include Headers = true
<br>Incremental Fetch = true

Incremental Fetch with Filtering:
<br>IMAP Filter = SINCE "01-Jan-2024" UNSEEN
<br>Incremental Fetch = true
<br>State Folder Identifier = INBOX

The processor automatically combines: UID ${lastUID}:* SINCE 01-Jan-2024 UNSEEN

Attachment Extraction:
<br>Fetch Mode = Attachments Only
<br>Include Attachments = true
<br>Max Attachment Size = 25 MB
<br>Max Attachments Per Message = 10

Attachments go to attachments relationship, email metadata to success.

## Performance Tuning

For High-Volume Mailboxes:
<br>Batch Size = 50
<br>Partial Fetch = true
<br>Fetch Buffer Size = 500 KB
<br>Max Message Size = 50 MB
<br>Incremental Fetch = true

For Memory-Constrained Environments:
<br>Batch Size = 5
<br>Partial Fetch = true
<br>Fetch Buffer Size = 100 KB
<br>Max Message Size = 10 MB

For Real-Time Processing:
<br>Reconnect Interval = 5 secs
<br>Connection Timeout = 10 secs
<br>Incremental Fetch = true

## Troubleshooting

Common Issues:

Connection Refused:
<br>Verify host and port.
<br>Check network connectivity.
<br>Ensure IMAP access is enabled.

Authentication Failed:
<br>For Gmail use App Password not regular password.
<br>For OAuth2 verify token provider configuration.
<br>Check username format usually full email.

No Messages Found:
<br>Verify folder name exists.
<br>Check IMAP filter syntax.
<br>Enable debug logging to see raw IMAP communication.

Memory Issues:
<br>Enable Partial Fetch.
<br>Reduce Batch Size.
<br>Decrease Max Message Size.

Enable Debug Logging:
<br>Add to conf/logback.xml:
<br><logger name="org.custom.processors.CustomIMAP" level="DEBUG"/>

## Building from Source

Clone repository:
<br>git clone [https://github.com/julia-per/nifi-imap-sni-processor.git]
<br>cd nifi-processor

Build with tests:
<br>mvn clean install

Build without tests:
<br>mvn clean install -DskipTests

Install to local Maven repository:
<br>mvn clean install

Project structure:
<br>nifi-custom-processors - Processor implementation
<br> -src/main/java/org/custom/processors
<br>  ---CustomIMAP.java
<br>  ---CustomIMAPConnection.java
<br> ---CustomIMAPMessageProcessor.java
<br>---IMAPFilterParser.java
<br> -src/main/resources/META-INF/services/org.apache.nifi.processor.Processor
<br>nifi-custom-imap-nar - NAR packaging

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## Author

Created by Julia P. as part of own development portfolio.

Happy data flowing!
