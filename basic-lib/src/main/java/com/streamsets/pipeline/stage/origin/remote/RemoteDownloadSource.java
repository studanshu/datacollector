/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.remote;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.ObjectLengthException;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.vfs2.FileNotFoundException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;

import static com.streamsets.pipeline.stage.origin.lib.DataFormatParser.DATA_FORMAT_CONFIG_PREFIX;

public class RemoteDownloadSource extends BaseSource {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteDownloadSource.class);
  private static final String OFFSET_DELIMITER = "::";
  private static final String MINUS_ONE = "-1";
  private static final String CONF_PREFIX = "conf.";

  private final RemoteDownloadConfigBean conf;
  private final File knownHostsFile;
  private final File errorArchive;
  private final byte[] moveBuffer;

  private final NavigableSet<RemoteFile> fileQueue = new TreeSet<>(new Comparator<RemoteFile>() {
    @Override
    public int compare(RemoteFile f1, RemoteFile f2) {
      if (f1.lastModified < f2.lastModified) {
        return -1;
      } else if (f1.lastModified > f2.lastModified) {
        return 1;
      } else {
        return f1.filename.compareTo(f2.filename);
      }
    }
  });

  private URI remoteURI;
  private volatile Offset currentOffset = null;
  private InputStream currentStream = null;
  private FileObject remoteDir;
  private DataParser parser;
  private final FileSystemOptions options = new FileSystemOptions();
  private ErrorRecordHandler errorRecordHandler;

  public RemoteDownloadSource(RemoteDownloadConfigBean conf) {
    this.conf = conf;
    if (conf.knownHosts != null && !conf.knownHosts.isEmpty()) {
      this.knownHostsFile = new File(conf.knownHosts);
    } else {
      this.knownHostsFile = null;
    }
    if (conf.errorArchiveDir != null && !conf.errorArchiveDir.isEmpty()) {
      this.errorArchive = new File(conf.errorArchiveDir);
      this.moveBuffer = new byte[64 * 1024];
    } else {
      this.errorArchive = null;
      this.moveBuffer = null;
    }
  }

  @Override
  public List<ConfigIssue> init() {

    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.REMOTE.getLabel(),
        DATA_FORMAT_CONFIG_PREFIX,
        issues
    );

    try {
      this.remoteURI = new URI(conf.remoteAddress);
    } catch (Exception ex) {
      issues.add(getContext().createConfigIssue(
          Groups.REMOTE.getLabel(), CONF_PREFIX + "remoteAddress", Errors.REMOTE_01, conf.remoteAddress));
    }

    try {
      FileSystemManager fsManager = VFS.getManager();
      // If password is not specified, add the username to the URI
      switch (conf.auth) {
        case PRIVATE_KEY:
          String schemeBase = remoteURI.getScheme() + "://";
          remoteURI = new URI(schemeBase + conf.username + "@" + remoteURI.toString().substring(schemeBase.length()));
          File privateKeyFile = new File(conf.privateKey);
          if (!privateKeyFile.exists() || !privateKeyFile.isFile() || !privateKeyFile.canRead()) {
            issues.add(getContext().createConfigIssue(
                Groups.CREDENTIALS.getLabel(), CONF_PREFIX + "privateKey", Errors.REMOTE_10, conf.privateKey));
          } else {
            if (!remoteURI.getScheme().equals("sftp")) {
              issues.add(getContext().createConfigIssue(
                  Groups.CREDENTIALS.getLabel(), CONF_PREFIX + "privateKey", Errors.REMOTE_11));
            } else {
              SftpFileSystemConfigBuilder.getInstance().setIdentities(options, new File[]{privateKeyFile});
              if (conf.privateKeyPassphrase != null && !conf.privateKeyPassphrase.isEmpty()) {
                SftpFileSystemConfigBuilder.getInstance()
                    .setUserInfo(options, new SDCUserInfo(conf.privateKeyPassphrase));
              }
            }
          }
          break;
        case PASSWORD:
          StaticUserAuthenticator auth = new StaticUserAuthenticator(
              remoteURI.getHost(), conf.username, conf.password);
          DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(options, auth);
          break;
        default:
          break;
      }

      if (remoteURI.getScheme().equals("ftp")) {
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(options, true);
        FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, conf.userDirIsRoot);
        if (conf.strictHostChecking) {
          issues.add(getContext().createConfigIssue(
              Groups.CREDENTIALS.getLabel(), CONF_PREFIX + "strictHostChecking", Errors.REMOTE_12));
        }
      }

      if (remoteURI.getScheme().equals("sftp")) {
        SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, conf.userDirIsRoot);
        if (conf.strictHostChecking) {
          if (knownHostsFile != null) {
            if (knownHostsFile.exists() && knownHostsFile.isFile() && knownHostsFile.canRead()) {
              SftpFileSystemConfigBuilder.getInstance().setKnownHosts(options, knownHostsFile);
              SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(options, "yes");
            } else {
              issues.add(getContext().createConfigIssue(
                  Groups.CREDENTIALS.getLabel(), CONF_PREFIX + "knownHosts", Errors.REMOTE_06, knownHostsFile));
            }

          } else {
            issues.add(getContext().createConfigIssue(
                Groups.CREDENTIALS.getLabel(), CONF_PREFIX +"strictHostChecking", Errors.REMOTE_07));
          }
        } else {
          SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(options, "no");
        }
      }

      if (issues.isEmpty()) {
        // To ensure we can connect, else we fail validation.
        remoteDir = fsManager.resolveFile(remoteURI.toString(), options);
      }

    } catch (FileSystemException | URISyntaxException ex) {
      issues.add(getContext().createConfigIssue(
          Groups.REMOTE.getLabel(), CONF_PREFIX + "remoteAddress", Errors.REMOTE_08, conf.remoteAddress));
      LOG.error("Error trying to login to remote host", ex);
    }
    return issues;
  }


  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    // Just started up, currentOffset has not yet been set.
    // This method returns MINUS_ONE when only no events have ever been read
    if (currentOffset == null
        && lastSourceOffset != null
        && !lastSourceOffset.isEmpty()
        && !lastSourceOffset.equals(MINUS_ONE)) {
      currentOffset = new Offset(lastSourceOffset);
    }
    String offset = MINUS_ONE;
    RemoteFile next = null;
    try {
      // Time to read the next file
      if (currentStream == null) {
        Optional<RemoteFile> nextOpt = getNextFile();
        if (nextOpt.isPresent()) {
          next = nextOpt.get();
          LOG.info("Started reading file: " + next.filename);
          currentStream = next.remoteObject.getContent().getInputStream();
          // When starting up, reset to offset 0 of the file picked up for read only if:
          // -- we are starting up for the very first time, hence current offset is null
          // -- or the next file picked up for reads is not the same as the one we left off at (because we may have completed that one).
          if (currentOffset == null || !currentOffset.fileName.equals(next.filename)) {
            currentOffset = new Offset(next.remoteObject.getName().getBaseName(),
                next.remoteObject.getContent().getLastModifiedTime(), 0L);
          }
          parser = conf.dataFormatConfig.getParserFactory().getParser(
              currentOffset.offsetStr, currentStream, String.valueOf(currentOffset.offset));
        } else {
          if (currentOffset == null) {
            return offset;
          } else {
            return currentOffset.offsetStr;
          }
        }
      }
      offset = addRecordsToBatch(maxBatchSize, batchMaker);
    } catch (IOException | DataParserException ex) {
      handleFatalException(ex, next);
    } finally {
      if (!MINUS_ONE.equals(offset) && currentOffset != null) {
        currentOffset.setOffset(Long.parseLong(offset));
      }
    }
    if (currentOffset != null) {
      return currentOffset.offsetStr;
    }
    return offset;
  }

  private String addRecordsToBatch(int maxBatchSize, BatchMaker batchMaker) throws IOException, StageException {
    String offset = MINUS_ONE;
    for (int i = 0; i < maxBatchSize; i++) {
      try {
        Record record = parser.parse();
        if (record != null) {
          batchMaker.addRecord(record);
          offset = parser.getOffset();
        } else {
          parser.close();
          parser = null;
          currentStream.close();
          currentStream = null;
          break;
        }
      } catch (ObjectLengthException ex) {
        errorRecordHandler.onError(Errors.REMOTE_02, currentOffset.fileName, offset, ex);
      }
    }
    return offset;
  }

  private void moveFileToError(RemoteFile fileToMove) {
    if (errorArchive != null) {
      int read;
      File errorFile = new File(errorArchive, fileToMove.filename);
      if (errorFile.exists()) {
        errorFile = new File(errorArchive, fileToMove.filename + "-" + UUID.randomUUID().toString());
        LOG.info(fileToMove.filename + " is being written out as " + errorFile.getName() +
            " as another file of the same name exists");
      }
      try (InputStream is = fileToMove.remoteObject.getContent().getInputStream();
          OutputStream os = new BufferedOutputStream(new FileOutputStream(errorFile))) {
        while ((read = is.read(moveBuffer)) != -1) {
          os.write(moveBuffer, 0, read);
        }
      } catch (Exception ex) {
        LOG.warn("Error while trying to write out error file to " + errorFile.getName());
      }
    }
  }

  private void handleFatalException(Exception ex, RemoteFile next) throws StageException {
    if (ex instanceof FileNotFoundException) {
      LOG.warn("File: " + next.filename + " was found in listing, but is not downloadable", ex);
    }
    if (ex instanceof ClosedByInterruptException || ex.getCause() instanceof ClosedByInterruptException) {
      //If the pipeline was stopped, we may get a ClosedByInterruptException while reading avro data.
      //This is because the thread is interrupted when the pipeline is stopped.
      //Instead of sending the file to error, publish batch and move one.
    } else {
      try {
        if (parser != null) {
          parser.close();
        }
      } catch (IOException ioe) {
        LOG.error("Error while closing parser", ioe);
      } finally {
        parser = null;
      }
      try {
        if (currentStream != null) {
          currentStream.close();
        }
      } catch (IOException ioe) {
        LOG.error("Error while closing stream", ioe);
      } finally {
        currentStream = null;
      }
      String exOffset;
      if (ex instanceof OverrunException) {
        exOffset = String.valueOf(((OverrunException) ex).getStreamOffset());
      } else {
        try {
          exOffset = (parser != null) ? parser.getOffset() : MINUS_ONE;
        } catch (IOException ex1) {
          exOffset = MINUS_ONE;
        }
      }
      switch (getContext().getOnErrorRecord()) {
        case DISCARD:
          break;
        case TO_ERROR:
          // we failed to produce a record, which leaves the input file in an unknown state.
          moveFileToError(next);
          break;
        case STOP_PIPELINE:
          if (currentOffset != null) {
            throw new StageException(Errors.REMOTE_04, currentOffset.fileName, exOffset, ex);
          } else {
            throw new StageException(Errors.REMOTE_05, ex);
          }
        default:
          throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
              getContext().getOnErrorRecord(), ex));
      }
    }
  }

  private Optional<RemoteFile> getNextFile() throws FileSystemException {
    if (fileQueue.isEmpty()) {
      queueFiles();
    }
    return Optional.fromNullable(fileQueue.pollFirst());
  }

  private void queueFiles() throws FileSystemException {
    remoteDir.refresh();
    for (FileObject remoteFile : remoteDir.getChildren()) {
      long lastModified = remoteFile.getContent().getLastModifiedTime();
      RemoteFile tempFile = new RemoteFile(remoteFile.getName().getBaseName(), lastModified, remoteFile);
      if (shouldQueue(tempFile)) {
        // If we are done with all files, the files with the final mtime might get re-ingested over and over.
        // So if it is the one of those, don't pull it in.
        fileQueue.add(tempFile);
      }
    }
  }

  private boolean shouldQueue(RemoteFile remoteFile) throws FileSystemException {
    // Case 1: We started up for the first time, so anything we see must be queued
    return currentOffset == null ||
        // We poll for new files only when fileQueue is empty, so we don't need to check if this file is in the queue.
        // The file can be in the fileQueue only if the file was already queued in this iteration -
        // which is not possible, since we are iterating through the children,
        // so this is the first time we are seeing the file.
        // Case 2: The file is newer than the last one we read/are reading
        ((remoteFile.lastModified > currentOffset.timestamp) ||
            // Case 3: The file has the same timestamp as the last one we read, but is lexicographically higher, and we have not queued it before.
            (remoteFile.lastModified == currentOffset.timestamp && remoteFile.filename.compareTo(currentOffset.fileName) > 0) ||
            // Case 4: It is the same file as we were reading, but we have not read the whole thing, so queue it again - recovering from a shutdown.
            remoteFile.filename.equals(currentOffset.fileName) && remoteFile.remoteObject.getContent().getSize() > currentOffset.offset);
  }


  private class RemoteFile {
    final String filename;
    final long lastModified;
    final FileObject remoteObject;

    RemoteFile(String filename, long lastModified, final FileObject remoteObject) {
      this.filename = filename;
      this.lastModified = lastModified;
      this.remoteObject = remoteObject;
    }
  }

  // Offset format: Filename::timestamp::offset. I miss case classes here.
  private class Offset {
    final String fileName;
    final long timestamp;
    private long offset;
    String offsetStr;

    Offset(String offsetStr) {
      String[] parts = offsetStr.split(OFFSET_DELIMITER);
      Preconditions.checkArgument(parts.length == 3);
      this.offsetStr = offsetStr;
      this.fileName = parts[0];
      this.timestamp = Long.parseLong(parts[1]);
      this.offset = Long.parseLong(parts[2]);
    }

    Offset(String fileName, long timestamp, long offset) {
      this.fileName = fileName;
      this.offset = offset;
      this.timestamp = timestamp;
      this.offsetStr = getOffsetStr();
    }

    void setOffset(long offset) {
      this.offset = offset;
      this.offsetStr = getOffsetStr();
    }

    private String getOffsetStr() {
      return fileName + OFFSET_DELIMITER + timestamp + OFFSET_DELIMITER + offset;
    }
  }

  private static class SDCUserInfo implements com.jcraft.jsch.UserInfo {

    private final String passphrase;

    SDCUserInfo(String passphrase) {
      this.passphrase = passphrase;
    }

    @Override
    public String getPassphrase() {
      return passphrase;
    }

    @Override
    public String getPassword() {
      return null;
    }

    @Override
    public boolean promptPassphrase(String message) {
      return true;
    }

    @Override
    public boolean promptYesNo(String message) {
      return false;
    }

    @Override
    public void showMessage(String message) {
    }

    @Override
    public boolean promptPassword(String message) {
      return false;
    }
  }

}
