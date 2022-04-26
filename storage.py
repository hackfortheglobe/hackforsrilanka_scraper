from distutils.command.upload import upload
from ftplib import FTP
import os


class Storage:

  localAssetFolder = './assets/'
  lastFileName = 'last_ceb_filename.txt'
  currentFileName = 'ceb_current.pdf'

  remote_host = "ftp.rafaco.es"
  remote_username = "scraper@hackforsrilanka.rafaco.es"
  remote_password = os.environ.get('FTP_PASSWORD')
  remoteDebugValue = 0

  def __init__(self, start_datetime, logger, dev_mode):
    self.start_datetime = start_datetime
    self.logger = logger
    self.dev_mode = dev_mode


  def get_local_last_path(self):
      return self.localAssetFolder + self.lastFileName

  def get_local_doc_path(self):
    return self.localAssetFolder + self.currentFileName

  def get_remote_doc_name(self):
    formatedDate = self.start_datetime.strftime("%y-%m-%d_%H-%M-%S")
    docName = "ceb_%s.pdf" % (formatedDate)
    return docName


  def download_last_processed(self):
    if not self.dev_mode:
      self.download_file(self.lastFileName, self.get_local_last_path())
    return

  def validate_doc_id(self, docId):
    # Check if the file is present and their content is not the currentId
    if os.path.isfile(self.get_local_last_path()):
        text_file = open(self.get_local_last_path(), "r")
        lastId = text_file.read()
        text_file.close()
        if (lastId == docId):
            return False
    # Is valid: return True
    return True

  
  def save_processed(self, docId):
    self.save_doc_id(docId)
    self.save_doc_file()
  
  def save_doc_id(self, docId):
    # Override the content of localLastIdPath for next validate_target_id()
    with open(self.get_local_last_path(), 'w') as f:
        f.write(docId)

    if not self.dev_mode:
      self.upload_file(self.get_local_last_path(), self.lastFileName)

  def save_doc_file(self):
    localFilePath = self.get_local_doc_path()
    remoteFilePath = self.get_remote_doc_name()

    if not self.dev_mode:
      self.upload_file(localFilePath, remoteFilePath)



  def download_file(self, remoteFilePath, localFilePath):
    session = FTP(self.remote_host)
    session.encoding = "utf-8"
    session.set_debuglevel(self.remoteDebugValue)
    self.logger.info("Connecting with " + self.remote_host)
    session.login(self.remote_username, self.remote_password)

    self.logger.info("Preparing local file " + localFilePath)
    localFile = open(localFilePath, 'wb')
    self.logger.info("Downloading " + remoteFilePath + " to " + localFilePath)
    session.retrbinary('RETR ' + remoteFilePath, localFile.write, 1024)
    localFile.close()
    session.quit()

  def upload_file(self, localFilePath, remoteFilePath):
    session = FTP(self.remote_host)
    session.encoding = "utf-8"
    session.set_debuglevel(self.remoteDebugValue)
    self.logger.info("Connecting with " + self.remote_host)
    session.login(self.remote_username, self.remote_password)
    self.logger.info("Reading local file " + localFilePath)
    fileToSend = open(localFilePath, 'rb')
    self.logger.info("Sending to " + remoteFilePath)
    session.storbinary('STOR ' + remoteFilePath, fileToSend)
    fileToSend.close()
    self.logger.info("Closing connection")
    session.quit()
