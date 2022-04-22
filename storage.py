
import os
from datetime import datetime


class Storage:

  localLastIdPath = './assets/last_ceb_filename.txt'
  localCurrentPath = './assets/ceb_current.pdf'

  def __init__(self, start_datetime, logger):
    self.start_datetime = start_datetime
    self.logger = logger

  def myfunc(abc):
    print("Hello my name is " + abc.name)

  def validate_doc_id(self, docId):
    # Check if the file is present and their content is not currentId
    if os.path.isfile(self.localLastIdPath):
        text_file = open(self.localLastIdPath, "r")
        lastId = text_file.read()
        text_file.close()
        if (lastId == docId):
            return False
    # Is valid: return True
    return True

  def get_local_doc_path(self):
    return self.localCurrentPath

  def get_remote_doc_name(self):
    formatedDate = self.start_datetime.strftime("%y-%m-%d_%H-%M-%S")
    docName = "ceb_%s.pdf" % (formatedDate)
    return docName

  def save_doc_id(self, docId):
    # Override the content of localLastIdPath for next validate_target_id()
    with open(self.localLastIdPath,'w') as f:
        f.write(docId)
