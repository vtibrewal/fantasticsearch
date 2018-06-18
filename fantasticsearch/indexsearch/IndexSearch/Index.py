import os, json, hashlib, logging

from django.conf import settings
from Utility import Utility

class Index():
    def __init__(self):
        ''' Will use this class for indexing '''

    def indexJSON(self, doc):
        utility = Utility()
        utility.dir_change(settings.DATA_DIR)
        logging.basicConfig(filename=os.path.join(settings.DATA_DIR,'indexsearch.log'),level=logging.DEBUG)
        doc_data = json.dumps(doc)
        doc_md5 = hashlib.md5(doc_data).hexdigest()
        logging.info('Post request for indexing sent with data %s and md5 %s' %(doc_data, doc_md5))
        parts = '/'.join([doc_md5[i:i+4] for i in range(0, 28, 4)])
        doc_path = os.path.join(settings.DOC_DIR, parts)
        utility.dir_change(doc_path)
        for i in range(1000):
            if os.path.exists(doc_md5):
                with open(doc_md5) as file:
                    content = file.read()
                if content == doc_data:
                    logging.info('Document already in database. Returning 400.')
                    return ["Document already exists in the database", 400]
                else:
                    doc_md5 += str(i)
            else:
                logging.info('Writing %s to %s/%s' %(doc_data, doc_path, doc_md5))
                with open(doc_md5, 'w') as file:
                    file.write(doc_data)
                break
        title_index = utility.create_index(doc, 'title', doc_md5)
        data_index = utility.create_index(doc, 'data', doc_md5)
        logging.info('Document indexed successfully. Returning 200.')
        return ["Document indexed successfully. Total indexed files = "+str(utility.indexed_file_count()), 200]

if __name__ == "__main__":
    index = Index()
    msg,code = index.indexJSON("")
