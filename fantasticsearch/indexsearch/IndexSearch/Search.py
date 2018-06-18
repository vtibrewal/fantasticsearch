import os, json, logging

from django.conf import settings
from collections import OrderedDict
from Utility import Utility

class Search():
    def __init__(self):
        ''' Will use this class for searching '''
    
    def searchJSON(self, query_term, query_type, query_field):
        utility = Utility()
        utility.dir_change(settings.DATA_DIR)
        logging.basicConfig(filename=os.path.join(settings.DATA_DIR,'indexsearch.log'),level=logging.DEBUG)
        results = hashes = []
        hashes = utility.get_hashes(query_term, query_type, query_field)
        if hashes == None:
            logging.info('No document matched the query. Returning 404.')
            return None, 404
        logging.info('Document hashes returned %s' %(','.join(hashes)))
        for hash in hashes:
            parts = '/'.join([hash[i:i+4] for i in range(0, 28, 4)])
            doc_path = os.path.join(settings.DOC_DIR, parts)
            utility.dir_change(doc_path)    
            with open(hash) as file:  
                data = json.loads(file.read(), object_pairs_hook=OrderedDict)
            results.append(data)
        logging.info('Returning 200 with results %s' %(str(results)))
        return results, 200

if __name__ == "__main__":
    search = Search()
    msg,code = search.searchJSON("","","")
