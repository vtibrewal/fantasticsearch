import os, re, sys, json, math, pickle, hashlib, logging, subprocess

from django.conf import settings

class Utility():
    def __init__(self):
        ''' Constructor for this class. '''

    def dir_change(self, dir_path):
        ''' Function to create and change directory to dir_path'''
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
            except OSError:
                logging.warning("Make of the directory %s failed" % dir_path)
        try:
            os.chdir(dir_path)
        except OSError:
            logging.warning("Change to the directory %s failed" % dir_path)

    def indexed_file_count(self):
        ''' Function to count total number of files in our indexed database'''
        cmd = "find "+settings.DOC_DIR+" -type f"
        cmd = cmd.split()
        resp = subprocess.check_output(cmd)
        file_count = len(resp.split('\n')) - 1
        logging.info("Command %s returned file count to be %d" %(''.join(cmd), file_count))
        return file_count

    def tokenize(self, string_stream):
        ''' Function to remove all special characters, convert to lower_case and tokenize'''
        sanitized = re.sub('[^A-Za-z0-9]+', ' ', string_stream)
        normalized = sanitized.lower()
        logging.info("Converted string %s to %s" %(string_stream, normalized))
        return normalized.split()
    
    def create_local_index(self, string_stream):
        ''' Function to create a dictionary of all unique words and their indices in a string'''
        tokens = self.tokenize(string_stream)
        index = {}
        for i in range(len(tokens)):
            token = tokens[i]
            if token not in index.keys():
                index[token] = []
            index[token].append(i)
        logging.info("Created a dictionary for string %s as %s" %(string_stream, str(index)))
        return index

    def read_index(self, field):
        ''' Function to read the inverted index based on field value'''
        index_file = field+'.index'
        self.dir_change(settings.DATA_DIR)
        if not os.path.exists(index_file) or os.stat(index_file).st_size == 0:
            index_fh = open(index_file, 'wb')
            pickle.dump(settings.INDEX_INITIAL, index_fh)
            index_fh.close()
        index_fh = open(index_file, 'rb')
        index = pickle.load(index_fh)
        index_fh.close()
        logging.info('Returning the index %s for field %s' %(str(index), field))
        return index

    def write_index(self, field, index_value):
        ''' Function to write the inverted index to a file based on field value'''
        index_file = field+'.index'
        self.dir_change(settings.DATA_DIR)
        index_fh = open(index_file, 'wb')
        pickle.dump(index_value, index_fh)
        index_fh.close()
        logging.info('Opened the file %s and dumped %s' %(index_file, str(index_value)))

    def create_index(self, document, field, hash):
        ''' Function to create the inverted index based on field and store the file_hash along with indices'''
        local_index = self.create_local_index(document[field])
        index = self.read_index(field)
        for word in local_index.keys():
            if word not in index[word[0]].keys():
                index[word[0]][word] = {}
            index[word[0]][word][hash] = local_index[word]
        self.write_index(field, index)

    def calculate_tf_idf(self, hash, hash_index):
        ''' Function to calculate the terf frequency and inverse document frequency for a given token in a given file'''
        total_docs = self.indexed_file_count()      #Total file count
        word_in_docs = len(hash_index)              #Number of files containing that word
        word_in_docs = 1 if word_in_docs == 0 else word_in_docs     #Normalizing to avoid division by 0
        div = total_docs/float(word_in_docs)
        idf = math.log(div, 10)
        tf = len(hash_index[hash]) if hash in hash_index.keys() else 0
        tf_idf = idf*tf
        logging.info('tf_idf calculted in file %s is %s with tf being %s' %(hash, str(tf_idf), str(tf)))
        return tf, tf_idf

    def update_cache(self, query_tokens, query_type, query_field, hashes):
        ''' Function to update the cache with latest results'''
        cache_file = settings.CACHE_FILE
        self.dir_change(settings.DATA_DIR)
        cache_fh = open(cache_file, 'rb')
        cache = pickle.load(cache_fh)
        cache_fh.close()
        while os.stat(cache_file).st_size > 10000:   #Clean Cache if the size exceeds 10 KBs
            cache.popitem()
            cache_fh = open(cache_file, 'wb')
            pickle.dump(cache, cache_fh)
            cache_fh.close()
        query_tokens = ' '.join(query_tokens)
        cache[query_tokens] = {}
        cache[query_tokens][query_type] = {}
        cache[query_tokens][query_type][query_field] = hashes
        cache_fh = open(cache_file, 'wb')
        pickle.dump(cache, cache_fh)
        cache_fh.close()
        logging.info('Updated cache with values %s %s %s %s' %(query_tokens, query_type, query_field, ''.join(hashes)))

    def lookup_cache(self, query_tokens, query_type, query_field):
        ''' Function to lookup the cache'''
        cache_file = settings.CACHE_FILE
        self.dir_change(settings.DATA_DIR)
        if not os.path.exists(cache_file) or os.stat(cache_file).st_size == 0:
            cache_fh = open(cache_file, 'wb')
            pickle.dump({}, cache_fh)
            cache_fh.close()
        cache_fh = open(cache_file, 'rb')
        cache = pickle.load(cache_fh)
        cache_fh.close()
        query_tokens = ' '.join(query_tokens)
        if query_tokens in cache.keys():
            if query_type in cache[query_tokens].keys():
                if query_field in cache[query_tokens][query_type].keys():
                    hashes = cache[query_tokens][query_type][query_field]
                    logging.info('Cache hit, found the following files %s in cache for values %s %s %s' %(''.join(hashes), query_tokens, query_type, query_field))
                    return hashes
        logging.info('Hard Luck! Nothing found in cache')
        return []

    def get_hashes(self, query_term, query_type, query_field):
        ''' Function to search based on the parameters given'''
        hashes = []
        tf_idf_dict = {}
        query_tokens = self.tokenize(query_term)            #Tokenize query terms
        hashes = self.lookup_cache(query_tokens, query_type, query_field)   #Try luck in cache
        if hashes:
            return hashes
        index = self.read_index(query_field)    #Read the inverted index
        for token in query_tokens:              
            if token in index[token[0]].keys(): #Select the hashes which match any of the query term
                hash = index[token[0]][token]
                hashes.extend(hash.keys())
        result_set = list(set(hashes))      #Eliminate duplicates that might have come due to multiple query terms in same hash
        logging.info('These hashes %s contain at least one of these query terms %s' %(''.join(result_set), ''.join(query_tokens))) 
        for hash in result_set:             #Rank them based on their tf_idf score
            score = 0
            flag = True
            previous_token_indices = []
            for token in query_tokens:
                tf = tf_idf = 0
                if token in index[token[0]].keys():     #Check if token is present in hash
                    tf, tf_idf = self.calculate_tf_idf(hash, index[token[0]][token])
                if  query_type == 'phrase':     #Special handling for phrase queries
                    if tf == 0:                 #Eliminate all hashes where any of the search token is missing
                        flag = False
                        break
                    else:
                        token_indices = index[token[0]][token][hash]    #Get the indices of token and eliminate the hash if they are not in order
                        if not previous_token_indices:
                            previous_token_indices = token_indices
                        else:
                            accepted_token_indices = [x+1 for x in previous_token_indices] 
                            common_indices = list(set(accepted_token_indices).intersection(token_indices))
                            if not common_indices:
                                flag = False
                                break
                score += tf_idf
            if flag:
                tf_idf_dict[hash] = score
        hashes = []
        for key, value in sorted(tf_idf_dict.iteritems(), key=lambda (k,v): (v,k)):
            hashes.insert(0,key)
        self.update_cache(query_tokens, query_type, query_field, hashes) # Update the cache with results
        return None if not hashes else hashes

if __name__ == "__main__":
    utiltiy = Utility()