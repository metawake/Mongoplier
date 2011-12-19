# Sample usage (default hostname and post used):
# mongoplier.py --source collection --database testdb --collection test_collection --copy_type copy_one --nrepeats 1
# finds 1st document in a collection and fills it into collection "nrepeats" times.

# Advanced usage (+hostname specified, +copy of all documents N times, instead of just first one)
# mongoplier.py --host localhost --port 27017 --source collection --database testdb --collection test_collection --copytype copy_one --nrepeats 1
# takes the whole collection data and fills it into collection "nrepeats" times

import argparse
import pymongo

class Mongoplier:

    args = None
    DEFAULTS = {
        'source' : 'collection',
        'host': 'localhost',
        'port': 27017,
        'database': 'test',
        'collection': 'test',
        'nrepeats': 1,
        'copy_type': 'copy_one',
    }
    connection = None
    
    def __init__(self):
        self.get_args()
        
    def prepare_db(self):    
        self.connection = pymongo.Connection(self.args.host, self.args.port)
        self.db = self.connection[self.args.database]
        
    def finalize_db(self):          
        self.connection.disconnect()
        
    def get_args(self):
        parser = argparse.ArgumentParser(description='Fill mongodb with test data.')
        parser.add_argument('--source', metavar='source_name', type=str, 
                            default=self.DEFAULTS.get('source'),
                            help='json filename or collection name as source of data')
        
        parser.add_argument('--host', metavar='host_name', type=str, 
                    default=self.DEFAULTS.get('host'),
                    help='host name')

        parser.add_argument('--port', metavar='port', type=str, 
                    default=self.DEFAULTS.get('port'),
                    help='port number')

        parser.add_argument('--database', metavar='database_name', type=str, 
                            default=self.DEFAULTS.get('database'),
                            help='database name')
        
        parser.add_argument('--collection', metavar='collection_name', type=str, 
                            default=self.DEFAULTS.get('collection'),
                            help='collection name')
        
        parser.add_argument('--nrepeats', metavar='number_of_repeats', type=str, 
                            default=self.DEFAULTS.get('nrepeats'),
                            help='The number of repeats/copies to make')
        parser.add_argument('--copy_type', metavar='copy_type', type=str, 
                            default=self.DEFAULTS.get('copy_type'),
                            help='Copy 1st document or all data: copy_one or copy_all')

        self.args = parser.parse_args()

    def copy_one(self, collection, nrepeat):
        record_to_copy = self.db[collection].find_one();
        if not record_to_copy:
            raise Exception("No documents found, can't copy anything.")
            
        del record_to_copy["_id"]
        
        from datetime import datetime
        start = datetime.now()
        for i in range (0, nrepeat):
            self.db[collection].insert(record_to_copy, safe=True, manipulate=False)
        end = datetime.now()
        print "\n Result time = %s" % (end - start).seconds  
        print "\nDone!\nCollection %s: inserted %d copies of \n %s" % (self.args.collection, nrepeat, str(record_to_copy))   
        
    def copy_all(self, collection, nrepeat):
        records_to_copy = self.db[collection].find();
        if not records_to_copy:
            raise Exception("No documents found, can't copy anything.")
         
        insertable_collection = []
        for record in records_to_copy:
            del record["_id"]
            insertable_collection.append(record) 
            
        from datetime import datetime
        start = datetime.now()
        for i in range (0, nrepeat):
            self.db[collection].insert(insertable_collection, safe=True, manipulate=False)
        end = datetime.now()
        print "\n Result time = %s" % (end - start).seconds
          
        print "Done!\nCollection %s: inserted %d copies of original collection, total inserted records=%s" % (self.args.source, nrepeat, len(insertable_collection)* nrepeat)   
       

    def run(self):
        
        self.prepare_db()
        if self.args.copy_type == 'copy_one':
            self.copy_one(self.args.collection, int(self.args.nrepeats))
        if self.args.copy_type == 'copy_all':
            self.copy_all(self.args.collection, int(self.args.nrepeats))
                
        self.finalize_db()

# Entry point of the application
try:
    app = Mongoplier()
    app.run()
except Exception as e:
    print e

