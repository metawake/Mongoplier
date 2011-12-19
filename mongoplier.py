import argparse
import pymongo
import uuid

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
        'random_field': 'organization_id',
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

        parser.add_argument('--random_field', metavar='random_field', type=str, 
                            default=self.DEFAULTS.get('random_field'),
                            help='A field where random UUID string is populated')


        self.args = parser.parse_args()

    def copy_one(self, collection, nrepeat):
        record_to_copy = self.db[collection].find_one();
        if not record_to_copy:
            raise Exception("No documents found, can't copy anything.")
            
        record_to_copy = self.transform_source(record_to_copy)
        
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
            record = self.transform_source(record)
            insertable_collection.append(record) 
            
        from datetime import datetime
        start = datetime.now()
        for i in range (0, nrepeat):
            self.db[collection].insert(insertable_collection, safe=True, manipulate=False)
        end = datetime.now()
        print "\n Result time = %s" % (end - start).seconds
          
        print "Done!\nCollection %s: inserted %d copies of original collection, total inserted records=%s" % (self.args.source, nrepeat, len(insertable_collection)* nrepeat)   
       
    def transform_source(self, record_to_copy):
        del record_to_copy["_id"]
        
        if "random_field" in self.args:
            record_to_copy[self.args.random_field] = uuid.uuid4()
        
        return record_to_copy 

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

