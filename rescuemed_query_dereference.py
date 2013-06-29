'''
Links to data in other collections can be dereferenced by your application on your query results.
This way, you guarantee immediate consistency of your data. This is often what one wants.
In our data model, the collection for additional (concomitant) medication is linked to
data on rescue medication. The identifier needed for referencing is made up of patient Id concatenated
with the study day when additional medication was administered. Since we need always to be able to
retrieve the actual status on intake of add. medication together with the information on rescue medication
taken, the referencing (normalization) is perfect for our needs (under the assumption that the collection of additional medication is much more frequently updated than the collection of rescue medication records). For simplicity, all patients in our data model have taken Drug A (dose=25mg) or nothing at a specific day.

Here, I have implemented such dereferencing using the Pymongo driver's dereference() method.

Lookup data:
------------

Input  data file = rescuemed.json.txt
                   additional_medication.json.txt
Output data file = rescuemed_dereferenced.json 
'''


l = open(os.getcwd() + '/rescuemed_dereferenced.json', 'w')
try:    
   docs = dbh.rescuemed.find({"fineparticles": {"$gt":70},"$where":"function() {var x = this.sulfurdioxid[0].mor; var y = this.sulfurdioxid[2].eve; var z = this.sulfurdioxid[3].triplemean; if ((z > 60 || y-x > 18) && this.Dose > 0) return this;}"}, {"_id":0})
   for doc in docs:
      # Resolve (dereference) the link to collection: additional_medication:
      thisref = dbh.dereference(doc['ref'][0])
      if thisref is not None: doc['ref'][0] = thisref
      else: doc['ref'][0] = 0
      l.write(json.dumps(doc) + '\n')
finally:
   l.close()
