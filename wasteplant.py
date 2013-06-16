'''
Copyright (c) 2013, Dipl. oec.troph. Dirk Engfer, Germany
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

'''
-----------------------
Purposes of the script:
-----------------------

1.) Demonstrate the use of document-orientated databases in the field of epidemiology,
that is, building up data structures that can easily be accessed/evaluated by means of
widely acknowledged methods for processing JSON-like data structures. Analyzing
big data often takes advantage of applying databases other than relational data models.
2.) Example script for using Pymongo.
3.) Example for MongoDB's integrated feature of referencing data in different collections
using DBRef.


The following database collections will be created in mydb1:
subjects
additional_medication
sulfur
fineparticles
rescuemed

++++++++++++++++++++++++++++++++++++++++++++++++
Note: From these artificially created test data
NO scientific conclusions can be drawn. The
data created by running this script is for
demonstration purposes only. Similarities
between these test data and real world
investigations are not intended but can
occur.
++++++++++++++++++++++++++++++++++++++++++++++++

-------------------------
Epidemiological scenario:
-------------------------

With this Python script, artificial test data is generated for testing hypotheses such like:
Living in the neighbourhood of a waste incinerator has a higher risk in
developing chronic bronchitis compared to living in a higher distance from
a waste incinerator.

Study design (based on artificial test data) is as follows:
Outdoor air pollution in terms of sulfur-dioxide concentration is the risk factor and is measured in ug/m3.
Co-variate is the concentration of fine particles (particulate matter) and is measured in ug/m3.
Negative environmental health effect is investigated in terms of inhaled rescue medication (acute dyspnea): with each
puff the inhaler device sends automatically a GPS signal that in turn results in the generation of a
database record relating to the registered patient (who experiences dyspnea) with date, time and location of inhalation.
Study population is made up of subjects living in 5 regions around waste incinerators. For each region, subjects are devided into
3 groups for living in a distance from the the waste incinerator of (a) 0-<10 km, (b) 10-<20 km, and (c) 20-<30 km.
Cases are patients suffering from chronic bronchitis. Assumption: all cases are registered and identified. All patients use an inhaler device that is featured with a special GPS sender for instant monitoring. A signal is sent and captured into a database indicating the administration to drug. A puff contains a theoretical amount of active substance.

Evaluation:

Is there evidence for a correlation between exposure to immisions of sulfur-dioxide in the air and the intake of rescue medication (number of puffs/dose amount over a time period) i.e. the health outcome effect?

Over a long time period, air concentration of sulfur-dioxide is measured 3 times daily. As another interfering cause for taking rescue medication the daily level of fine particles in the air is measured in these regions.

Data Model:

The data model integrates these environmental parameters with doses of rescue medication and other concomitant medication for treatment of chronic bronchitis. As data become extremely large, especially when the observational period is for 10 years or more, it was made the decision to store records as documents. Documents embed most information in contrast to a relational design, where relationships are established via JOINs of tables. However, here documents also relate to other data (collections) via references (DBRef) where it makes sense.

'''


from bson.dbref import DBRef
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

import os
import sys
from random import choice

def make_item(key=None, ra=None, day=None, re=None, embedded_itemsL=None):

    if day % 2 == 0:
        dayfactor = 0.95
    else:
        dayfactor = 1.05
        
    if key == "subjects":
        resultL = []
        if radiusD[ra] == 2: populcount = 50000
        else: populcount = 25000
        if regionsD[re] == 1: populcount = (populcount * 100) / 90
        if regionsD[re] == 2: populcount = (populcount * 100) / 95
        if regionsD[re] == 3: populcount = (populcount * 100) / 110
        
        for x in range(1,populcount + 1):
           subjectsD = {'SubjectId' : re + '_' + ra + '_' + str(x), 'Region':re, 'Radius':ra}
           resultL.append(subjectsD)
        dbh.subjects.insert(resultL, safe=True)
        resultD = {key : resultL}
                        
    if key == "sulfur":
        raw = 20 # ug/m3
        resultL = []
        tempresult = (radiusD[ra] * raw) * regionsD[re] * dayfactor
        sumval = 0.0
        f = lambda x, y: x * y
        for key2, val in zip(sulfurLL[0], sulfurLL[1]):
            subD = {key2 : f(val, tempresult)}
            resultL.append(subD)
            sumval = sumval + f(val, tempresult)
        resultL.append({"triplemean" : sumval / 3})
        resultD = {'sulfurdioxid' : resultL, 'Day' : day, 'Region' : re, 'Radius' : ra}
        
    if key == "fineparticles":
        raw = 1 # 1ug/m3
        resultD = {key : raw * max(day%10, 1) * (regionsD[re] * 10), 'Day' : day, 'Region' : re, 'Radius' : ra, '_id' : re + '_' + ra + '_' + str(day) }
    
    if key == "rescuemed_per_day":
       resultL = []
       # Build the daily dose (in ug) - from puffs of rescue medication inhaled
       inidose = 50 * radiusD[ra] * regionsD[re]
       if radiusD[ra] == 2: patients_count = 5000
       else: patients_count = 2500
         
       for x in range(1,patients_count + 1):
          myran = choice([1,1,2,4])
          if myran == 4:
             dose = 0
          else:
             dose = inidose * myran
          dailydoseD = {'PatientId' : re + '_' + ra + '_' + str(x), 'Day' : day, 'Dose' : int(dose), 'ref' : [DBRef('additional_medication', re + '_' + ra + '_' + str(x))]}
          for embedD in embedded_itemsL:
             for mykey in embedD.keys():
                dailydoseD[mykey] = embedD[mykey]
          dbh.rescuemed.insert(dailydoseD, safe=True)
          resultL.append(dailydoseD)
       resultD = {key : resultL}
       
    if key == "additional_medication":
       resultL = []
       resultD = {}
       for x in range(1, 5001):
          if x % 10 == 0:
             for d in range(num_days):
                if d % 7 == 0:
                   resultL.append({'Day' : d, 'Dose' : 25, 'Drug':'A', 'PatientId' : re + '_' + ra + '_' + str(x), '_id' : re + '_' + ra + '_' + str(x) + '_' + str(d)})
       dbh.additional_medication.insert(resultL, safe=True)
       resultD['intake'] = resultL
           
    return resultD
            

class connectAndGetDB:
    """ connect to a particular Mongodb database """

    def handle(self):

       try:
          connection = MongoClient()
          dbh = connection.mydb1

       except ConnectionFailure, e:
          sys.stderr.write("Could not connect to MongoDB %s" % e)
          sys.exit(1)
       return dbh

def pr(printobj):
    print >> l, printobj


connection_instance = connectAndGetDB() # get mydb1
dbh = connection_instance.handle()
dbh.subjects.drop()
dbh.additional_medication.drop()
dbh.sulfur.drop()
dbh.fineparticles.drop()
dbh.rescuemed.drop()

num_days = 20   # Increase the observational period [days] for measuring air pollution as you wish

l = open(os.getcwd() + '/log.txt', 'w')
try:    
    # List of pollution types: sulfur-dioxide, fine particles (particle pollution)
    covarL =["sulfur", "fineparticles"]
    # Dict of Radius (10km, 20km, 30km) with weighing factors
    radiusD = {"radius10":3, "radius20":2, "radius30":1}
    # Dict of Regions around a waste incinerator (5 regions)
    regionsD = {"region1":0.7,"region2":0.8,"region3":0.9,"region4":1,"region5":1.1}
    # Matrix on sulfur-dioxide imission-factors for morning, midday and evening measures
    sulfurLL = [["mor","mid","eve"], [1, 1.1, 1.3]]

    newitem1L = []
    newitem2L = []
    for ra in radiusD:
        for d in range(num_days):
            for re in regionsD:
                if d == 0:
                   newitem0 = make_item(key="subjects",day=d,ra=ra, re=re)
                   newitem4 = make_item(key="additional_medication", day=d, ra=ra, re=re) 
                newitem1 = make_item(key="sulfur", day=d, ra=ra, re=re)
                newitem1L.append(newitem1)
                newitem2 = make_item(key="fineparticles", day=d, ra=ra, re=re)
                newitem2L.append(newitem2)
                if d % 7 == 0:
                   item_sulfD = {}
                   item_fineD = {}
                   item_sulfD['sulfurdioxid'] = newitem1['sulfurdioxid']
                   item_fineD['fineparticles'] =newitem2['fineparticles']
                   newitem3 = make_item(key="rescuemed_per_day", day=d, ra=ra, re=re, embedded_itemsL=[item_sulfD, item_fineD])
                   
                   if d == 7: pr(newitem3)
    dbh.sulfur.insert(newitem1L, safe=True)
    dbh.fineparticles.insert(newitem2L, safe=True)
        
finally:
    l.close()
