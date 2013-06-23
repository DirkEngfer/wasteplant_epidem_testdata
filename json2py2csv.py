import os
import json
import csv

mypath = os.getcwd()
infile = open(os.path.join(mypath, 'rescuemed_query_results.txt'),'r')
csvfile = open(os.path.join(mypath, 'rescuemed_query_results.csv'),'w')

try:
   mywriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
   mywriter.writerow( ['PatientId', 'Dose [ug]', 'Sulfurdioxide [ug/m3]'])
   for line in infile:
      mydict = json.loads(line)
      resultline = [str(mydict['PatientId']), str(mydict['Dose']), str(mydict['sulfurdioxid'][3]['triplemean'])]
      mywriter.writerow(resultline)

finally:
   infile.close()
   csvfile.close()
