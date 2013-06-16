// Scientific interest is on extreme values of air pollution and patients taking rescue medication
// (chronic bronchitis) at the days with high exposure to air pollution:
// Get all documents from the collection rescuemed per day,
// where the actual daily concentration of fineparticles is > 70 ug
// AND the patient inhaled rescue medication to whatever extent (but at least one puff)
// AND the actual daily mean value concentration of sulfurdioxide in the outdoor air
//     is either > 60 ug or the difference of the sulfurdioxide evening measure and the morning
//     measure is greater than 18 ug.




// Perform the following query on the MongoDB mydb1 (build up the database by running wasteplant.py).

docs = dbh.rescuemed.find({"fineparticles": {"$gt":70},"$where":"function() {var x = this.sulfurdioxid[0].mor;
 var y = this.sulfurdioxid[2].eve; var z = this.sulfurdioxid[3].triplemean;
 if ((z > 60 || y-x > 18) && this.Dose > 0) return this;}"})
 
 
// --> The query syntax can run with Python (Pymongo), too. Or directly run the query against your
//     MongoDB shell.
 
// This query uses input data: rescuemed (cf. file rescuemed.json.txt)
// and produces a result subset, cf. file rescuemed_query_results.txt.
//
// Notes:
// The query takes advantage of the JavaScript engine coming with the Mongo Shell. That way, one is able
// to build conditions with intermediary (computed) results (eg. y - x > 18). This comes in handy, when
// you are asked not to download the whole collection but rather only those documents of interest.
// Complex queries are possible with JavaScript.
// Philosophy of embedding other data into one document:
// As you can see, air pollution measures were merged with patient's dose administration records.
// As a result, no need to JOIN these data into patient related data. It ensures better performance in
// getting result documents (no costly operations on table joins are needed by the database).
// Furthermore, you can see that such aggregates (documents) do not lack of null values (a default value 
// of 'null' for all fields isn't prepopulated). In epidemiology, this is a nicety since many subject properties
// are missing ie. sparsely populated (in the sense of relational design: tables with many empty cells).
