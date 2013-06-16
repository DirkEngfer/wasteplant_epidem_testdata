// Scientific interest is on extreme values of air pollution and patients taking rescue medication
// (chronic bronchitis) at the days with high exposure to air pollution:
// Get all documents from the collection rescuemed per day,
// where the actual daily concentration of fineparticles is > 70 ug
// AND the patient inhaled rescue medication to whatever extent (but at least one puff)
// AND the actual daily mean value concentration of sulfurdioxide in the outdoor air
//     is either > 60 ug or the difference of the sulfurdioxide evening measure and the morning
//     measure is greater than 18 ug.

docs = dbh.rescuemed.find({"fineparticles": {"$gt":70},"$where":"function() {var x = this.sulfurdioxid[0].mor;
 var y = this.sulfurdioxid[2].eve; var z = this.sulfurdioxid[3].triplemean;
 if ((z > 60 || y-x > 18) && this.Dose > 0) return this;}"})
