# Data Description

## Subject_CORD_NER.csv

The source of "Subject_CORD_NER.csv" data are the following tables from SemMed: 
- PREDICATION 
- PREDICATION_AUX
- SENTENCE.

"Subject_CORD_NER.csv" file contains data about genes included in CORD_NER dataset from SemMed. The data is presented in a format of semantic triples: Subject-Predicate-Object, where data for Subject columns contain genes from CORD_NER dataset, Object columns contain data for genes included in the whole bio_covid and Predication columns contain relation between these data. The whole triples (Subject, Predicate, Object) are already mapped to UMLS concepts. 

Following query was used to retrieve this data from **SemMed ver 4.0 database**:

```bash
SELECT * FROM PREDICATION AS p 
LEFT JOIN PREDICATION_AUX as pa 
ON p.PREDICATION_ID = pa.PREDICATION_ID 
LEFT JOIN SENTENCE as s 
ON p.SENTENCE_ID = s.SENTENCE_ID 
WHERE OBJECT_NAME IN list_of_genes_from_CORD_NER_dataset 
AND 
SUBJECT IN list_of_genes_in_bio_covid;
```
where list_of_genes_in_bio_covid is a list of genes previously inserted from CORD_NER dataset and list_of_genes_in_bio_covid isa list of genes previously inserted to bio_covid.



## Object_CORD_NER.csv

The source of "Object_CORD_NER.csv" data are the following tables from SemMed: 
- PREDICATION 
- PREDICATION_AUX
- SENTENCE.

"Object_CORD_NER.csv" file contains data about genes included in CORD_NER dataset from SemMed. The data is presented in a format of semantic triples: Subject-Predicate-Object, where data for Object columns contain genes from CORD_NER dataset, Subject columns contain data for genes included in the whole bio_covid and Predication columns contain relation between these data. The whole triples (Subject, Predicate, Object) are already mapped to UMLS concepts. 

Following query was used to retrieve this data from **SemMed ver 4.0 database**:

```bash
SELECT * FROM PREDICATION AS p 
LEFT JOIN PREDICATION_AUX as pa 
ON p.PREDICATION_ID = pa.PREDICATION_ID 
LEFT JOIN SENTENCE as s 
ON p.SENTENCE_ID = s.SENTENCE_ID 
WHERE SUBJECT_NAME IN list_of_genes_from_CORD_NER_dataset 
AND 
OBJECT IN list_of_genes_in_bio_covid;
```

where list_of_genes_in_bio_covid is a list of genes previously inserted from CORD_NER dataset and list_of_genes_in_bio_covid isa list of genes previously inserted to bio_covid.

[SemMed database details](https://skr3.nlm.nih.gov/SemMedDB/dbinfo.html)