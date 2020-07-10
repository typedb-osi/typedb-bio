# Instructions

In this directory, add the file `CORD-NER-FULL.json` from here: https://xuanwang91.github.io/2020-03-20-cord19-ner/

# Data Description

CORD-NER-full.json: the full dataset including the meta-data, full-text corpus and CORD-19-NER results from the 29,500 documents in [COVID-19 Open Research Dataset Challenge (CORD-19)](https://www.kaggle.com/allen-institute-for-ai/CORD-19-research-challenge) corpus (2020-03-13). The size of the dataset is about 1.2GB. It includes the following items:

  - id: the line number (0-29499) in "all_sources_metadata_2020-03-13.csv" in the CORD-19 corpus (2020-03-13).
  - source: CZI (1236 records), PMC (27337), bioRxiv (566) and medRxiv (361).
  - doi: populated for all BioRxiv/MedRxiv paper records and most of the other records (26357 non null).
  - pmcid: populated for all PMC paper records (27337 non null).
  - pubmed_id: populated for some of the records.
  - Other meta-data: publish_time, authors and journal, title, abstract.
  - body: full-text corpus
  - entities: CORD-19-NER annotation results

Each line represents one document in the dataset. The file schema is shown as below.

```
{'id':0, 'source':'xxx', 'doi':'xxx', 'pmcid':'xxx', 'pubmed_id':'xxx', 'publish_time':'xxx', 'authors':'xxx', 'journal':'xxx', 'title':'xxx', 'abstract':'xxx', 'body':'xxx', 'entities':[{'text':'xxx', 'start':0, 'end':3, 'type':'xxx'}, ...]}, ...]}
...
```