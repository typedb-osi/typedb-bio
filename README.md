# TypeDB Bio: Biomedical Knowledge Graph

**[Overview](#overview)** | **[Installation](#installation)** | **[Datasets](#datasets)** |
 **[Examples](#examples)** | **[How You Can Help](#how-you-can-help)** | **[Further Learning](#further-learning)**

[![Discord](https://img.shields.io/discord/665254494820368395?color=7389D8&label=chat&logo=discord&logoColor=ffffff)](https://vaticle.com/discord)
[![Discussion Forum](https://img.shields.io/discourse/https/forum.vaticle.com/topics.svg)](https://forum.vaticle.com)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typedb-796de3.svg)](https://stackoverflow.com/questions/tagged/typedb)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-typeql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/typeql)

## Overview
TypeDB Bio is an open source biomedical knowledge graph to enable research in areas such as drug discovery, precision medicine and drug repurposing. It provides biomedical researchers an intuitive way to query interconnected and heterogeneous biomedical data in one single place.

For example, by querying for the virus *SARS-CoV-2,* we can find the associated human protein, *proteasome subunit alpha type-2* (PSMA2), a component of the proteasome, implicated in *SARS-CoV-2* replication, and its encoding gene (*PSMA2*). Additionally, we can identify the drug *carfilzomib,* a known inhibitor of the proteasome that could therefore be researched as a potential treatment for patients with Covid-19.

<img width="962" alt="image" src="https://user-images.githubusercontent.com/20223184/179493757-039db277-22ac-4bf4-b79e-f32a44481f7d.png">

By examining these specific relationships and their attributes, we can further investigate any connected biological components and better understand their inter-relations. This helps researchers to efficiently study the mechanisms of protein interactions, infections, the immune response, and help to find targets for the development of treatments or drugs more efficiently. We can also expand our search to include contextual information as is shown below:

<img width="832" alt="image" src="https://user-images.githubusercontent.com/20223184/179494158-1ac3acf4-fc56-49bf-8614-e8bc600246c6.png">

The team behind TypeDB Bio consists of a partnership between [GSK](http://gsk.com/), [Oxford PharmaGenesis](https://www.pharmagenesis.com/) and [Vaticle](https://vaticle.com/)

The schema that models the underlying knowledge graph alongside the descriptive query language, TypeQL, makes writing complex queries an extremely straightforward and intuitive process. Furthermore, TypeDB's automated reasoning, allows TypeDB Bio to become an intelligent database of biomedical data in the biomedical field that infers implicit knowledge based on the explicitly stored data. TypeDB Bio can understand biological facts, infer based on new findings and enforce research constraints, all at query (run) time.

## Installation
**Prerequesites**: Python >3.10, [TypeDB Core 2.15.0](https://vaticle.com/download#core), [TypeDB Python Client API 2.14.3](https://docs.vaticle.com/docs/client-api/python), [TypeDB Studio 2.11.0](https://vaticle.com/download#typedb-studio)

Clone this repo:

```bash
git clone https://github.com/vaticle/typedb-bio.git
```

Download the CORD-NER data set from [this link](https://uofi.app.box.com/s/k8pw7d5kozzpoum2jwfaqdaey1oij93x/file/651148518303) and add it to this directory: `Dataset/CORD_NER`

Set up a virtual environment and install the dependencies:

```bash
cd <path/to/typedb-bio>/
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start typedb
```bash
typedb server
```

Start the migrator script

```bash
python migrator.py -n 4 # insert using 4 threads
```

For help with the migrator script command line options:

```bash
python migrator.py -h
```

Now grab a coffee (or two) while the migrator builds the database and schema for you!

## Testing
Install the test dependencies:

```bash
pip install -r requirements_test.txt
```

Run the tests:

```bash
python -m pytest -v -s tests
```

## Development
Install the development dependencies:

```bash
pip install -r requirements_dev.txt
pre-commit install
```

## Examples
TypeQL queries can be run either in the [TypeDB console](https://docs.vaticle.com/docs/console/console), in [TypeDB Studio](https://docs.vaticle.com/docs/studio/overview) or through [client APIs](https://docs.vaticle.com/docs/client-api/overview).  However, we encourage running the queries on TypeDB Studio to have the best visual experience.

```bash
# What are the drugs that interact with the genes associated to the virus Sars?

match
$virus isa virus, has virus-name "SARS";
$gene isa gene;
$drug isa drug;
$rel1 ($gene, $virus) isa gene-virus-association;
$rel2 ($gene, $drug) isa drug-gene-interaction;
offset 0; limit 20;
```

<img width="600" alt="image" src="https://user-images.githubusercontent.com/20223184/179508451-efc2b0f7-280d-4639-8582-46a9ab8882e4.png">

## Datasets

Currently the datasets we've integrated include:

* [CORD-NER](https://xuanwang91.github.io/2020-03-20-cord19-ner/): The CORD-19 dataset that the White House released has been annotated and made publicly available. It uses various NER methods to recognise named entities on CORD-19 with distant or weak supervision.
* [Uniprot](https://www.uniprot.org/uniprot/?query=proteome:UP000005640%20reviewed:yes): We’ve downloaded the reviewed human subset, and ingested genes, transcripts and protein identifiers.
* [Coronaviruses](https://github.com/vaticle/typedb-bio/tree/master/Dataset/Coronaviruses): This is an annotated dataset of coronaviruses and their potential drug targets put together by Oxford PharmaGenesis based on literature review.
* [DGIdb](http://www.dgidb.org/downloads): We’ve taken the *Interactions TSV* which includes all drug-gene interactions.
* [Human Protein Atlas](https://www.proteinatlas.org/about/download): The *Normal Tissue Data* includes the expression profiles for proteins in human tissues.
* [Reactome](https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt): This dataset connects pathways and their participating proteins.
* [DisGeNet](https://www.disgenet.org/downloads): We’ve taken the *curated gene-disease-associations* dataset, which contains associations from Uniprot, CGI, ClinGen, Genomics England and CTD, PsyGeNET, and Orphanet.
* [SemMed](https://skr3.nlm.nih.gov/SemMedDB/dbinfo.html): This is a subset of the SemMed version 4.0 database

In progress:

* [CORD-19](https://www.semanticscholar.org/cord19): We incorporate the original corpus which includes peer-reviewed publications from bioRxiv, medRxiv and others.
    * TODO: write migrator script
* [TissueNet](https://netbio.bgu.ac.il/labwebsite/tissuenet-v-2-download/)
    * TODO: `./Migrators/TissueNet/TissueNetMigrator.py` incomplete: only migrates a single data file and is not called in `./migrator.py`.

We plan to add many more datasets!

## **How You Can Help**

This is an on-going project and we need your help! If you want to contribute, you can help out by helping us including:

- Migrate more data sources (e.g. clinical trials, DrugBank, Excelra)
- Extend the schema by adding relevant rules
- Create a website
- Write tutorials and articles for researchers to get started

If you wish to get in touch, please talk to us on the #typedb-bio channel on our Discord ([link here](https://www.vaticle.com/discord)).

- Konrad Myśliwiec ([LinkedIn](https://www.linkedin.com/in/konrad-my%C5%9Bliwiec-764ba9163/))
- Kim Wager ([LinkedIn](https://www.linkedin.com/in/kimwager/))
- Tomás Sabat ([LinkedIn](https://www.linkedin.com/in/tom%C3%A1s-sabat-83265841/))

## **Further Learning**
- [TypeDB for Life Sciences](https://vaticle.com/use-cases/life-sciences)
- [Predicting Novel Disease Targets at AstraZeneca](https://www.youtube.com/watch?v=-N2NNVVPULM)
- [Accelerating Drug Discovery with a TypeDB Knowledge Graph](https://blog.vaticle.com/biograkn-accelerating-biomedical-knowledge-discovery-with-a-grakn-knowledge-graph-84706768d7d4)
- [Presentation of TypeDB Bio at Orbit 2021](https://www.youtube.com/watch?v=e-3BITuDgu8)
- [Drug Discovery Knowledge Graphs](https://blog.vaticle.com/drug-discovery-knowledge-graphs-46db4212777c)
- [Using a Knowledge Graph for Precision Medicine](https://blog.vaticle.com/precision-medicine-knowledge-graph-eea957d60c08)
- [Drug Repurposing with a TypeDB Knowledge Graph for Bioinformatics](https://blog.vaticle.com/drug-repositioning-with-a-grakn-ai-knowledge-graph-for-bioinformatics-4701591f38c1)
- [What is a Knowledge Graph?](https://blog.vaticle.com/what-is-a-knowledge-graph-5234363bf7f5)
