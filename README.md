# BioGrakn COVID 

**[Overview](#overview)** | **[Installation](#installation)** |
 **[Examples](#examples)** 

[![Discussion Forum](https://img.shields.io/discourse/https/discuss.grakn.ai/topics.svg)](https://discuss.grakn.ai)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-grakn-796de3.svg)](https://stackoverflow.com/questions/tagged/grakn)
[![Stack Overflow](https://img.shields.io/badge/stackoverflow-graql-3dce8c.svg)](https://stackoverflow.com/questions/tagged/graql)

BioGrakn COVID is an open source project to build a knowledge graph to enable research in the SARS-CoV-2 virus and COVID-19.

## Overview
BioGrakn COVID provides an intuitive way to query interconnected and heterogeneous biomedical data in one single place. The schema that models the underlying knowledge graph alongside the descriptive query language, Graql, makes writing complex queries an extremely straightforward and intuitive process. Furthermore, the automated reasoning capability of Grakn, allows BioGrakn to become an intelligent database of biomedical data that infers implicit knowledge based on the explicitly stored data. BioGrakn can understand biological facts, infer based on new findings and enforce research constraints, all at query (run) time.

## Installation
**Prerequesites**: Python >3.6, [Grakn Core 1.8.0](https://grakn.ai/download#core), [Grakn Python Client API](https://dev.grakn.ai/docs/client-api/python), [Grakn Workbase 1.3.1](https://grakn.ai/download#workbase) (optional but, recommended)
```bash
    cd <path/to/biograkn-covid>/
    python migrator.py
```
Grab a coffee while the migrator builds the database and schema for you!

## Examples
Graql queries can be run either on grakn console, on workbase or through client APIs.  However, we encourage running the queries on Grakn Workbase to have the best visual experience. Please follow this [tutorial](https://www.youtube.com/watch?v=Y9awBeGqTes&t=197s) on how to run queries on Workbase.

```bash
# For the  gene "SIRT7" give me abstracts mentioned, and for all the authors,
# their patents, and then which protein complexes are mentioned in there, and give me the individual proteins

match 
$g isa gene, has gene-symbol "SIRT7"; 
$p isa paper;
$a isa person;
$patent isa patent; 
$pr isa paragraph;
$pr2 isa paragraph;
$r1 (mentioning-paragraph: $pr, mentioned: $g) isa mentioning;
$r2 (contained-paper-paragraph: $pr, containing-paper: $p) isa paper-paragraph-containment;
$r3 (authored-paper: $p, author: $a) isa authorship; 
$r4 (inventor: $a, invented: $patent) isa invention; 
$r5 (containing-patent: $patent, contained-patent-paragraph: $pr2) isa patent-paragraph-containment;
$prcom (associated-protein: $protein, associated-protein: $protein2) isa protein-complex; 
$r6 (mentioning-paragraph: $pr2, mentioned: $prcom) isa mentioning; 
get; 

```
More example available from **Query Examples** directory.
