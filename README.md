
<div align="center">

**AMAZe: A Multi-Agent Zero-shot Index Advisor for Relational Databases**
----------

<p align="center">
  <a href="https://www.arxiv.org/abs/2508.16044">Paper</a> •
  <a href="#Overview">Overview</a> •
  <a href="#Installation">Installation</a> •
  <a href="#Running AMAZe">Running AMAZe</a> •
  <a href="#Datasets">Datasets</a> 
</p>

</div>


## Overview 
**AMAZe**  Query rewrite, which aims to generate more efficient queries by altering a SQL query’s structure without changing the query result, has been an important research problem. In order to maintain equivalence between the rewritten query and the original one during rewriting, traditional query rewrite methods always rewrite the queries following certain rewrite rules. However, some problems still remain. Firstly, existing methods of finding the optimal choice or sequence of rewrite rules are still limited and the process always costs a lot of resources. Methods involving discovering new rewrite rules typically require complicated proofs of structural logic or extensive user interactions. Secondly, current query rewrite methods usually rely highly on DBMS cost estimators which are often not accurate. In this paper, we address these problems by proposing a novel method of query rewrite named LLM-R2, adopting a large language model (LLM) to propose possible rewrite rules for a database rewrite system. To further improve the inference ability of LLM in recommending rewrite rules, we train a contrastive model by curriculum to learn query representations and select effective query demonstrations for the LLM. Experimental results have shown that our method can significantly improve the query executing efficiency and outperform the baseline methods. In addition, our method enjoys high robustness across different datasets.



</div>

## Installation

**PostgreSQL requirement: PostgreSQL 14.4**

**Python requirement: Python>=3.8**

Before running the project, install the Python dependencies by: ```pip install -r requirement.txt```


## Running AMAZe

**Step 1: Set your API key in ```LLM_zero_shot.py``` and ```agents.py```**

**Step 2: Set your DB connection in ```eval_index.py``` and ```get_postgre.py```**

**Step 3: Run ```LLM_zero_shot.py``` with specified params**


## Datasets

We used three datasets in this work and you can download the datasets from the following links:

**TPC-H:** https://github.com/gregrahn/tpch-kit

**TPC-DS:** https://github.com/gregrahn/tpcds-kit

**IMDB:** https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/2QYZBT

**DSB:** https://github.com/microsoft/dsb

