# Data Platform & Pipelines

## Objetivo
Implementar uma **arquitetura de dados em camadas** (*Raw → Refined → Curated*), com processamento em batch utilizando **Apache Spark** (via **PySpark**), armazenamento otimizado em **formato colunar** (*Parquet*) e integração com um **Data Warehouse** (*Snowflake* ou *PostgreSQL*).  
O projeto visa consolidar práticas de **ETL/ELT**, **modelagem dimensional** e **orquestração de pipelines**.

## O que foi desenvolvido
A primeira etapa consistiu na **ingestão de dados públicos** obtidos no [Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), armazenando-os na camada *Raw* do **Data Lake** em formato **CSV**.  
Em seguida, os dados foram **transformados** utilizando **Apache Spark** por meio da API **PySpark**, convertendo-os para o formato **Parquet** e aplicando **limpeza**, **padronização** e **enriquecimento**, além de realizar **particionamento** para otimizar consultas.  
Na camada *Curated*, os dados foram **organizados para consumo analítico**, seguindo uma **modelagem dimensional** no formato *Star Schema*.  
Por fim, os dados processados foram **carregados** no **Snowflake** (ou *PostgreSQL* para simulação) e **disponibilizados para análises** e integração com ferramentas de **Business Intelligence**.

## Tecnologias utilizadas
O desenvolvimento foi realizado em **Python**, utilizando o **Apache Spark** por meio de sua API para Python (**PySpark**) como principal ferramenta de **processamento distribuído**. O **armazenamento no Data Lake** foi feito no formato **Parquet**, garantindo **compressão** e **eficiência** nas consultas.  
A **orquestração dos pipelines** foi planejada com **Apache Airflow** ou **Dagster**, de forma a automatizar o fluxo de **ingestão → transformação → carga** para o **Data Warehouse**.  
Para a camada analítica, foi utilizado o **Snowflake** como **Data Warehouse** principal, com suporte para simulação em **PostgreSQL**.  
Bibliotecas auxiliares como **Pandas** e **SQLAlchemy** foram empregadas para **manipulação de dados** e **integração** com bancos relacionais.

## Referências
- [Architecting Data Lakes – O'Reilly](https://www.oreilly.com/library/view/architecting-data-lakes/9781492042518/ch04.html)  
- [Brazilian E-Commerce Dataset – Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## Licença
Este projeto está licenciado sob os termos da **MIT License**, uma licença de software **permissiva** que permite **uso**, **modificação** e **distribuição** livremente, inclusive para fins comerciais, desde que seja mantido o aviso de **copyright**
e a **isenção de responsabilidade**. Não há garantias ou responsabilidades sobre o uso do software.
