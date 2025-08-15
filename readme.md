# Data Platform & Pipelines

## Objetivo
Este projeto foi desenvolvido como um *estudo prático* de engenharia de dados. O objetivo é implementar uma **arquitetura de dados em camadas** (`Raw` → `Refined` → `Curated`) para consolidar conhecimentos em:
- `ETL/ELT` e `modelagem dimensional`.
- Processamento em `batch` com `Apache Spark` (via `PySpark`).
- Otimização de armazenamento com formato `Parquet`.

---

## O que foi feito
1.  **Ingestão de Dados:** Ingestão de dados públicos do [Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) para a camada `Raw` em formato CSV.
2.  **Transformação com PySpark:** Os dados foram processados para limpeza, padronização e enriquecimento, convertidos para o formato `Parquet` e otimizados com `particionamento`.
3.  **Modelagem e Carga:** Aplicação de `modelagem dimensional` (*Star Schema*) na camada `Curated` e carregamento dos dados em um `Data Warehouse` (Snowflake/PostgreSQL).

---

## Tecnologias Utilizadas
- `Python`: Linguagem de programação principal.
- `Apache Spark (PySpark)`: Para processamento distribuído de dados.
- `Parquet`: Formato de arquivo otimizado para o Data Lake.
- `Snowflake / PostgreSQL`: Para o Data Warehouse.

---

## Referências
- [Brazilian E-Commerce Dataset – Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [Architecting Data Lakes – O'Reilly](https://www.oreilly.com/library/view/architecting-data-lakes/9781492042518/ch04.html)

---

## Licença
Este projeto está licenciado sob a [**MIT License**](https://github.com/seu-usuario/seu-repositorio/blob/main/LICENSE).