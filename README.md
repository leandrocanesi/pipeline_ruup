Claro! Abaixo está um `README.md` completo e organizado para documentar todo o seu projeto Open-Meteo, incluindo os notebooks, execução com JupyterLab e Airflow, e os detalhes da DAG, ambiente e dependências.

---

```markdown
# 🌦️ Open-Meteo Pipeline — Projeto de Engenharia de Dados

Este projeto implementa um pipeline de previsão climática baseado em notebooks orquestrados com **Apache Airflow** e processados com **Papermill**. Os dados são tratados com **PySpark** e **Delta Lake**, e visualizados com **Plotly**.

---

## 📁 Estrutura do Projeto

```bash
projetos/Airflow/
├── .venv/                        # Ambiente virtual Python
├── dags/
│   └── open_meteo_pipeline_dag.py   # DAG principal do Airflow
├── notebooks/
│   ├── extraction/              # Notebook de extração de dados
│   │   └── 01_extract_open_meteo.ipynb
│   ├── bronze/                  # Validação inicial (camada bronze)
│   │   └── 02_validate_bronze_open_meteo.ipynb
│   ├── silver/                  # Transformação (camada silver)
│   │   └── 03_transform_silver_open_meteo.ipynb
│   ├── gold/                    # Enriquecimento (camada gold)
│   │   └── 04_prepare_gold_open_meteo.ipynb
│   ├── dashboard/               # Geração de dashboard
│   │   └── 05_dashboard_open_meteo.ipynb
│   └── executed/                # Resultados dos notebooks executados pelo Airflow
├── data/                        # Dados Delta Lake
│   ├── bronze/
│   ├── silver/
│   ├── gold/
└── requirements.txt             # Dependências do projeto
```

---

## 🧪 Como rodar

### 1. Clone o repositório e entre na pasta

```bash
cd ~/projetos/Airflow
```

### 2. Crie o ambiente virtual

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

> 📌 **Nota:** Certifique-se de instalar também `pyspark`, `delta-spark`, `ipywidgets`, `plotly`, entre outros que aparecem abaixo.

---

## 🚀 JupyterLab

### Inicie o servidor Jupyter:

```bash
jupyter lab
```

### Acesse via navegador:
```
http://localhost:8888
```

> **Dica:** Habilite a visualização de arquivos ocultos no menu **View → Show Hidden Files** para visualizar a pasta `.venv`.

---

## 🎯 Apache Airflow

### Inicialização do banco de dados

```bash
airflow db migrate
```

### Crie um usuário administrador:

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### Inicie o scheduler e webserver:

```bash
airflow scheduler
airflow webserver
```

Acesse o Airflow via navegador:

```
http://localhost:8080
```

---

## ⚙️ DAG principal

A DAG está localizada em:

```bash
dags/open_meteo_pipeline_dag.py
```

Ela orquestra os seguintes notebooks:

1. `extraction/01_extract_open_meteo.ipynb`
2. `bronze/02_validate_bronze_open_meteo.ipynb`
3. `silver/03_transform_silver_open_meteo.ipynb`
4. `gold/04_prepare_gold_open_meteo.ipynb`
5. `dashboard/05_dashboard_open_meteo.ipynb`

Execute a DAG manualmente pela interface ou via terminal:

```bash
airflow dags trigger open_meteo_pipeline_dag
```

---

## 📦 Requisitos (requirements.txt)

```txt
apache-airflow==2.10.4
apache-airflow[celery, http, sqlite]
apache-airflow-providers-papermill
apache-airflow-providers-docker
apache-airflow-providers-cncf-kubernetes
apache-airflow-providers-common-io
papermill
pyspark
delta-spark
pandas
plotly
ipywidgets
matplotlib
seaborn
requests
```

> ✅ **Dica:** Para instalar Delta Lake:
```bash
pip install delta-spark
```

---

## 🧼 Problemas comuns

| Erro | Solução |
|------|---------|
| `ModuleNotFoundError: No module named 'pyspark'` | Execute `pip install pyspark` |
| `FileNotFoundError` nos notebooks | Verifique o caminho correto no PapermillOperator |
| Widgets não interativos no dashboard | Isso é esperado ao executar via Airflow. Use `fig.show()` por cidade. |

---

## 🧠 Sobre o projeto

Este pipeline demonstra práticas modernas de **engenharia de dados com notebooks orquestrados**, usando o melhor dos mundos: Jupyter para prototipagem + Airflow para produção.

---

**Desenvolvido por:** _[Seu Nome Aqui]_ 🧪  
```

---

Se quiser que eu gere esse conteúdo automaticamente dentro de um `README.md` no seu ambiente ou salve no projeto, me avise que te passo o comando ou até o script.
