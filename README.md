# Modern ETL Pipeline

[English](#english) | [Português](#português)

## English

### Overview
A scalable and robust ETL (Extract, Transform, Load) pipeline built with Python. Features comprehensive data processing, error handling, monitoring, and scheduling capabilities for modern data engineering workflows.

### Features
- **Multi-Source Extraction**: CSV, Database, API data sources
- **Data Validation**: Comprehensive cleaning and quality checks
- **Error Handling**: Robust error management and logging
- **Monitoring**: Detailed metrics and execution tracking
- **Scheduling**: Automated pipeline execution
- **Flexible Configuration**: YAML-based configuration
- **Multiple Output Formats**: Database, CSV, JSON targets

### Technologies Used
- **Python 3.8+**
- **Pandas**: Data manipulation and analysis
- **SQLite**: Database operations
- **PyYAML**: Configuration management
- **Schedule**: Task scheduling
- **Logging**: Comprehensive monitoring

### Installation

1. Clone the repository:
```bash
git clone https://github.com/galafis/Modern-ETL-Pipeline.git
cd Modern-ETL-Pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the pipeline:
```bash
python etl_pipeline.py
```

### Usage

#### Basic Execution
```bash
# Run pipeline once
python etl_pipeline.py

# Run with scheduling
python etl_pipeline.py --schedule
```

#### Configuration
Create a `config.yaml` file to customize the pipeline:

```yaml
sources:
  api_url: "https://api.example.com/data"
  database_path: "data/source.db"
  csv_path: "data/raw/input.csv"

targets:
  database_path: "data/output/warehouse.db"
  csv_path: "data/output/processed_data.csv"
  json_path: "data/output/processed_data.json"

schedule:
  enabled: true
  interval_minutes: 60
```

#### Python API
```python
from etl_pipeline import ETLPipeline

# Initialize pipeline
pipeline = ETLPipeline('config.yaml')

# Create sample data
pipeline.create_sample_data()

# Run pipeline
pipeline.run_pipeline()

# Start scheduler
pipeline.schedule_pipeline()
```

### Pipeline Components

#### Data Extractor
- **CSV Extraction**: Read data from CSV files
- **Database Extraction**: SQL query execution
- **API Extraction**: REST API data retrieval
- **Error Handling**: Graceful failure management

#### Data Transformer
- **Data Cleaning**: Remove duplicates, handle missing values
- **Outlier Detection**: Statistical outlier removal
- **Business Logic**: Custom transformations
- **Data Validation**: Quality checks and constraints

#### Data Loader
- **Database Loading**: SQLite/SQL database targets
- **File Export**: CSV and JSON output formats
- **Batch Processing**: Efficient bulk operations
- **Transaction Management**: ACID compliance

### Monitoring & Logging

#### Execution Metrics
- Processing time and performance
- Row counts and data volume
- Success/failure rates
- Error tracking and alerts

#### Log Files
- `etl_pipeline.log`: Detailed execution logs
- `data/output/pipeline_metrics.json`: Performance metrics
- Structured logging with timestamps

### Project Structure
```
Modern-ETL-Pipeline/
├── etl_pipeline.py         # Main pipeline implementation
├── config.yaml            # Configuration file (optional)
├── requirements.txt       # Dependencies
├── README.md              # Documentation
├── data/
│   ├── raw/               # Source data
│   ├── processed/         # Intermediate data
│   └── output/            # Final output
└── logs/                  # Log files
```

### Data Flow
1. **Extract**: Pull data from multiple sources
2. **Validate**: Check data quality and integrity
3. **Transform**: Apply business rules and cleaning
4. **Load**: Store processed data in target systems
5. **Monitor**: Track metrics and performance

### Contributing
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Create a Pull Request

### License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Português

### Visão Geral
Pipeline ETL (Extract, Transform, Load) escalável e robusto construído com Python. Apresenta processamento abrangente de dados, tratamento de erros, monitoramento e capacidades de agendamento para fluxos modernos de engenharia de dados.

### Funcionalidades
- **Extração Multi-Fonte**: Fontes de dados CSV, Database, API
- **Validação de Dados**: Limpeza abrangente e verificações de qualidade
- **Tratamento de Erros**: Gerenciamento robusto de erros e logging
- **Monitoramento**: Métricas detalhadas e rastreamento de execução
- **Agendamento**: Execução automatizada do pipeline
- **Configuração Flexível**: Configuração baseada em YAML
- **Múltiplos Formatos de Saída**: Alvos Database, CSV, JSON

### Tecnologias Utilizadas
- **Python 3.8+**
- **Pandas**: Manipulação e análise de dados
- **SQLite**: Operações de banco de dados
- **PyYAML**: Gerenciamento de configuração
- **Schedule**: Agendamento de tarefas
- **Logging**: Monitoramento abrangente

### Instalação

1. Clone o repositório:
```bash
git clone https://github.com/galafis/Modern-ETL-Pipeline.git
cd Modern-ETL-Pipeline
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Execute o pipeline:
```bash
python etl_pipeline.py
```

### Uso

#### Execução Básica
```bash
# Executar pipeline uma vez
python etl_pipeline.py

# Executar com agendamento
python etl_pipeline.py --schedule
```

#### Configuração
Crie um arquivo `config.yaml` para personalizar o pipeline:

```yaml
sources:
  api_url: "https://api.example.com/data"
  database_path: "data/source.db"
  csv_path: "data/raw/input.csv"

targets:
  database_path: "data/output/warehouse.db"
  csv_path: "data/output/processed_data.csv"
  json_path: "data/output/processed_data.json"

schedule:
  enabled: true
  interval_minutes: 60
```

#### API Python
```python
from etl_pipeline import ETLPipeline

# Inicializar pipeline
pipeline = ETLPipeline('config.yaml')

# Criar dados de exemplo
pipeline.create_sample_data()

# Executar pipeline
pipeline.run_pipeline()

# Iniciar agendador
pipeline.schedule_pipeline()
```

### Componentes do Pipeline

#### Extrator de Dados
- **Extração CSV**: Ler dados de arquivos CSV
- **Extração de Database**: Execução de consultas SQL
- **Extração de API**: Recuperação de dados de API REST
- **Tratamento de Erros**: Gerenciamento gracioso de falhas

#### Transformador de Dados
- **Limpeza de Dados**: Remover duplicatas, tratar valores ausentes
- **Detecção de Outliers**: Remoção estatística de outliers
- **Lógica de Negócio**: Transformações personalizadas
- **Validação de Dados**: Verificações de qualidade e restrições

#### Carregador de Dados
- **Carregamento de Database**: Alvos SQLite/SQL database
- **Exportação de Arquivos**: Formatos de saída CSV e JSON
- **Processamento em Lote**: Operações bulk eficientes
- **Gerenciamento de Transações**: Conformidade ACID

### Monitoramento e Logging

#### Métricas de Execução
- Tempo de processamento e performance
- Contagens de linhas e volume de dados
- Taxas de sucesso/falha
- Rastreamento de erros e alertas

#### Arquivos de Log
- `etl_pipeline.log`: Logs detalhados de execução
- `data/output/pipeline_metrics.json`: Métricas de performance
- Logging estruturado com timestamps

### Estrutura do Projeto
```
Modern-ETL-Pipeline/
├── etl_pipeline.py         # Implementação principal do pipeline
├── config.yaml            # Arquivo de configuração (opcional)
├── requirements.txt       # Dependências
├── README.md              # Documentação
├── data/
│   ├── raw/               # Dados fonte
│   ├── processed/         # Dados intermediários
│   └── output/            # Saída final
└── logs/                  # Arquivos de log
```

### Fluxo de Dados
1. **Extrair**: Puxar dados de múltiplas fontes
2. **Validar**: Verificar qualidade e integridade dos dados
3. **Transformar**: Aplicar regras de negócio e limpeza
4. **Carregar**: Armazenar dados processados em sistemas alvo
5. **Monitorar**: Rastrear métricas e performance

### Contribuindo
1. Faça um fork do repositório
2. Crie uma branch de feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -am 'Adicionar nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Crie um Pull Request

### Licença
Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

