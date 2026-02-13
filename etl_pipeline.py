#!/usr/bin/env python3
"""
Modern ETL Pipeline
Scalable ETL pipeline with monitoring, error handling, and data validation.
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
import schedule
import time
from typing import Dict, List, Any
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataExtractor:
    """Extract data from various sources."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def extract_csv(self, file_path: str) -> pd.DataFrame:
        """Extract data from CSV file."""
        try:
            self.logger.info(f"Extracting data from CSV: {file_path}")
            df = pd.read_csv(file_path)
            self.logger.info(f"Extracted {len(df)} rows from CSV")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting CSV data: {e}")
            raise
    
    def extract_database(self, query: str, connection_string: str) -> pd.DataFrame:
        """Extract data from database."""
        try:
            self.logger.info(f"Extracting data from database")
            conn = sqlite3.connect(connection_string)
            df = pd.read_sql_query(query, conn)
            conn.close()
            self.logger.info(f"Extracted {len(df)} rows from database")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting database data: {e}")
            raise
    
    def extract_api(self, url: str, headers: Dict = None) -> pd.DataFrame:
        """Extract data from API (mock implementation)."""
        try:
            self.logger.info(f"Extracting data from API: {url}")
            # Mock API data
            data = {
                'id': range(1, 101),
                'name': [f'Product_{i}' for i in range(1, 101)],
                'price': np.random.uniform(10, 1000, 100),
                'category': np.random.choice(['A', 'B', 'C'], 100),
                'created_at': pd.date_range('2024-01-01', periods=100, freq='D')
            }
            df = pd.DataFrame(data)
            self.logger.info(f"Extracted {len(df)} rows from API")
            return df
        except Exception as e:
            self.logger.error(f"Error extracting API data: {e}")
            raise

class DataTransformer:
    """Transform and clean data."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate data."""
        try:
            self.logger.info("Starting data cleaning")
            original_rows = len(df)
            
            # Remove duplicates
            df = df.drop_duplicates()
            
            # Handle missing values
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())
            
            categorical_columns = df.select_dtypes(include=['object']).columns
            df[categorical_columns] = df[categorical_columns].fillna('Unknown')
            
            # Remove outliers (simple method)
            for col in numeric_columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
            
            cleaned_rows = len(df)
            self.logger.info(f"Data cleaning completed: {original_rows} -> {cleaned_rows} rows")
            
            return df
        except Exception as e:
            self.logger.error(f"Error cleaning data: {e}")
            raise
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business transformations."""
        try:
            self.logger.info("Starting data transformation")
            
            # Add calculated columns
            if 'price' in df.columns:
                df['price_category'] = pd.cut(df['price'], 
                                            bins=[0, 50, 200, 500, float('inf')], 
                                            labels=['Low', 'Medium', 'High', 'Premium'])
            
            # Add timestamp
            df['processed_at'] = datetime.now()
            
            # Normalize text columns
            text_columns = df.select_dtypes(include=['object']).columns
            for col in text_columns:
                if col not in ['processed_at']:
                    df[col] = df[col].str.strip().str.title()
            
            self.logger.info("Data transformation completed")
            return df
        except Exception as e:
            self.logger.error(f"Error transforming data: {e}")
            raise

class DataLoader:
    """Load data to target destinations."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def load_to_database(self, df: pd.DataFrame, table_name: str, connection_string: str):
        """Load data to database."""
        try:
            self.logger.info(f"Loading {len(df)} rows to database table: {table_name}")
            conn = sqlite3.connect(connection_string)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            self.logger.info("Data loaded to database successfully")
        except Exception as e:
            self.logger.error(f"Error loading data to database: {e}")
            raise
    
    def load_to_csv(self, df: pd.DataFrame, file_path: str):
        """Load data to CSV file."""
        try:
            self.logger.info(f"Loading {len(df)} rows to CSV: {file_path}")
            df.to_csv(file_path, index=False)
            self.logger.info("Data loaded to CSV successfully")
        except Exception as e:
            self.logger.error(f"Error loading data to CSV: {e}")
            raise
    
    def load_to_json(self, df: pd.DataFrame, file_path: str):
        """Load data to JSON file."""
        try:
            self.logger.info(f"Loading {len(df)} rows to JSON: {file_path}")
            df.to_json(file_path, orient='records', date_format='iso')
            self.logger.info("Data loaded to JSON successfully")
        except Exception as e:
            self.logger.error(f"Error loading data to JSON: {e}")
            raise

class ETLPipeline:
    """Main ETL Pipeline orchestrator."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.config = self.load_config(config_path)
        self.extractor = DataExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader = DataLoader(self.config)
        
        # Create output directories
        Path('data/raw').mkdir(parents=True, exist_ok=True)
        Path('data/processed').mkdir(parents=True, exist_ok=True)
        Path('data/output').mkdir(parents=True, exist_ok=True)
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        default_config = {
            'sources': {
                'api_url': 'https://api.example.com/data',
                'database_path': 'data/source.db',
                'csv_path': 'data/raw/input.csv'
            },
            'targets': {
                'database_path': 'data/output/warehouse.db',
                'csv_path': 'data/output/processed_data.csv',
                'json_path': 'data/output/processed_data.json'
            },
            'schedule': {
                'enabled': True,
                'interval_minutes': 60
            }
        }
        
        try:
            if Path(config_path).exists():
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                return {**default_config, **config}
        except Exception as e:
            self.logger.warning(f"Could not load config file: {e}. Using defaults.")
        
        return default_config
    
    def create_sample_data(self):
        """Create sample data for demonstration."""
        # Create sample CSV
        sample_data = {
            'id': range(1, 201),
            'product_name': [f'Product_{i}' for i in range(1, 201)],
            'price': np.random.uniform(5, 500, 200),
            'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home'], 200),
            'stock_quantity': np.random.randint(0, 100, 200),
            'created_date': pd.date_range('2023-01-01', periods=200, freq='D')
        }
        
        df = pd.DataFrame(sample_data)
        df.to_csv('data/raw/input.csv', index=False)
        
        # Create sample database
        conn = sqlite3.connect('data/source.db')
        df.to_sql('products', conn, if_exists='replace', index=False)
        conn.close()
        
        self.logger.info("Sample data created successfully")
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline."""
        start_time = datetime.now()
        try:
            self.logger.info("Starting ETL Pipeline execution")
            
            # Extract data from multiple sources
            extracted_data = []
            
            # Extract from CSV
            try:
                csv_data = self.extractor.extract_csv('data/raw/input.csv')
                extracted_data.append(csv_data)
            except Exception as e:
                self.logger.warning(f"CSV extraction failed: {e}")
            
            # Extract from API (mock)
            try:
                api_data = self.extractor.extract_api('https://api.example.com/data')
                extracted_data.append(api_data)
            except Exception as e:
                self.logger.warning(f"API extraction failed: {e}")
            
            if not extracted_data:
                raise Exception("No data sources available")
            
            # Combine all extracted data
            combined_data = pd.concat(extracted_data, ignore_index=True)
            self.logger.info(f"Combined data: {len(combined_data)} rows")
            
            # Transform data
            cleaned_data = self.transformer.clean_data(combined_data)
            transformed_data = self.transformer.transform_data(cleaned_data)
            
            # Load data to targets
            self.loader.load_to_database(
                transformed_data, 
                'processed_products', 
                self.config['targets']['database_path']
            )
            
            self.loader.load_to_csv(
                transformed_data, 
                self.config['targets']['csv_path']
            )
            
            self.loader.load_to_json(
                transformed_data, 
                self.config['targets']['json_path']
            )
            
            # Log pipeline metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            metrics = {
                'execution_time': duration,
                'rows_processed': len(transformed_data),
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'status': 'success'
            }
            
            self.log_metrics(metrics)
            self.logger.info(f"ETL Pipeline completed successfully in {duration:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {e}")
            metrics = {
                'execution_time': 0,
                'rows_processed': 0,
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'status': 'failed',
                'error': str(e)
            }
            self.log_metrics(metrics)
            raise
    
    def log_metrics(self, metrics: Dict[str, Any]):
        """Log pipeline execution metrics."""
        metrics_file = 'data/output/pipeline_metrics.json'
        
        # Load existing metrics
        existing_metrics = []
        if Path(metrics_file).exists():
            try:
                with open(metrics_file, 'r') as f:
                    existing_metrics = json.load(f)
            except (json.JSONDecodeError, IOError, OSError):
                existing_metrics = []
        
        # Add new metrics
        existing_metrics.append(metrics)
        
        # Save updated metrics
        with open(metrics_file, 'w') as f:
            json.dump(existing_metrics, f, indent=2)
    
    def schedule_pipeline(self):
        """Schedule pipeline execution."""
        if not self.config['schedule']['enabled']:
            self.logger.info("Scheduling is disabled")
            return
        
        interval = self.config['schedule']['interval_minutes']
        self.logger.info(f"Scheduling pipeline to run every {interval} minutes")
        
        schedule.every(interval).minutes.do(self.run_pipeline)
        
        while True:
            schedule.run_pending()
            time.sleep(60)

def main():
    """Main execution function."""
    print("Modern ETL Pipeline")
    print("=" * 30)
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Create sample data
    pipeline.create_sample_data()
    
    # Run pipeline once
    pipeline.run_pipeline()
    
    # Optionally start scheduler
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--schedule':
        pipeline.schedule_pipeline()

if __name__ == "__main__":
    main()

