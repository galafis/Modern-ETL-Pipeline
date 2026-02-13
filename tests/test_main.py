"""
Functional tests for Modern-ETL-Pipeline
"""

import pytest
import os
import sys
import tempfile
import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_pipeline import DataExtractor, DataTransformer, DataLoader, ETLPipeline


@pytest.fixture
def sample_config():
    return {
        'sources': {
            'api_url': 'https://api.example.com/data',
            'database_path': 'data/source.db',
            'csv_path': 'data/raw/input.csv',
        },
        'targets': {
            'database_path': 'data/output/warehouse.db',
            'csv_path': 'data/output/processed_data.csv',
            'json_path': 'data/output/processed_data.json',
        },
        'schedule': {'enabled': False, 'interval_minutes': 60},
    }


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        'id': [1, 2, 3, 3],
        'name': ['Alice', 'Bob', None, 'Charlie'],
        'price': [10.0, 200.0, 50.0, 50.0],
        'category': ['A', 'B', 'A', 'C'],
    })


class TestDataExtractor:
    """Tests for DataExtractor."""

    def test_extract_csv_creates_dataframe(self, sample_config, tmp_path):
        """extract_csv returns a DataFrame from a CSV file."""
        csv_file = tmp_path / "test_input.csv"
        df_original = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        df_original.to_csv(csv_file, index=False)

        extractor = DataExtractor(sample_config)
        result = extractor.extract_csv(str(csv_file))

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ['col1', 'col2']

    def test_extract_api_returns_dataframe(self, sample_config):
        """extract_api returns a DataFrame with mock data."""
        extractor = DataExtractor(sample_config)
        result = extractor.extract_api('https://api.example.com/data')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 100
        assert 'id' in result.columns
        assert 'price' in result.columns


class TestDataTransformer:
    """Tests for DataTransformer."""

    def test_clean_data_removes_duplicates(self, sample_config, sample_dataframe):
        """clean_data removes duplicate rows."""
        transformer = DataTransformer(sample_config)
        result = transformer.clean_data(sample_dataframe)

        assert len(result) < len(sample_dataframe)
        assert result.duplicated().sum() == 0

    def test_clean_data_fills_nulls(self, sample_config):
        """clean_data fills null values (median for numeric, 'Unknown' for text)."""
        df = pd.DataFrame({
            'value': [10.0, None, 30.0],
            'label': ['X', None, 'Z'],
        })
        transformer = DataTransformer(sample_config)
        result = transformer.clean_data(df)

        assert result['value'].isna().sum() == 0
        assert result['label'].isna().sum() == 0
        assert 'Unknown' in result['label'].values

    def test_transform_data_adds_price_category(self, sample_config):
        """transform_data adds price_category when a price column exists."""
        df = pd.DataFrame({
            'price': [10.0, 100.0, 300.0, 800.0],
            'name': ['a', 'b', 'c', 'd'],
        })
        transformer = DataTransformer(sample_config)
        result = transformer.transform_data(df)

        assert 'price_category' in result.columns
        assert 'processed_at' in result.columns


class TestDataLoader:
    """Tests for DataLoader."""

    def test_load_to_csv_creates_file(self, sample_config, tmp_path):
        """load_to_csv writes a DataFrame to a CSV file."""
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        output_path = tmp_path / "output.csv"

        loader = DataLoader(sample_config)
        loader.load_to_csv(df, str(output_path))

        assert output_path.exists()
        loaded = pd.read_csv(output_path)
        assert len(loaded) == 2


class TestETLPipeline:
    """Tests for ETLPipeline orchestrator."""

    def test_pipeline_can_be_instantiated(self):
        """ETLPipeline initialises with default config when no YAML exists."""
        pipeline = ETLPipeline()
        assert pipeline.config is not None
        assert pipeline.extractor is not None
        assert pipeline.transformer is not None
        assert pipeline.loader is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
