import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from main import MainDBProcessor, Aggregate, ProcessingStatus, FuzzyMatchResult

@pytest.fixture
def mock_engine():
    return MagicMock()

@pytest.fixture
def mock_session(mock_engine):
    session = MagicMock(spec=Session)
    session.bind = mock_engine
    return session

@pytest.fixture
def processor(mock_engine, mock_session):
    with patch('main.create_engine', return_value=mock_engine):
        with patch('main.sessionmaker', return_value=lambda: mock_session):
            return MainDBProcessor('mock://db_url', 'test_table', percentage_score=85, num_cores=2, batch_size=1000)

def test_init(processor):
    assert processor.input_table_name == 'test_table'
    assert processor.percentage_score == 85
    assert processor.num_cores == 2
    assert processor.batch_size == 1000

def test_check_memory_usage(processor):
    with patch('main.psutil.virtual_memory') as mock_memory:
        mock_memory.return_value.percent = 50
        with patch.object(processor, 'log_status') as mock_log:
            processor.check_memory_usage()
            mock_log.assert_called_once_with("Memory usage: 50%")

def test_insert_in_batches(processor, mock_session):
    matches = [
        (1, 'source1', 'matched1', 2, 90.0, 'src1'),
        (2, 'source2', 'matched2', 3, 85.0, 'src2'),
    ]
    processor.insert_in_batches(matches)
    assert mock_session.execute.called
    assert mock_session.commit.called

def test_process_suppliers(processor, mock_session):
    mock_session.query.return_value.scalar.return_value = 100
    mock_session.query.return_value.first.return_value = ProcessingStatus(last_processed_id=0, total_records=100, processed_records=0)
    
    mock_data = MagicMock()
    mock_data.__len__.return_value = 50
    mock_data.empty = False
    mock_data['i'].max.return_value = 50
    mock_session.query.return_value.filter.return_value.order_by.return_value.limit.return_value = mock_data
    
    with patch('main.pd.read_sql', return_value=mock_data):
        with patch('main.Pool') as mock_pool:
            mock_pool.return_value.__enter__.return_value.map.return_value = [[]]
            processor.process_suppliers()
    
    assert mock_session.commit.called

def test_update_normalized_names_batch(processor, mock_session):
    updates = [{'i': 1, 'normalized_name': 'test1'}, {'i': 2, 'normalized_name': 'test2'}]
    processor.update_normalized_names_batch(updates)
    assert mock_session.execute.called
    assert mock_session.commit.called

def test_ensure_columns_exist(processor, mock_engine):
    with patch('main.inspect') as mock_inspect:
        mock_inspect.return_value.get_columns.return_value = [{'name': 'existing_column'}]
        processor.ensure_columns_exist()
    assert mock_engine.begin.called

def test_reset_processing_status(processor, mock_session):
    processor.reset_processing_status()
    assert mock_session.query.return_value.delete.called
    assert mock_session.add.called
    assert mock_session.commit.called

def test_update_normalized_names(processor, mock_session):
    mock_session.query.return_value.filter.return_value.count.return_value = 100
    mock_session.query.return_value.filter.return_value.limit.return_value.offset.return_value.all.return_value = [
        Aggregate(i=1, supplier_name='Test Supplier')
    ]
    
    with patch.object(processor, 'update_normalized_names_batch') as mock_update:
        processor.update_normalized_names()
    
    assert mock_update.called

def test_create_joined_view(processor, mock_engine):
    processor.create_joined_view()
    assert mock_engine.begin.called

if __name__ == '__main__':
    pytest.main()
