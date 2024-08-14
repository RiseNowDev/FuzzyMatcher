import pytest
from unittest.mock import MagicMock, patch
from project_utils import file_staging
from psycopg2 import sql

@pytest.fixture
def mock_connect():
    with patch('psycopg2.connect') as mock:
        yield mock

@pytest.fixture
def mock_cursor(mock_connect):
    mock_cursor = MagicMock()
    mock_connect.return_value.cursor.return_value = mock_cursor
    return mock_cursor

@pytest.fixture
def mock_datetime():
    with patch('project_utils.file_staging.datetime') as mock:
        mock.now.return_value.strftime.return_value = '20220101_120000'
        yield mock

def test_create_table_and_import(mock_connect, mock_cursor, mock_datetime):
    mock_df = MagicMock()
    mock_df.columns = ['i', 'source', 'supplier_name']
    mock_df.iterrows.return_value = [(0, ['1', 'source1', 'supplier1']), (1, ['2', 'source2', 'supplier2'])]

    file_staging.create_table_and_import(mock_df, 'customer1', 'project1')

    # Check if execute was called with the correct SQL
    calls = mock_cursor.execute.call_args_list
    create_table_call = next((call for call in calls if 'CREATE TABLE' in str(call)), None)
    assert create_table_call is not None, "CREATE TABLE query was not executed"

    # Check insert calls
    insert_calls = [call for call in calls if 'INSERT INTO' in str(call)]
    assert len(insert_calls) == 2, "Expected 2 INSERT calls"

    # Assert the connection was committed and closed
    mock_connect.return_value.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_connect.return_value.close.assert_called_once()

if __name__ == '__main__':
    pytest.main()