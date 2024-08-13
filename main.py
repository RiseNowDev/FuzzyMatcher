# Import necessary libraries
import pandas as pd  # For data manipulation
from rapidfuzz import process, fuzz  # For fuzzy string matching
from multiprocessing import Pool, cpu_count, Manager  # For parallel processing
from tqdm import tqdm  # For progress bars
import time  # For time-related functions
import logging  # For logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, func, Text, Table, text, MetaData, bindparam  # For database operations
from sqlalchemy.orm import declarative_base, sessionmaker  # For ORM and session management
from sqlalchemy.dialects.postgresql import insert  # For PostgreSQL-specific operations
from sqlalchemy import inspect  # For database inspection
import psutil  # For system and process utilities
import numpy as np  # For numerical operations
from collections import defaultdict  # For dictionary operations
import multiprocessing as mp  # For multiprocessing
from sqlalchemy.exc import OperationalError, InternalError  # For handling SQLAlchemy exceptions
import random  # For random number generation
import re  # For regular expressions
import datetime  # For timestamp generation

# Define the Base class for ORM models
Base = declarative_base()

# Define ORM models
# This Python class named Aggregate represents a table named 'csu_redux' with columns for
# integer primary key, supplier name, normalized name, match status, and source.
class Aggregate(Base):
    """
    Represents an aggregate entity in the 'csu_redux' table.

    Attributes:
        i (int): The primary key of the entity.
        supplier_name (str): The name of the supplier.
        normalized_name (str): The normalized name of the supplier.
        matched (bool): Indicates if the entity has been matched.
        source (str): The source of the entity.
    """
    __tablename__ = 'csu_redux'
    i = Column(Integer, primary_key=True)
    supplier_name = Column(String)
    normalized_name = Column(String)
    matched = Column(Boolean, default=False)
    source = Column(String)

# This class defines a table named 'processing_status' with columns for id, last_processed_id,
# total_records, and processed_records.
class ProcessingStatus(Base):
    """
    Represents the processing status of a record.

    Attributes:
        id (int): The unique identifier of the processing status.
        last_processed_id (int): The ID of the last processed record.
        total_records (int): The total number of records.
        processed_records (int): The number of records that have been processed.
    """
    __tablename__ = 'processing_status'
    id = Column(Integer, primary_key=True)
    last_processed_id = Column(Integer)
    total_records = Column(Integer)
    processed_records = Column(Integer)

# The `FuzzyMatchResult` class defines a database table with columns for storing fuzzy matching
# results.
class FuzzyMatchResult(Base):
    """
    Represents a fuzzy match result between two strings.

    Attributes:
        id (int): The unique identifier of the match result.
        source_string (str): The original string from the source.
        matched_string (str): The string that was matched.
        similarity_score (float): The similarity score between the source and matched strings.
        source_id (int): The identifier of the source.
        matched_id (int): The identifier of the matched string.
        validation_status (str): The validation status of the match result.
        source (str): The source of the match result.
    """

    __tablename__ = 'fuzzy_match_results'  # This will be overridden
    id = Column(Integer, primary_key=True)
    source_string = Column(String)
    matched_string = Column(String)
    similarity_score = Column(Float)
    source_id = Column(Integer)
    matched_id = Column(Integer)
    validation_status = Column(String)
    source = Column(String)

    # Add this method to set the table name dynamically
    @classmethod
    def set_table_name(cls, name):
        cls.__table__.name = name

# Function to normalize names by removing digits, special characters, and converting to lowercase
import re

def normalize_name(name: str) -> str:
    """
    Normalize a given name by removing digits, replacing special characters with space,
    converting to lowercase, removing extra whitespace, and joining words.

    Args:
        name (str): The name to be normalized.

    Returns:
        str: The normalized name.
    """
    if not name:
        return ''
    name = re.sub(r'\d+', '', name)  # Remove digits
    name = re.sub(r'[^\w\s]', ' ', name)  # Replace special characters with space
    name = name.lower()  # Convert to lowercase
    return ' '.join(name.split())  # Remove extra whitespace and join words

# Custom weighted ratio function combining multiple string similarity metrics
def weighted_ratio(s1, s2, score_cutoff=0):
    """
    Calculates the weighted ratio between two strings.

    Args:
        s1 (str): The first string.
        s2 (str): The second string.
        score_cutoff (int, optional): The minimum score required for a match. Defaults to 0.

    Returns:
        float: The weighted ratio score between the two strings.
    """
    token_sort_ratio = fuzz.token_sort_ratio(s1, s2)
    token_set_ratio = fuzz.token_set_ratio(s1, s2)
    partial_ratio = fuzz.partial_ratio(s1, s2)

    # Calculate weighted average
    weighted_score = (0.4 * token_sort_ratio +
                      0.4 * token_set_ratio +
                      0.2 * partial_ratio)

    return weighted_score

# Function to find matches and scores for a chunk of data
def find_matches_and_scores(args: tuple) -> list:
    """
    Find matches and scores for a given chunk of data.

    Args:
        args (tuple): A tuple containing the chunk of data, all normalized names, and the percentage score.

    Returns:
        list: A list of matches and their scores.

    """
    chunk, all_normalized_names, percentage_score = args
    matches = []
    for idx, row in tqdm(chunk.iterrows(), total=len(chunk), desc="Processing chunk", leave=False):
        normalized_name = row['normalized_name']
        results = process.extract(
            normalized_name,
            all_normalized_names,
            scorer=weighted_ratio,
            limit=100
        )
        
        # Log the results of the matching
        logging.debug(f"Matching results for '{normalized_name}': {results}")

        # Filter results based on percentage_score during extraction
        filtered_results = [match for match in results if match[1] >= percentage_score and row['i'] < match[2]]
        if not filtered_results:
            logging.debug(f"No matches found for '{normalized_name}' with score >= {percentage_score}")
        else:
            matches.extend((row['i'], normalized_name, all_normalized_names[match[2]], match[2], match[1], row['source'])
                           for match in filtered_results)
    return matches

# Class to process the main database
class MainDBProcessor:
    def __init__(self, db_url, percentage_score=85, num_cores=None, batch_size=200000):
        """
        Initializes the MainDBProcessor object with database URL and processing parameters.

        Args:
            db_url (str): The database URL for connecting to the PostgreSQL database.
            percentage_score (int, optional): The minimum percentage score for matching. Defaults to 85.
            num_cores (int, optional): The number of CPU cores to use for processing. Defaults to None, which uses all available cores.
            batch_size (int, optional): The number of records to process in each batch. Defaults to 200000.
        """
        self.db_url = db_url  # Store the database URL
        self.percentage_score = percentage_score  # Store the percentage score for matching
        self.num_cores = num_cores or cpu_count()  # Set the number of cores to use, defaulting to all available cores
        self.batch_size = batch_size  # Store the batch size for processing
        self.engine = create_engine(db_url)  # Create a database engine
        self.Session = sessionmaker(bind=self.engine)  # Create a session factory
        self.session = self.Session()  # Create a new session
        self.metadata = MetaData()  # Initialize metadata for database reflection
        self.metadata.reflect(bind=self.engine)  # Reflect the database schema
        
        # Generate timestamped output table name
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_table_name = f'fuzzy_match_results_{timestamp}'  # Set the output table name
        
        self._setup_logging()  # Set up logging
        self._create_tables()  # Create necessary tables
        self.ensure_columns_exist()  # Ensure necessary columns exist

    # Function to check memory usage
    def check_memory_usage(self):
        """
        The `check_memory_usage` function logs information about the system's memory usage, including total
        memory, available memory, used memory, and memory usage percentage.
        """
        memory = psutil.virtual_memory()
        logging.info(f"Total memory: {memory.total / (1024 ** 3):.2f} GB")
        logging.info(f"Available memory: {memory.available / (1024 ** 3):.2f} GB")
        logging.info(f"Used memory: {memory.used / (1024 ** 3):.2f} GB")
        logging.info(f"Memory usage: {memory.percent}%")

    # Function to insert matches in batches
    def insert_in_batches(self, matches):
        """
        The `insert_in_batches` function inserts data in batches into a database table while handling
        conflicts by not updating existing records.
        
        :param matches: The `matches` parameter in the `insert_in_batches` method seems to be a list of
        matches where each match is a tuple or list containing at least 6 elements. Here is the breakdown of
        each element based on how they are accessed in the method:
        """
        batch_size = 10000
        for i in range(0, len(matches), batch_size):
            batch = matches[i:i + batch_size]
            insert_data = [
                {
                    'source_string': match[1],
                    'matched_string': match[2],
                    'similarity_score': match[4],
                    'source_id': match[0],
                    'matched_id': match[3],
                    'source': match[5],
                    'validation_status': 'pending'
                }
                for match in batch
            ]
            insert_stmt = insert(FuzzyMatchResult.__table__).values(insert_data)
            on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['id'])
            self.session.execute(on_conflict_stmt)
            self.session.commit()

    # Function to process suppliers
    def process_suppliers(self):
        """
        The `process_suppliers` function processes supplier data in batches, finding matches and scores
        using multiprocessing, and updating processing status accordingly.
        """
        total_rows = self.session.query(func.count(Aggregate.i)).scalar()
        logging.info(f"Total rows in Aggregate table: {total_rows}")  # Log total rows

        status = self.session.query(ProcessingStatus).first()
        if not status:
            status = ProcessingStatus(last_processed_id=0, total_records=total_rows, processed_records=0)
            self.session.add(status)
            self.session.commit()

        logging.info(f"Current processing status: last_processed_id={status.last_processed_id}, total_records={status.total_records}, processed_records={status.processed_records}")

        start_time = time.time()
        with tqdm(total=total_rows, desc="Processing suppliers") as pbar:
            while status.processed_records < total_rows:
                logging.info(f"Starting processing loop. status.processed_records: {status.processed_records}, total_rows: {total_rows}")
                logging.debug(f"Processing from ID {status.last_processed_id + 1}")
                query = self.session.query(Aggregate.i, Aggregate.normalized_name, Aggregate.source)\
                    .filter(Aggregate.i > status.last_processed_id)\
                    .order_by(Aggregate.i)\
                    .limit(self.batch_size)
                logging.info(f"Executing SQL: {query}")
                data = pd.read_sql(
                    query.statement,
                    self.session.bind
                )

                logging.info(f"Query executed. Data shape: {data.shape}")

                if data.empty:
                    logging.info("No more data to process.")
                    break

                logging.info(f"Fetched {len(data)} rows for processing.")
                all_normalized_names = data['normalized_name'].tolist()
                
                # Add more logging to check the contents of the data
                logging.debug(f"Data fetched: {data.head()}")

                chunks = [data.iloc[i:i + len(data) // self.num_cores] for i in range(0, len(data), len(data) // self.num_cores)]
                args = [(chunk, all_normalized_names, self.percentage_score) for chunk in chunks]

                with Pool(processes=self.num_cores) as pool:
                    results = list(tqdm(pool.imap(find_matches_and_scores, args), total=len(args), desc="Finding matches"))

                matches = [match for result in results for match in result]
                self.insert_in_batches(matches)

                status.last_processed_id = int(data['i'].max())
                status.processed_records += int(len(data))
                self.session.commit()
                pbar.update(len(data))

                elapsed_time = time.time() - start_time
                logging.info(f"Processed {status.processed_records}/{total_rows} rows in {elapsed_time:.2f} seconds")

        logging.info("Processing complete")
        self.session.close()

    # Function to update normalized names in batches
    def update_normalized_names_batch(self, updates):
        """
        The function `update_normalized_names_batch` updates the `normalized_name` field in the `Aggregate`
        table for a batch of records provided in the `updates` parameter.
        
        :param updates: The `updates` parameter in the `update_normalized_names_batch` method is expected to
        be a dictionary containing the values that need to be updated in the database table. Each key-value
        pair in the dictionary represents the column to be updated and the new value to be set for that
        column in the database table
        """
        logging.debug(f"Updating batch of {len(updates)} records")
        update_stmt = (
            Aggregate.__table__.update()
            .where(Aggregate.i == bindparam('b_i'))
            .values(normalized_name=bindparam('normalized_name'))
        )
        try:
            self.session.execute(update_stmt, [{'b_i': update['i'], 'normalized_name': update['normalized_name']} for update in updates])
            self.session.commit()
        except Exception as e:
            logging.error(f"Error updating batch: {str(e)}")
            self.session.rollback()

    # Function to ensure necessary columns exist in the database
    def ensure_columns_exist(self):
        """
        This function ensures that specific columns exist in a table by adding them if they are missing.
        """
        inspector = inspect(self.engine)
        existing_columns = [col['name'] for col in inspector.get_columns('csu_redux')]

        with self.engine.begin() as connection:
            if 'normalized_name' not in existing_columns:
                connection.execute(text("ALTER TABLE csu_redux ADD COLUMN normalized_name VARCHAR"))
                logging.info("Added 'normalized_name' column to csu_redux table")

            if 'matched' not in existing_columns:
                connection.execute(text("ALTER TABLE csu_redux ADD COLUMN matched BOOLEAN DEFAULT FALSE"))
                logging.info("Added 'matched' column to csu_redux table")

    # Function to ensure necessary columns exist in the fuzzy match results table
    def ensure_fuzzy_match_results_columns_exist(self):
        """
        The function `ensure_fuzzy_match_results_columns_exist` checks and adds specific columns to a
        table if they do not already exist.
        """
        inspector = inspect(self.engine)
        existing_columns = [col['name'] for col in inspector.get_columns(self.output_table_name)]

        with self.engine.begin() as connection:
            if 'source_string' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.output_table_name} ADD COLUMN source_string VARCHAR"))
                logging.info(f"Added 'source_string' column to {self.output_table_name} table")

            if 'matched_string' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.output_table_name} ADD COLUMN matched_string VARCHAR"))
                logging.info(f"Added 'matched_string' column to {self.output_table_name} table")

            if 'similarity_score' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.output_table_name} ADD COLUMN similarity_score FLOAT"))
                logging.info(f"Added 'similarity_score' column to {self.output_table_name} table")

            if 'source_id' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.output_table_name} ADD COLUMN source_id INTEGER"))
                logging.info(f"Added 'source_id' column to {self.output_table_name} table")

            if 'matched_id' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.output_table_name} ADD COLUMN matched_id INTEGER"))
                logging.info(f"Added 'matched_id' column to {self.output_table_name} table")

            if 'validation_status' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.output_table_name} ADD COLUMN validation_status VARCHAR"))
                logging.info(f"Added 'validation_status' column to {self.output_table_name} table")

            if 'source' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.output_table_name} ADD COLUMN source VARCHAR"))
                logging.info(f"Added 'source' column to {self.output_table_name} table")

    # Function to set up logging
    def _setup_logging(self):
        """
        The function sets up logging and creates necessary tables for a FuzzyMatchResult object.
        """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Function to create necessary tables
    def _create_tables(self):
        """
        The function `_create_tables` sets the table name for `FuzzyMatchResult`, creates all tables in the
        database, and ensures the existence of certain columns.
        """
        FuzzyMatchResult.set_table_name(self.output_table_name)
        Base.metadata.create_all(self.engine)
        self.ensure_columns_exist()
        self.ensure_fuzzy_match_results_columns_exist()

    def reset_processing_status(self):
        self.session.query(ProcessingStatus).delete()
        self.session.commit()
        status = ProcessingStatus(last_processed_id=0, total_records=0, processed_records=0)
        self.session.add(status)
        self.session.commit()

    def update_normalized_names(self):
        logging.info("Updating normalized names...")
        query = self.session.query(Aggregate).filter(Aggregate.normalized_name.is_(None))
        total = query.count()
        logging.info(f"Total records to update: {total}")

        batch_size = 10000
        for i in tqdm(range(0, total, batch_size), desc="Updating normalized names"):
            batch = query.limit(batch_size).offset(i).all()
            updates = []
            for record in batch:
                normalized = normalize_name(record.supplier_name)
                if normalized:  # Only add to updates if normalized name is not empty
                    updates.append({'i': record.i, 'normalized_name': normalized})
            
            if updates:  # Only call update_normalized_names_batch if there are updates
                self.update_normalized_names_batch(updates)
        
        logging.info("Finished updating normalized names.")

if __name__ == "__main__":
    # Define the database URL for PostgreSQL
    db_url = "postgresql://overlord:password@localhost:5432/csu"

    # Prompt user for percentage score, defaulting to 85 if no input is provided
    percentage_score = float(input("Enter the percentage score to use for matching (default 85): ") or 85)
    
    # Prompt user for number of cores to use
    num_cores_input = input("Enter the number of cores to use (default max): ")
    # Convert input to integer if provided, otherwise set to None (which will use max cores)
    num_cores = int(num_cores_input) if num_cores_input.strip() else None
    
    # Prompt user for batch size, defaulting to 200000 if no input is provided
    batch_size = int(input("Enter the batch size (default 200000): ") or 200000)

    # Initialize the MainDBProcessor with user-provided parameters
    processor = MainDBProcessor(db_url, percentage_score, num_cores, batch_size)
    
    # Record the start time for performance measurement
    start_time = time.time()
    
    # Check and log current memory usage
    processor.check_memory_usage()
    
    # Call this method before process_suppliers
    processor.reset_processing_status()
    processor.update_normalized_names()
    
    # Start the main processing of suppliers
    processor.process_suppliers()
    
    # Record the end time after processing is complete
    end_time = time.time()

    # Calculate and log the total processing time
    elapsed_time = end_time - start_time
    logging.info(f"Total processing time: {elapsed_time:.2f} seconds")

    # Begin displaying final statistics
    
    # Create a new database engine and session
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Query the total number of records in the Aggregate table
    total_records = session.query(func.count(Aggregate.i)).scalar()
    
    # Query the number of processed (matched) records
    processed_records = session.query(func.count(Aggregate.i)).filter(Aggregate.matched == True).scalar()
    
    # Calculate the number of remaining (unprocessed) records
    remaining_records = total_records - processed_records

    # Log the statistics
    logging.info(f"Total records: {total_records}")
    logging.info(f"Processed records: {processed_records}")
    logging.info(f"Remaining records: {remaining_records}")

    # Query the number of unmatched records
    unmatched_records = session.query(func.count(Aggregate.i)).filter(Aggregate.matched == False).scalar()
    
    # Query the last processed record from the ProcessingStatus table
    last_processed = session.query(ProcessingStatus).first()

    # Log additional statistics
    logging.info(f"Total records in Aggregate table: {total_records}")
    logging.info(f"Unmatched records: {unmatched_records}")
    logging.info(f"Last processed ID: {last_processed.last_processed_id if last_processed else 'None'}")

    # Close the database session
    session.close()