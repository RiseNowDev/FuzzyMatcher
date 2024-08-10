import pandas as pd
from rapidfuzz import process, fuzz
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import time
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, func, Text, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect
import psutil
import numpy as np
from collections import defaultdict
import multiprocessing as mp
from sqlalchemy.exc import OperationalError, InternalError
import random
import re

# Move these global variable definitions to the top of the file
total_processed = mp.Value('i', 0)
total_to_process = mp.Value('i', 0)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Base = declarative_base()


class Aggregate(Base):
    __tablename__ = 'aggregated_spend'
    i = Column(Integer, primary_key=True)
    supplier_name = Column(String)
    normalized_name = Column(String)
    matched = Column(Boolean)  # Changed from String to Boolean


class FuzzyMatchResult(Base):
    __tablename__ = 'fuzzy_match_results'
    id = Column(Integer, primary_key=True)
    source_string = Column(String)
    matched_string = Column(String)
    similarity_score = Column(Float)
    source_id = Column(Integer)
    matched_id = Column(Integer)
    validation_status = Column(String)


class ProcessingStatus(Base):
    __tablename__ = 'processing_status'
    id = Column(Integer, primary_key=True)
    last_processed_id = Column(Integer)
    total_records = Column(Integer)
    processed_records = Column(Integer)


def normalize_name(name: str) -> str:
    if not name:
        return ''
    name = re.sub(r'\d+', '', name)  # Remove digits
    name = re.sub(r'[^\w\s]', ' ', name)  # Replace special characters with space
    name = name.lower()  # Convert to lowercase
    return ' '.join(name.split())  # Remove extra whitespace and join words


def weighted_ratio(s1, s2, score_cutoff=0):
    """
    Custom weighted ratio function combining multiple string similarity metrics.
    """
    token_sort_ratio = fuzz.token_sort_ratio(s1, s2)
    token_set_ratio = fuzz.token_set_ratio(s1, s2)
    partial_ratio = fuzz.partial_ratio(s1, s2)

    # Calculate weighted average
    weighted_score = (0.4 * token_sort_ratio +
                      0.4 * token_set_ratio +
                      0.2 * partial_ratio)

    return weighted_score


def find_matches_and_scores(args: tuple) -> list:
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
        # Filter results based on percentage_score after extraction
        filtered_results = [match for match in results if match[1] >= percentage_score]
        new_matches = [(row['i'], normalized_name, all_normalized_names[match[2]], match[2], match[1])
                       for match in filtered_results if row['i'] < match[2]]
        matches.extend(new_matches)
        with total_processed.get_lock():
            total_processed.value += 1
    return matches


def insert_in_batches(session, data, batch_size=10000):
    total_inserted = 0
    pbar = tqdm(total=len(data), desc="Inserting matches", unit="match")
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        try:
            stmt = insert(FuzzyMatchResult).values([
                {
                    "source_id": match[0],
                    "source_string": match[1],
                    "matched_string": match[2],
                    "matched_id": match[3],
                    "similarity_score": match[4]
                }
                for match in batch
            ])
            result = session.execute(stmt)
            session.commit()
            total_inserted += result.rowcount
            pbar.update(len(batch))
        except Exception as e:
            logging.error(f"Error inserting batch into fuzzy_match_results: {str(e)}")
            session.rollback()
    pbar.close()
    logging.info(f"Total records inserted into fuzzy_match_results: {total_inserted}")
    return total_inserted


def ensure_columns_exist(engine):
    inspector = inspect(engine)
    existing_columns = [col['name'] for col in inspector.get_columns('aggregated_spend')]

    with engine.begin() as connection:
        if 'normalized_name' not in existing_columns:
            connection.execute(text("ALTER TABLE aggregated_spend ADD COLUMN normalized_name VARCHAR"))
            logging.info("Added 'normalized_name' column to aggregated_spend table")

        if 'matched' not in existing_columns:
            connection.execute(text("ALTER TABLE aggregated_spend ADD COLUMN matched BOOLEAN DEFAULT FALSE"))
            logging.info("Added 'matched' column to aggregated_spend table")


def check_memory_usage():
    memory = psutil.virtual_memory()
    if memory.available < 5 * 1024 * 1024 * 1024:  # Less than 5GB available
        logging.warning("Available memory is running low. Consider reducing batch size or freeing up memory.")


def retry_on_db_error(func):
    def wrapper(*args, **kwargs):
        max_retries = 5
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except (OperationalError, InternalError) as e:
                if attempt == max_retries - 1:
                    raise
                logging.warning(f"Database error occurred: {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay + random.uniform(0, 1))
                retry_delay *= 2

    return wrapper


@retry_on_db_error
def update_normalized_names_batch(session, updates):
    try:
        for aggregate_id, normalized_name in updates:
            session.query(Aggregate).filter(Aggregate.i == aggregate_id).update({"normalized_name": normalized_name})
        session.commit()
    except Exception as e:
        logging.error(f"Error updating normalized names: {str(e)}")
        session.rollback()
        # If the transaction fails, try updating one by one
        for aggregate_id, normalized_name in updates:
            try:
                session.query(Aggregate).filter(Aggregate.i == aggregate_id).update(
                    {"normalized_name": normalized_name})
                session.commit()
            except Exception as inner_e:
                logging.error(f"Error updating single record (id={aggregate_id}): {str(inner_e)}")
                session.rollback()


def process_suppliers(db_url, num_cores=None, percentage_score=85, batch_size=100000):
    global total_to_process  # This line is no longer necessary, but can be kept for clarity

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)

    ensure_columns_exist(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    # Get or create the processing status
    status = session.query(ProcessingStatus).first()
    if status is None:
        status = ProcessingStatus(last_processed_id=0, total_records=0, processed_records=0)
        session.add(status)
        session.commit()

    # Check if all records are unmatched
    total_records = session.query(func.count(Aggregate.i)).scalar()
    unmatched_records = session.query(func.count(Aggregate.i)).filter(Aggregate.matched == False).scalar()

    if unmatched_records == total_records:
        logging.info("All records are unmatched. Resetting the processing status.")
        status.last_processed_id = 0
        status.processed_records = 0
        session.commit()

    # Load all normalized names
    logging.info("Loading all normalized names...")
    with tqdm(total=total_records, desc="Loading normalized names") as pbar:
        all_normalized_names = []
        for chunk in pd.read_sql_query(session.query(Aggregate.i, Aggregate.normalized_name).statement, engine,
                                       chunksize=100000):
            all_normalized_names.extend([(id, name) for id, name in chunk.values if name])
            pbar.update(len(chunk))

    id_to_name = dict(all_normalized_names)
    all_normalized_names = list(id_to_name.values())
    logging.info(f"Loaded {len(all_normalized_names)} normalized names")

    # Fetch unprocessed rows
    query = session.query(Aggregate).filter(
        Aggregate.i > status.last_processed_id,
        (Aggregate.matched.is_(None)) | (Aggregate.matched == False)
    )
    total_rows = query.count()
    num_cores = num_cores or cpu_count()

    logging.info(f"Total unprocessed rows: {total_rows}")
    logging.info(f"Using {num_cores} cores")

    total_to_process.value = total_rows
    start_time = time.time()

    with tqdm(total=total_rows, desc="Overall progress") as pbar:
        for offset in range(0, total_rows, batch_size):
            check_memory_usage()

            # Modify the query to only fetch rows where normalized_name is NULL or empty
            data = pd.read_sql_query(
                query.filter((Aggregate.normalized_name == None) | (Aggregate.normalized_name == ''))
                .order_by(Aggregate.i).offset(offset).limit(batch_size).statement,
                engine
            )

            if data.empty:
                logging.info("No more unnormalized rows found. Moving to matching phase.")
                break

            # Update normalized_name in batches of 100
            updates = []
            with tqdm(total=len(data), desc="Normalizing names") as update_pbar:
                for _, row in data.iterrows():
                    normalized_name = normalize_name(row['supplier_name'])
                    updates.append((row['i'], normalized_name))
                    update_pbar.update(1)

                    if len(updates) == 100:
                        update_normalized_names_batch(session, updates)
                        updates = []

                # Update any remaining names
                if updates:
                    update_normalized_names_batch(session, updates)

            # Refresh data with all rows in this batch, including those already normalized
            data = pd.read_sql_query(
                query.order_by(Aggregate.i).offset(offset).limit(batch_size).statement,
                engine
            )

            chunk_size = len(data) // num_cores
            data_chunks = np.array_split(data, num_cores)

            args = [(chunk, all_normalized_names, percentage_score) for chunk in data_chunks]

            with Pool(processes=num_cores) as pool:
                results = list(
                    tqdm(pool.imap(find_matches_and_scores, args), total=len(args), desc="Processing suppliers"))

            matches = [match for chunk_result in results for match in chunk_result]

            # Insert matches into fuzzy_match_results table
            logging.info(f"Inserting {len(matches)} matches into fuzzy_match_results")
            insert_in_batches(session, matches)

            # Update matched status in the Aggregate table
            with tqdm(total=len(data), desc="Updating matched status") as update_pbar:
                for i in data['i']:
                    session.query(Aggregate).filter(Aggregate.i == i).update({Aggregate.matched: True})
                    update_pbar.update(1)
            session.commit()

            # Update processing status
            status.last_processed_id = int(data['i'].max())
            status.processed_records += len(data)
            session.commit()

            # Update the overall progress bar
            pbar.update(len(data))

            # Calculate and display progress
            elapsed_time = time.time() - start_time
            processed_rows = status.processed_records
            remaining_rows = total_rows - processed_rows
            estimated_total_time = (elapsed_time / processed_rows) * total_rows if processed_rows > 0 else 0
            estimated_remaining_time = max(0, estimated_total_time - elapsed_time)

            logging.info(f"Records processed: {processed_rows}")
            logging.info(f"Records remaining: {remaining_rows}")
            logging.info(f"Estimated time to completion: {estimated_remaining_time:.2f} seconds")

            # Add debug logging
            logging.info(f"Sample of normalized names: {data['normalized_name'].head().tolist()}")
            logging.info(f"Number of non-empty normalized names: {data['normalized_name'].notna().sum()}")

    session.close()

    logging.info("Processing complete and database updated.")


if __name__ == "__main__":
    db_url = "postgresql://overlord:password@localhost:5432/postgres"

    percentage_score = float(input("Enter the percentage score to use for matching (default 85): ") or 85)
    num_cores_input = input("Enter the number of cores to use (default max): ")
    num_cores = int(num_cores_input) if num_cores_input.strip() else None
    batch_size = int(input("Enter the batch size (default 200000): ") or 200000)

    start_time = time.time()
    check_memory_usage()
    process_suppliers(db_url, num_cores, percentage_score, batch_size)
    end_time = time.time()

    elapsed_time = end_time - start_time
    logging.info(f"Total processing time: {elapsed_time:.2f} seconds")

    # Display final statistics
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    total_records = session.query(func.count(Aggregate.i)).scalar()
    processed_records = session.query(func.count(Aggregate.i)).filter(Aggregate.matched == True).scalar()
    remaining_records = total_records - processed_records

    logging.info(f"Total records: {total_records}")
    logging.info(f"Processed records: {processed_records}")
    logging.info(f"Remaining records: {remaining_records}")

    unmatched_records = session.query(func.count(Aggregate.i)).filter(Aggregate.matched == False).scalar()
    last_processed = session.query(ProcessingStatus).first()

    logging.info(f"Total records in Aggregate table: {total_records}")
    logging.info(f"Unmatched records: {unmatched_records}")
    logging.info(f"Last processed ID: {last_processed.last_processed_id if last_processed else 'None'}")

    session.close()
