import pandas as pd
from rapidfuzz import process, fuzz
from multiprocessing import Pool, cpu_count
import tqdm
import time
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, func, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Base = declarative_base()

class Normie(Base):
    __tablename__ = 'normie'
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
    return str(name).strip().lower()

def find_matches_and_scores(args: tuple) -> list:
    chunk, all_normalized_names, percentage_score = args
    matches = []
    for idx, row in chunk.iterrows():
        normalized_name = row['normalized_name']
        results = process.extract(
            normalized_name,
            all_normalized_names,
            scorer=fuzz.token_set_ratio,
            score_cutoff=percentage_score,
            limit=100
        )
        new_matches = [(row['i'], normalized_name, all_normalized_names[match[2]], match[2], match[1]) 
                        for match in results if row['i'] < match[2]]
        matches.extend(new_matches)
        if not new_matches:
            logging.info(f"No matches found for '{normalized_name}' with score >= {percentage_score}")
    logging.info(f"Total matches found in this chunk: {len(matches)}")
    return matches

def insert_in_batches(session, data, batch_size=1000):
    total_inserted = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        try:
            result = session.execute(
                insert(FuzzyMatchResult),
                [
                    {
                        "source_id": match[0],
                        "source_string": match[1],
                        "matched_string": match[2],
                        "matched_id": match[3],
                        "similarity_score": match[4]
                    }
                    for match in batch
                ]
            )
            session.commit()
            total_inserted += result.rowcount
        except Exception as e:
            logging.error(f"Error inserting batch into fuzzy_match_results: {str(e)}")
            session.rollback()

    logging.info(f"Total records inserted into fuzzy_match_results: {total_inserted}")
    return total_inserted

def process_suppliers(db_url, num_cores=None, percentage_score=89, batch_size=10000):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get or create the processing status
    status = session.query(ProcessingStatus).first()
    if status is None:
        status = ProcessingStatus(last_processed_id=0, total_records=0, processed_records=0)
        session.add(status)
        session.commit()

    # Check if all records are unmatched
    total_records = session.query(func.count(Normie.i)).scalar()
    unmatched_records = session.query(func.count(Normie.i)).filter(Normie.matched == False).scalar()

    if unmatched_records == total_records:
        logging.info("All records are unmatched. Resetting the processing status.")
        status.last_processed_id = 0
        status.processed_records = 0
        session.commit()

    # Fetch unprocessed rows
    query = session.query(Normie).filter(
        Normie.i > status.last_processed_id,
        Normie.matched == False
    )
    total_rows = query.count()
    num_cores = num_cores or cpu_count()

    logging.info(f"Total unprocessed rows: {total_rows}")
    logging.info(f"Using {num_cores} cores")

    start_time = time.time()

    for offset in range(0, total_rows, batch_size):
        data = pd.read_sql_query(
            query.order_by(Normie.i).offset(offset).limit(batch_size).statement,
            engine
        )

        if data.empty:
            logging.info("No more unprocessed rows found. Exiting.")
            break

        # Update normalized_name in the Normie table before processing
        for index, row in data.iterrows():
            normalized_name = normalize_name(row['supplier_name'])
            session.query(Normie).filter(Normie.i == row['i']).update({
                Normie.normalized_name: normalized_name,
            }, synchronize_session=False)
        session.commit()

        # Refresh data with updated normalized_names
        data = pd.read_sql_query(
            query.order_by(Normie.i).offset(offset).limit(batch_size).statement,
            engine
        )

        all_normalized_names = data['normalized_name'].tolist()

        chunk_size = len(data) // num_cores
        data_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        args = [(chunk, all_normalized_names, percentage_score) for chunk in data_chunks]

        with Pool(processes=num_cores) as pool:
            results = list(tqdm.tqdm(pool.imap(find_matches_and_scores, args), total=len(args), desc="Processing suppliers"))

        matches = [match for chunk_result in results for match in chunk_result]

        # Insert matches into fuzzy_match_results table
        logging.info(f"Inserting {len(matches)} matches into fuzzy_match_results")
        insert_in_batches(session, matches)

        # Update matched status in the Normie table
        for index, row in data.iterrows():
            session.query(Normie).filter(Normie.i == row['i']).update({
                Normie.matched: True
            }, synchronize_session=False)
        session.commit()

        # Update processing status
        status.last_processed_id = int(data['i'].max())  # Convert numpy.int64 to Python int
        status.processed_records += len(data)
        session.commit()  # Commit after each batch

        # Calculate and display progress
        elapsed_time = time.time() - start_time
        processed_rows = status.processed_records
        remaining_rows = total_rows - processed_rows
        estimated_total_time = (elapsed_time / processed_rows) * total_rows if processed_rows > 0 else 0
        estimated_remaining_time = max(0, estimated_total_time - elapsed_time)

        logging.info(f"Records processed: {processed_rows}")
        logging.info(f"Records remaining: {remaining_rows}")
        logging.info(f"Estimated time to completion: {estimated_remaining_time:.2f} seconds")

    session.close()

    logging.info("Processing complete and database updated.")

if __name__ == "__main__":
    db_url = "postgresql://overlord:password@localhost:5432/postgres"

    percentage_score = float(input("Enter the percentage score to use for matching (default 89): ") or 89)
    num_cores_input = input("Enter the number of cores to use (default max): ")
    num_cores = int(num_cores_input) if num_cores_input.strip() else None
    batch_size = int(input("Enter the batch size (default 10000): ") or 10000)

    start_time = time.time()
    process_suppliers(db_url, num_cores, percentage_score, batch_size)
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    logging.info(f"Total processing time: {elapsed_time:.2f} seconds")

    # Display final statistics
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    total_records = session.query(func.count(Normie.i)).scalar()
    processed_records = session.query(func.count(Normie.i)).filter(Normie.matched == True).scalar()
    remaining_records = total_records - processed_records

    logging.info(f"Total records: {total_records}")
    logging.info(f"Processed records: {processed_records}")
    logging.info(f"Remaining records: {remaining_records}")

    # Add these lines for debugging
    unmatched_records = session.query(func.count(Normie.i)).filter(Normie.matched == False).scalar()
    last_processed = session.query(ProcessingStatus).first()
    
    logging.info(f"Total records in Normie table: {total_records}")
    logging.info(f"Unmatched records: {unmatched_records}")
    logging.info(f"Last processed ID: {last_processed.last_processed_id if last_processed else 'None'}")
    
    session.close()