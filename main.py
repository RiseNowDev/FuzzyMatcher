import pandas as pd
from rapidfuzz import process, fuzz
from multiprocessing import Pool, cpu_count
import time
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, func, text, MetaData, bindparam
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect
import psutil
import datetime
from utils import weighted_ratio, normalize_name, find_matches_and_scores

# Define the Base class for ORM models
Base = declarative_base()

# Define ORM models
class Aggregate(Base):
    __tablename__ = 'csu_redux'  # This will be overridden
    i = Column(Integer, primary_key=True)
    supplier_name = Column(String)
    normalized_name = Column(String)
    matched = Column(Boolean, default=False)
    source = Column(String)

    @classmethod
    def set_table_name(cls, name):
        cls.__table__.name = name

class ProcessingStatus(Base):
    __tablename__ = 'processing_status'
    id = Column(Integer, primary_key=True)
    last_processed_id = Column(Integer)
    total_records = Column(Integer)
    processed_records = Column(Integer)

class FuzzyMatchResult(Base):
    __tablename__ = 'fuzzy_match_results'  # This will be overridden
    id = Column(Integer, primary_key=True)
    source_string = Column(String)
    matched_string = Column(String)
    similarity_score = Column(Float)
    source_id = Column(Integer)
    matched_id = Column(Integer)
    validation_status = Column(String)
    source = Column(String)

    @classmethod
    def set_table_name(cls, name):
        cls.__table__.name = name

class MainDBProcessor:
    def __init__(self, db_url, input_table_name, percentage_score=85, num_cores=None, batch_size=200000):
        self.db_url = db_url
        self.input_table_name = input_table_name
        self.percentage_score = percentage_score
        self.num_cores = num_cores or cpu_count()
        self.batch_size = batch_size
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_table_name = f'fuzzy_match_results_{timestamp}'
        
        self._setup_logging()
        self._create_tables()
        self.ensure_columns_exist()

    def check_memory_usage(self):
        memory = psutil.virtual_memory()
        self.log_status(f"Memory usage: {memory.percent}%")

    def insert_in_batches(self, matches):
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

    def process_suppliers(self):
        total_rows = self.session.query(func.count(Aggregate.i)).scalar()
        self.log_status(f"Total rows in Aggregate table: {total_rows}")

        status = self.session.query(ProcessingStatus).first()
        if not status:
            status = ProcessingStatus(last_processed_id=0, total_records=total_rows, processed_records=0)
            self.session.add(status)
            self.session.commit()

        self.log_status(f"Current processing status: last_processed_id={status.last_processed_id}, total_records={status.total_records}, processed_records={status.processed_records}")

        start_time = time.time()
        last_report_time = start_time
        while status.processed_records < total_rows:
            query = self.session.query(Aggregate.i, Aggregate.normalized_name, Aggregate.source)\
                .filter(Aggregate.i > status.last_processed_id)\
                .order_by(Aggregate.i)\
                .limit(self.batch_size)
            data = pd.read_sql(query.statement, self.session.bind)

            if data.empty:
                self.log_status("No more data to process.")
                break

            all_normalized_names = data['normalized_name'].tolist()
            chunks = [data.iloc[i:i + len(data) // self.num_cores] for i in range(0, len(data), len(data) // self.num_cores)]
            args = [(chunk, all_normalized_names, self.percentage_score) for chunk in chunks]

            with Pool(processes=self.num_cores) as pool:
                results = pool.map(find_matches_and_scores, args)

            matches = [match for result in results for match in result]
            self.insert_in_batches(matches)

            status.last_processed_id = int(data['i'].max())
            status.processed_records += int(len(data))
            self.session.commit()

            current_time = time.time()
            if current_time - last_report_time >= 3:
                elapsed_time = current_time - start_time
                progress = (status.processed_records / total_rows) * 100
                self.log_status(f"Progress: {progress:.2f}% | Processed {status.processed_records}/{total_rows} rows in {elapsed_time:.2f} seconds")
                last_report_time = current_time

        self.log_status("Processing complete")
        self.session.close()

    def update_normalized_names_batch(self, updates):
        update_stmt = (
            Aggregate.__table__.update()
            .where(Aggregate.i == bindparam('b_i'))
            .values(normalized_name=bindparam('normalized_name'))
        )
        try:
            self.session.execute(update_stmt, [{'b_i': update['i'], 'normalized_name': update['normalized_name']} for update in updates])
            self.session.commit()
        except Exception as e:
            self.log_error(f"Error updating batch: {str(e)}")
            self.session.rollback()

    def ensure_columns_exist(self):
        inspector = inspect(self.engine)
        existing_columns = [col['name'] for col in inspector.get_columns(self.input_table_name)]

        with self.engine.begin() as connection:
            if 'normalized_name' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.input_table_name} ADD COLUMN normalized_name VARCHAR"))
                self.log_status(f"Added 'normalized_name' column to {self.input_table_name} table")

            if 'matched' not in existing_columns:
                connection.execute(text(f"ALTER TABLE {self.input_table_name} ADD COLUMN matched BOOLEAN DEFAULT FALSE"))
                self.log_status(f"Added 'matched' column to {self.input_table_name} table")

    def ensure_fuzzy_match_results_columns_exist(self):
        inspector = inspect(self.engine)
        existing_columns = [col['name'] for col in inspector.get_columns(self.output_table_name)]

        required_columns = [
            ('source_string', 'VARCHAR'),
            ('matched_string', 'VARCHAR'),
            ('similarity_score', 'FLOAT'),
            ('source_id', 'INTEGER'),
            ('matched_id', 'INTEGER'),
            ('validation_status', 'VARCHAR'),
            ('source', 'VARCHAR')
        ]

        with self.engine.begin() as connection:
            for col_name, col_type in required_columns:
                if col_name not in existing_columns:
                    connection.execute(text(f"ALTER TABLE {self.output_table_name} ADD COLUMN {col_name} {col_type}"))
                    self.log_status(f"Added '{col_name}' column to {self.output_table_name} table")

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def _create_tables(self):
        Aggregate.set_table_name(self.input_table_name)
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
        self.log_status("Updating normalized names...")
        query = self.session.query(Aggregate).filter(Aggregate.normalized_name.is_(None))
        total = query.count()
        self.log_status(f"Total records to update: {total}")

        batch_size = 10000
        for i in range(0, total, batch_size):
            batch = query.limit(batch_size).offset(i).all()
            updates = [{'i': record.i, 'normalized_name': normalize_name(record.supplier_name)} for record in batch if normalize_name(record.supplier_name)]
            
            if updates:
                self.update_normalized_names_batch(updates)
            
            self.log_status(f"Updated {min(i + batch_size, total)}/{total} records")
        
        self.log_status("Finished updating normalized names.")

    def create_joined_view(self):
        view_name = f"joined_view_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        query = f"""
        CREATE VIEW {view_name} AS
        SELECT a.*, f.matched_string, f.similarity_score, f.validation_status
        FROM {self.input_table_name} a
        LEFT JOIN {self.output_table_name} f ON a.i = f.source_id
        """
        with self.engine.begin() as connection:
            connection.execute(text(query))
        self.log_status(f"Created joined view: {view_name}")

    def log_status(self, message):
        print(f"\033[94m[STATUS] {message}\033[0m")

    def log_error(self, message):
        print(f"\033[91m[ERROR] {message}\033[0m")

def main():
    db_url = "postgresql://overlord:password@localhost:5432/csu"
    input_table_name = input("Enter the input table name: ")
    percentage_score = float(input("Enter the percentage score to use for matching (default 85): ") or 85)
    num_cores_input = input("Enter the number of cores to use (default max): ")
    num_cores = int(num_cores_input) if num_cores_input.strip() else None
    batch_size = int(input("Enter the batch size (default 200000): ") or 200000)

    processor = MainDBProcessor(db_url, input_table_name, percentage_score, num_cores, batch_size)
    
    start_time = time.time()
    
    processor.check_memory_usage()
    processor.reset_processing_status()
    processor.update_normalized_names()
    processor.process_suppliers()
    processor.create_joined_view()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    processor.log_status(f"Total processing time: {elapsed_time:.2f} seconds")

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    total_records = session.query(func.count(Aggregate.i)).scalar()
    processed_records = session.query(func.count(Aggregate.i)).filter(Aggregate.matched == True).scalar()
    remaining_records = total_records - processed_records
    unmatched_records = session.query(func.count(Aggregate.i)).filter(Aggregate.matched == False).scalar()
    last_processed = session.query(ProcessingStatus).first()

    processor.log_status(f"Total records: {total_records}")
    processor.log_status(f"Processed records: {processed_records}")
    processor.log_status(f"Remaining records: {remaining_records}")
    processor.log_status(f"Unmatched records: {unmatched_records}")
    processor.log_status(f"Last processed ID: {last_processed.last_processed_id if last_processed else 'None'}")

    session.close()

if __name__ == "__main__":
    main()