import pandas as pd
from rapidfuzz import process, fuzz
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_name(name: str) -> str:
    return str(name).strip().lower()

def find_matches_and_scores(args: tuple) -> tuple:
    chunk, all_normalized_names, percentage_score, supplier_name, i = args
    matches = []
    for idx, row in chunk.iterrows():
        normalized_name = row['Normalized_Name']
        results = process.extract(
            normalized_name,
            all_normalized_names,
            scorer=fuzz.token_set_ratio,
            score_cutoff=percentage_score,
            limit=10  # Adjust this value based on your needs
        )
        matches.append(', '.join([f"{all_normalized_names[match[2]]}|{match[2]}|{match[2]}|{match[1]}" for match in results]))
    return matches

def process_suppliers(input_file, output_file, num_cores=None, percentage_score=89,
                      supplier_name='supplier_name', i='i'):
    data = pd.read_csv(input_file)
    total_rows = len(data)
    num_cores = num_cores or cpu_count()

    logging.info(f"Total rows: {total_rows}")
    logging.info(f"Using {num_cores} cores")

    # Normalize supplier names
    if 'Normalized_Name' not in data.columns:
        data['Normalized_Name'] = data[supplier_name].apply(normalize_name)

    all_normalized_names = data['Normalized_Name'].tolist()

    # Split data into chunks for multiprocessing
    chunk_size = total_rows // num_cores
    data_chunks = [data[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]

    args = [(chunk, all_normalized_names, percentage_score, supplier_name, i) for chunk in data_chunks]

    with Pool(processes=num_cores) as pool:
        results = list(tqdm(pool.imap(find_matches_and_scores, args), total=len(args), desc="Processing suppliers"))

    data['Potential_Matches'] = [match for chunk_result in results for match in chunk_result]
    data.to_excel(output_file, index=False, engine='openpyxl')

    logging.info(f"Results saved to {output_file}")

if __name__ == "__main__":
    input_file = "raw/archive/s1.csv"
    output_file = "output/test_csu_snorm.xlsx"

    percentage_score = float(input("Enter the percentage score to use for matching (default 89): ") or 89)
    name_field = input("Enter the name field to use (default supplier_name): ") or "supplier_name"
    i = input("Enter the name of the column which is your index. (default i): ") or "i"
    num_cores_input = input("Enter the number of cores to use (default max): ")
    num_cores = int(num_cores_input) if num_cores_input.strip() else None

    start_time = time.time()
    process_suppliers(input_file, output_file, num_cores, percentage_score, name_field, i)
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    logging.info(f"Total processing time: {elapsed_time:.2f} seconds")