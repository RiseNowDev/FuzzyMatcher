import pandas as pd
from fuzzywuzzy import fuzz
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def normalize_name(name: str) -> str:
    if not isinstance(name, str):
        name = str(name)
    return name.strip().lower()


def find_matches_and_scores(args: tuple) -> tuple:
    index, normalized_name, original_data, percentage_score, supplier_name, i = args
    matches = []
    if 'Normalized_Name' in original_data.columns:
        for idx, row in original_data.iterrows():
            score = fuzz.token_set_ratio(normalized_name, row['Normalized_Name'])
            if score > percentage_score:
                matches.append(f"{row[supplier_name]}|{row[i]}|{idx}|{score}")
    return index, ', '.join(matches)


def process_suppliers(input_file, output_file, num_cores=None, percentage_score=89,
                      supplier_name='supplier_name', i='i'):
    data = pd.read_csv(input_file)
    total_rows = len(data)
    rows_per_core = total_rows // (num_cores or cpu_count())

    # Normalize supplier names
    if 'Normalized_Name' not in data.columns:
        data['Normalized_Name'] = data[supplier_name].apply(normalize_name)

    args = [(idx, row['Normalized_Name'], data, percentage_score, supplier_name, i) for idx, row in data.iterrows()]

    with Pool(processes=num_cores or cpu_count()) as pool, tqdm(total=total_rows, desc="Processing suppliers"):
        results = list(pool.imap(find_matches_and_scores, args, chunksize=1))

    print(f"Results: {results}")

    result_dict = {index: match for index, match in results}
    data['Potential_Matches'] = data.index.map(result_dict)
    data.to_excel(output_file, index=False, engine='openpyxl')

    end_time = time.time()
    elapsed_time = end_time - start_time
    estimated_time = elapsed_time / (total_rows / rows_per_core)
    print(f"Total processing time: {elapsed_time:.2f} seconds")
    print(f"Estimated time to completion: {estimated_time:.2f} seconds")


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
