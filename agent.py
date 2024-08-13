from langchain_openai import ChatOpenAI
from langchain.agents import Tool, AgentExecutor
from langchain.agents.structured_chat.base import StructuredChatAgent
from langchain.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from typing import List, Union
from collections import defaultdict
import csv
import os
import concurrent.futures  # Add this import
from tqdm import tqdm  # Add this import for the progress bar

# Ensure your API key is set in the environment variable
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set")

# If you have an organization ID, set it in the environment variable
org_id = os.getenv("OPENAI_ORG_ID")

# Initialize ChatOpenAI with the custom model and organization ID if available
llm = ChatOpenAI(
    model_name="gpt-4o-mini-2024-07-18", # do not change this
    temperature=0,
    openai_api_key=api_key,
    organization=org_id if org_id else None,  # Add this line to use org_id if available
)

# Define a custom prompt template
prompt = ChatPromptTemplate.from_template(
    """You are an AI assistant tasked with suggesting a new name for grouped supplier entries.
Given the following information:
1. Source string: {source_string}
2. Matched string: {matched_string}
3. Similarity score: {similarity_score}
4. Source: {source}

Your task is to suggest an appropriate new name that represents this group of suppliers.
Consider the following guidelines:
- If the source_string and matched_string are identical or very similar, use that as the new name.
- If they differ, choose the more general or commonly used name.
- Remove any unnecessary details or specific locations unless they're crucial for identification.
- Ensure the name is clear, concise, and representative of the supplier group.

Current new name: {current_new_name}

Suggest a new name for this group:"""
)

# Define a tool for suggesting new names
def suggest_new_name(input_string):
    # Check if the input is a structured string or just a single value
    if ': ' in input_string:
        # Parse the structured input string
        lines = input_string.strip().split('\n')
        data = {}
        for line in lines:
            if ': ' in line:
                key, value = line.split(': ', 1)
                data[key.strip()] = value.strip()
    else:
        # If it's just a single value, use it as the source_string
        data = {
            "source_string": input_string,
            "matched_string": input_string,
            "similarity_score": "1.0",
            "source": "unknown",
            "current_new_name": input_string
        }

    chain = prompt | llm | StrOutputParser()
    return chain.invoke(data)

tools = [
    Tool(
        name="SuggestNewName",
        func=suggest_new_name,
        description="Useful for suggesting a new name for a group of suppliers. Provide either a structured input with all details or just the supplier name."
    )
]

# Define the agent
agent = StructuredChatAgent.from_llm_and_tools(llm=llm, tools=tools)

agent_executor = AgentExecutor.from_agent_and_tools(agent=agent, tools=tools, verbose=True)

def process_combination(source_string, matched_string, row):
    result = agent_executor.invoke({
        "input": f"""
        source_string: {source_string}
        matched_string: {matched_string}
        similarity_score: {row['similarity_score']}
        source: {row['source']}
        current_new_name: {row['new_name']}
        """
    })
    return (source_string, matched_string, result['output'])

def process_csv(input_file, output_file):
    # First pass: Gather all unique combinations and their occurrences
    unique_combinations = defaultdict(list)
    with open(input_file, 'r', newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            key = (row['source_string'], row['matched_string'])
            unique_combinations[key].append(row)

    # Process unique combinations with threading
    name_suggestions = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  # Use ThreadPoolExecutor
        futures = []
        for (source_string, matched_string), rows in unique_combinations.items():
            row = rows[0]
            futures.append(executor.submit(process_combination, source_string, matched_string, row))

        # Use tqdm to create a progress bar
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            source_string, matched_string, suggested_name = future.result()
            name_suggestions[(source_string, matched_string)] = suggested_name

    # Second pass: Write the output with suggested names
    with open(input_file, 'r', newline='') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['suggested_new_name']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            key = (row['source_string'], row['matched_string'])
            row['suggested_new_name'] = name_suggestions[key]
            writer.writerow(row)

# Main execution
if __name__ == "__main__":
    input_file = "output/ars_dump2.csv"  # Replace with your input CSV file path
    output_file = "normalized.csv"  # Replace with your desired output CSV file path
    process_csv(input_file, output_file)
    print(f"Processing complete. Results written to {output_file}")