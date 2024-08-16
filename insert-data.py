#!/usr/bin/python3

import json
import uuid
import argparse
import requests
import threading
import sys

# Configuration
ZINCSEARCH_URL = 'http://localhost:4080'
USERNAME = 'admin'              # Replace with your actual username
PASSWORD = 'Complexpass#123'   # Replace with your actual password

def convert_to_bulk_format(documents, index_name):
    bulk_lines = []
    for doc in documents:
        # Create the index action metadata line
        index_action = json.dumps({"index": {"_index": index_name}})
        # Create the document line
        doc_line = json.dumps(doc)
        bulk_lines.append(index_action)
        bulk_lines.append(doc_line)
    return "\n".join(bulk_lines) + "\n"

def bulk_insert(bulk_data):
    try:
        response = requests.post(
            f'{ZINCSEARCH_URL}/api/_bulk',
            auth=(USERNAME, PASSWORD),
            headers={'Content-Type': 'application/x-ndjson'},
            data=bulk_data
        )
        response.raise_for_status()
        print("Successfully inserted documents.")
    except requests.exceptions.RequestException as e:
        print(f"Error inserting documents: {e}")

def process_input(index_name):
    documents = []
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break
            line = line.strip()
            if line:
                try:
                    doc = json.loads(line)
                    doc['id'] = str(uuid.uuid4())  # Optionally generate a UUID for the id
                    documents.append(doc)
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON line: {line}")
        except KeyboardInterrupt:
            break
    
    if documents:
        bulk_data = convert_to_bulk_format(documents, index_name)
        bulk_insert(bulk_data)

def main():
    parser = argparse.ArgumentParser(description='Convert JSON to Bulk API format and insert into ZincSearch.')
    parser.add_argument('index_name', help='Name of the index in ZincSearch.')

    args = parser.parse_args()

    # Start processing input in a separate thread
    worker_thread = threading.Thread(target=process_input, args=(args.index_name,))
    worker_thread.start()
    worker_thread.join()

if __name__ == "__main__":
    main()
