from query_pipeline import SQLQueryPipeline
from history import ChatHistory
import json


def display_results(results: dict):
    """Display results in a structured format"""
    print("\n" + "="*60)
    print("QUERY ANALYSIS")
    print("="*60)
    print(f"Tables used: {results['tables']}")
    print(f"Joins: {results['joins']}")
    print(f"Columns selected: {results['columns']}")
    print(f"SQL Query: {results['sql_query']}")
    
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    
    if results['results']:
        # Display as table
        headers = list(results['results'][0].keys())
        print(" | ".join(headers))
        print("-" * (len(" | ".join(headers))))
        for row in results['results']:
            print(" | ".join(str(row[col]) for col in headers))
        print(f"\nTotal rows: {len(results['results'])}")
    else:
        print("No results found.")
    print("="*60)


def main():
    print("Welcome to the SQL Chatbot with Stepwise Pipeline!")
    print("Type 'exit' to quit. Type 'schema' to see DB schema.\n")
    
    pipeline = SQLQueryPipeline()
    history = ChatHistory()

    while True:
        user_input = input("\nYou: ")
        if user_input.lower() == 'exit':
            break
        if user_input.lower() == 'schema':
            print("\nDatabase schema:")
            for table, info in pipeline.schema.items():
                print(f"- {table}: columns={info['columns']}, foreign_keys={info['foreign_keys']}")
            print()
            continue
            
        try:
            # Process query through the stepwise pipeline
            results = pipeline.process_query(user_input)
            
            # Display results in structured format
            display_results(results)
            
            # Store in history
            history.add(user_input, json.dumps(results, indent=2))
            
        except Exception as e:
            print(f"\nError: {e}")
            history.add(user_input, f"Error: {e}")

    print("\nChat History:")
    for turn in history.get():
        print(f"\nYou: {turn['user']}")
        print(f"Bot: Query processed through stepwise pipeline")


if __name__ == "__main__":
    main()