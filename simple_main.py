 # simple_main.py - Step 1: Testing Single Table Queries

from simple_pipeline import SimpleSQLPipeline


def display_results(result):
    """Display results in a nice format"""
    print("\n" + "="*60)

    if result['success']:
        print("✅ SUCCESS")
        print(f"Question: {result['user_question']}")
        print(f"Table: {result['target_table']}")
        print(f"SQL: {result['sql_query']}")
        print(f"Found: {result['result_count']} rows")

        # Show first few results
        if result['results']:
            print("\n📊 Results:")
            print("-" * 40)

            # Show headers
            headers = list(result['results'][0].keys())
            print(" | ".join(headers))
            print("-" * 40)

            # Show first 5 rows
            for i, row in enumerate(result['results'][:5]):
                values = [str(row[col]) for col in headers]
                print(" | ".join(values))

            if len(result['results']) > 5:
                print(f"... and {len(result['results']) - 5} more rows")
        else:
            print("No data found")

    else:
        print("❌ FAILED")
        print(f"Error: {result['error']}")

    print("="*60)


def main():
    """Simple main function for testing"""
    print("🚀 Welcome to Simple SQL Pipeline - Step 1!")
    print("This version handles basic single-table queries.")
    print("Type 'exit' to quit, 'schema' to see tables.\n")

    # Initialize pipeline
    try:
        pipeline = SimpleSQLPipeline()
    except Exception as e:
        print(f"❌ Failed to initialize: {e}")
        return

    while True:
        user_input = input("\n💬 Ask me about your data: ").strip()

        if user_input.lower() == 'exit':
            print("👋 Goodbye!")
            break

        if user_input.lower() == 'schema':
            pipeline.show_schema()
            continue

        if not user_input:
            continue

        # Process the query
        result = pipeline.process_query(user_input)
        display_results(result)


if __name__ == "__main__":
    main()