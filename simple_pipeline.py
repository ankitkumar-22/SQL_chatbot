# simple_pipeline.py - Step 1: Single Table Queries

from langchain_groq import ChatGroq
from sqlalchemy import create_engine, text, inspect
from config import GROQ_API_KEY, MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
import re
from typing import List, Dict, Any


class SimpleSQLPipeline:
    """
    A simple SQL pipeline that handles basic single table queries.
    Perfect for learning and interviews!
    """
    
    def __init__(self):
        """Initialize the pipeline with database connection and LLM"""
        print("🔧 Setting up Simple SQL Pipeline...")
        
        # Create database connection
        self.engine = self._create_db_connection()
        
        # Initialize the LLM (Large Language Model)
        self.llm = ChatGroq(
            api_key=GROQ_API_KEY, 
            model="llama3-8b-8192",
            temperature=0  # Low temperature for consistent results
        )
        
        # Get database schema (table and column information)
        self.schema = self._get_database_schema()
        print(f"✅ Found {len(self.schema)} tables in database")
        print("🚀 Pipeline ready!")
    
    def _create_db_connection(self):
        """Create connection to MySQL database"""
        connection_url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
        return create_engine(connection_url)
    
    def _get_database_schema(self) -> Dict[str, List[str]]:
        """
        Get basic schema information: which tables exist and their columns
        This is simpler than the complex version - just table names and columns
        """
        inspector = inspect(self.engine)
        schema = {}
        
        for table_name in inspector.get_table_names():
            # Get column names for each table
            columns = inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]
            schema[table_name] = column_names
        
        return schema
    
    def process_query(self, user_question: str) -> Dict[str, Any]:
        """
        Main method: Convert user question to SQL and get results
        
        Steps:
        1. Identify which table the user is asking about
        2. Generate SQL query using LLM
        3. Clean and validate the SQL
        4. Execute query and return results
        """
        print(f"\n📝 Processing: '{user_question}'")
        
        try:
            # Step 1: Find the right table
            target_table = self._identify_table(user_question)
            if not target_table:
                return self._error_response("Could not identify which table you're asking about")
            
            print(f"🎯 Target table: {target_table}")
            
            # Step 2: Generate SQL using LLM
            sql_query = self._generate_sql(user_question, target_table)
            print(f"🔧 Generated SQL: {sql_query}")
            
            # Step 3: Validate the SQL is safe
            if not self._is_safe_sql(sql_query):
                return self._error_response("Generated SQL failed safety check")
            
            print(f"🔧 Final SQL to execute: '{sql_query}'")
            
            # Step 4: Execute the query
            results = self._execute_query(sql_query)
            
            return {
                'success': True,
                'user_question': user_question,
                'target_table': target_table,
                'sql_query': sql_query,
                'results': results,
                'result_count': len(results)
            }
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return self._error_response(str(e))
    
    def _identify_table(self, user_question: str) -> str:
        """
        Step 1: Figure out which table the user is asking about
        This is the simplest approach - ask the LLM to pick one table
        """
        available_tables = list(self.schema.keys())
        
        prompt = f"""
        You are a database expert. Look at this user question and identify which table they're asking about.
        
        User question: "{user_question}"
        
        Available tables: {available_tables}
        
        Return ONLY the table name that best matches the question. 
        If no table matches well, return "UNKNOWN".
        
        Examples:
        - "show me customers" → customers
        - "list all products" → products  
        - "how many orders" → orders
        
        Table name:"""
        
        response = self.llm.invoke(prompt).content.strip()
        
        # Clean the response and validate it's a real table
        table_name = response.lower().strip()
        if table_name in available_tables:
            return table_name
        
        # Fallback: check if any table name appears in the question
        question_lower = user_question.lower()
        for table in available_tables:
            if table.lower() in question_lower:
                return table
        
        return None
    
    def _generate_sql(self, user_question: str, target_table: str) -> str:
        """
        Step 2: Generate SQL query for the identified table
        We give the LLM the table structure and ask for SQL
        """
        table_columns = self.schema[target_table]
        
        prompt = f"""
        You must return ONLY a SQL query. No explanations. No text before or after.
        
        Question: "{user_question}"
        Table: {target_table}
        Columns: {table_columns}
        
        Rules:
        - Start immediately with SELECT
        - Use single quotes for strings
        - One line if possible
        - No semicolon at end
        
        Examples:
        SELECT * FROM customers
        SELECT first_name, last_name FROM customers WHERE city = 'NYC'
        SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM customers
        SELECT COUNT(*) FROM customers WHERE status = 'active'
        
        Query for "{user_question}":"""
        
        response = self.llm.invoke(prompt).content.strip()
        
        # If the LLM still adds explanation, use a simple extraction
        if not response.upper().strip().startswith('SELECT'):
            # Find the SELECT statement in the response
            lines = response.split('\n')
            for line in lines:
                if line.strip().upper().startswith('SELECT'):
                    response = line.strip()
                    break
        
        # Clean up the SQL
        sql = self._clean_sql(response)
        return sql
    
    def _clean_sql(self, sql: str) -> str:
        """Clean up the generated SQL query and extract just the SQL part"""
        print(f"🔍 Raw LLM response: '{sql}'")
        
        # Remove markdown formatting first
        sql = re.sub(r'```sql\n?|```\n?', '', sql)
        
        # Simple approach: if it doesn't start with SELECT, find the SELECT line
        if not sql.strip().upper().startswith('SELECT'):
            print("🔧 Response doesn't start with SELECT, extracting...")
            sql_extracted = self._extract_sql_from_response(sql)
        else:
            sql_extracted = sql.strip()
        
        # Remove extra whitespace and trailing semicolons
        sql_extracted = sql_extracted.strip().rstrip(';')
        sql_extracted = sql_extracted.rstrip(']')
        
        # Remove any remaining explanation text that might be at the end
        # Split by newlines and take only the SQL part
        lines = sql_extracted.split('\n')
        sql_lines = []
        
        for line in lines:
            line = line.strip()
            if line:
                # If line looks like SQL (has SQL keywords), keep it
                if (line.upper().startswith('SELECT') or 
                    any(keyword in line.upper() for keyword in ['FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT']) or
                    'CONCAT(' in line.upper() or
                    line.endswith(',') or
                    line.startswith('AND') or line.startswith('OR')):
                    sql_lines.append(line)
                else:
                    # This doesn't look like SQL, stop here
                    break
        
        if sql_lines:
            sql_extracted = ' '.join(sql_lines)
        
        # Fix common quote issues in SQL
        sql_extracted = sql_extracted.replace(''', "'").replace(''', "'")
        
        print(f"🧹 Cleaned SQL: '{sql_extracted}'")
        return sql_extracted
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract the actual SQL query from LLM response that might have explanatory text"""
        
        # Method 1: Look for SELECT and extract everything from there until end of SQL
        lines = response.strip().split('\n')
        sql_lines = []
        found_select = False
        
        for line in lines:
            line_stripped = line.strip()
            
            # Skip empty lines before finding SELECT
            if not found_select and not line_stripped:
                continue
                
            # Look for SELECT statement
            if not found_select and line_stripped.upper().startswith('SELECT'):
                found_select = True
                sql_lines.append(line_stripped)
                continue
            
            # If we found SELECT, keep adding lines that look like SQL
            if found_select:
                if line_stripped:
                    # Check if this looks like SQL (contains SQL keywords or continues previous line)
                    sql_keywords = ['FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'AS', 'AND', 'OR']
                    if (any(keyword in line_stripped.upper() for keyword in sql_keywords) or 
                        line_stripped.endswith(',') or 
                        line_stripped.startswith('(') or 
                        sql_lines[-1].endswith(',')):
                        sql_lines.append(line_stripped)
                    else:
                        # This line doesn't look like SQL, stop here
                        break
                else:
                    # Empty line might indicate end of SQL
                    break
        
        if sql_lines:
            result = ' '.join(sql_lines)
            print(f"🔍 Method 1 extracted: '{result}'")
            return result
        
        # Method 2: Use regex to find SELECT statement
        # Match SELECT ... FROM ... and optional WHERE/GROUP BY/ORDER BY
        pattern = r'SELECT\s+.+?FROM\s+\w+(?:\s+(?:WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT)\s+.+?)*'
        match = re.search(pattern, response, re.IGNORECASE | re.DOTALL)
        
        if match:
            result = match.group(0).strip()
            # Clean up whitespace
            result = ' '.join(result.split())
            print(f"🔍 Method 2 extracted: '{result}'")
            return result
        
        # Method 3: Last resort - find any line with SELECT and FROM
        for line in lines:
            line_clean = ' '.join(line.split())  # Normalize whitespace
            if 'SELECT' in line_clean.upper() and 'FROM' in line_clean.upper():
                print(f"🔍 Method 3 extracted: '{line_clean}'")
                return line_clean
        
        # If all else fails, return the original (this shouldn't happen with good prompts)
        print("⚠️ Could not extract SQL, returning original response")
        return response.strip()
    
    def _is_safe_sql(self, sql: str) -> bool:
        """
        Step 3: Enhanced safety check - make sure it's a SELECT query
        and doesn't contain dangerous operations
        """
        if not sql or sql.strip() == "":
            print("❌ SQL is empty")
            return False
        
        # Clean up the SQL for checking (remove extra spaces, newlines)
        sql_clean = ' '.join(sql.split())
        sql_upper = sql_clean.upper().strip()
        
        print(f"🔍 Checking SQL: {sql_clean}")
        
        # Must contain SELECT somewhere near the beginning
        if not sql_upper.startswith('SELECT'):
            # Maybe there's some whitespace or other characters, let's be more flexible
            if 'SELECT' not in sql_upper[:20]:  # SELECT should be in first 20 characters
                print("❌ SQL must start with SELECT")
                print(f"   First 20 chars: '{sql_upper[:20]}'")
                return False
        
        # Check for dangerous keywords
        dangerous_words = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE', 'EXEC']
        for word in dangerous_words:
            if word in sql_upper:
                print(f"❌ Dangerous keyword found: {word}")
                return False
        
        # Basic quote balance check (only for single quotes in string literals)
        # Count single quotes that are likely string delimiters
        quote_count = 0
        in_string = False
        for i, char in enumerate(sql):
            if char == "'" and (i == 0 or sql[i-1] != "\\"):  # Not escaped quote
                quote_count += 1
        
        if quote_count % 2 != 0:
            print("❌ Unbalanced single quotes in SQL")
            print(f"   Found {quote_count} single quotes")
            return False
        
        # Check for basic SQL structure
        if 'FROM' not in sql_upper:
            print("❌ SQL missing FROM clause")
            return False
        
        # Additional checks for common SQL patterns
        if sql_upper.count('(') != sql_upper.count(')'):
            print("❌ Unbalanced parentheses")
            return False
        
        print("✅ SQL passed all safety checks")
        return True
    
    def _execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        Step 4: Execute the SQL query and return results as list of dictionaries
        """
        try:
            with self.engine.connect() as connection:
                # First, test the query with EXPLAIN to catch syntax errors
                try:
                    connection.execute(text(f"EXPLAIN {sql_query}"))
                    print("✅ SQL syntax validated")
                except Exception as explain_error:
                    print(f"❌ SQL syntax error caught: {explain_error}")
                    raise Exception(f"SQL syntax error: {explain_error}")
                
                # If EXPLAIN worked, execute the actual query
                result = connection.execute(text(sql_query))
                column_names = result.keys()
                rows = result.fetchall()
                
                # Convert to list of dictionaries for easy handling
                results = []
                for row in rows:
                    row_dict = dict(zip(column_names, row))
                    results.append(row_dict)
                
                print(f"✅ Query executed successfully, got {len(results)} rows")
                return results
                
        except Exception as e:
            print(f"❌ Execution failed: {e}")
            raise
    
    def _error_response(self, error_message: str) -> Dict[str, Any]:
        """Standard error response format"""
        return {
            'success': False,
            'error': error_message,
            'results': [],
            'result_count': 0
        }
    
    def show_schema(self):
        """Helper method to display database schema"""
        print("\n📋 Database Schema:")
        print("=" * 50)
        for table_name, columns in self.schema.items():
            print(f"📁 Table: {table_name}")
            print(f"   Columns: {', '.join(columns)}")
            print()


# Test the pipeline
if __name__ == "__main__":
    # Initialize pipeline
    pipeline = SimpleSQLPipeline()
    
    # Show what tables we have
    pipeline.show_schema()
    
    # Test with some simple queries
    test_queries = [
        "show me all customers",
        "list customer names", 
        "get products",
        "count all orders"
    ]
    
    print("\n🧪 Testing Simple Queries:")
    print("=" * 60)
    
    for query in test_queries:
        result = pipeline.process_query(query)
        
        if result['success']:
            print(f"✅ '{query}'")
            print(f"   SQL: {result['sql_query']}")
            print(f"   Results: {result['result_count']} rows")
        else:
            print(f"❌ '{query}' - {result['error']}")
        print()