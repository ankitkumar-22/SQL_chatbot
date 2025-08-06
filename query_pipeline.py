from typing import List, Dict, Tuple, Optional
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from langchain_groq import ChatGroq
from config import GROQ_API_KEY, MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
import re


def extract_sql_condition(where_response: str) -> str:
    """
    Extract the first valid SQL condition from the LLM's verbose output.
    Looks for patterns like: col LIKE 'S%', col = 'value', etc.
    """
    # Try to find a quoted SQL condition
    match = re.search(r'([\w\.]+\s+(LIKE|=|>|<|<=|>=|!=)\s+\'?[^\n\r\']+\'?)', where_response, re.IGNORECASE)
    if match:
        return match.group(1)
    # Try to find a line that looks like a SQL condition
    for line in where_response.splitlines():
        line = line.strip()
        if any(op in line for op in ['LIKE', '=', '>', '<', '!=']):
            # Remove quotes if present
            return line.replace('"', '').replace("'", "'")
    # Fallback: if the response is a single line and short, use it
    if len(where_response.split()) < 10:
        return where_response.strip().replace('"', '').replace("'", "'")
    return ''


def validate_sql_syntax(sql_query: str) -> Tuple[bool, str]:
    """Validate SQL syntax using regex patterns"""
    # Basic SQL syntax checks
    required_clauses = ['SELECT', 'FROM']
    for clause in required_clauses:
        if clause not in sql_query.upper():
            return False, f"Missing required clause: {clause}"
    
    # Check for common syntax errors
    if sql_query.count('(') != sql_query.count(')'):
        return False, "Mismatched parentheses"
    
    if 'WHERE' in sql_query.upper() and 'WHERE' not in sql_query:
        return False, "Invalid WHERE clause"
    
    # Check for SQL injection patterns
    dangerous_patterns = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
    for pattern in dangerous_patterns:
        if pattern in sql_query.upper():
            return False, f"Potentially dangerous SQL operation detected: {pattern}"
    
    return True, "SQL syntax appears valid"


class SQLQueryPipeline:
    def __init__(self):
        self.engine = self._get_engine()
        self.schema = self._get_schema()
        self.llm = ChatGroq(api_key=GROQ_API_KEY, model="llama3-8b-8192")
        
    def _get_engine(self) -> Engine:
        url = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
        return create_engine(url)
    
    def _get_schema(self) -> Dict:
        inspector = inspect(self.engine)
        schema = {}
        for table in inspector.get_table_names():
            columns = inspector.get_columns(table)
            foreign_keys = inspector.get_foreign_keys(table)
            schema[table] = {
                'columns': [col['name'] for col in columns],
                'foreign_keys': foreign_keys
            }
        return schema
    
    def step1_identify_tables(self, user_query: str) -> List[str]:
        """Step 1: Identify which tables are relevant to the query"""
        schema_info = f"Available tables: {list(self.schema.keys())}"
        prompt = f"""
        Given this database schema:
        {schema_info}
        
        User query: "{user_query}"
        
        Identify which tables are needed to answer this query. 
        IMPORTANT: Return ONLY the table names as a comma-separated list, nothing else.
        Example: customers,orders
        """
        
        response = self.llm.invoke(prompt).content
        # Extract table names from response - handle various formats
        response_clean = response.strip().replace('\n', ' ').replace('`', '')
        
        # Try to extract table names from the response
        tables = []
        if ',' in response_clean:
            tables = [table.strip() for table in response_clean.split(',')]
        else:
            # If no comma, try to find table names in the response
            available_tables = list(self.schema.keys())
            for table in available_tables:
                if table.lower() in response_clean.lower():
                    tables.append(table)
        
        # Validate tables exist in schema
        valid_tables = [table for table in tables if table in self.schema]
        
        # If no valid tables found, try to guess based on keywords
        if not valid_tables:
            query_lower = user_query.lower()
            for table in self.schema.keys():
                if table.lower() in query_lower or any(word in query_lower for word in table.lower().split('_')):
                    valid_tables.append(table)
        
        print(f"Step 1 - Tables identified: {valid_tables}")
        return valid_tables
    
    def step2_analyze_joins(self, tables: List[str]) -> List[Dict]:
        """Step 2: Analyze if joins are needed and determine join conditions"""
        joins = []
        
        if len(tables) > 1:
            # Find foreign key relationships between tables
            for table1 in tables:
                for table2 in tables:
                    if table1 != table2:
                        # Check if table1 has foreign key to table2
                        for fk in self.schema[table1]['foreign_keys']:
                            if fk['referred_table'] == table2:
                                joins.append({
                                    'from_table': table1,
                                    'to_table': table2,
                                    'from_column': fk['constrained_columns'][0],
                                    'to_column': fk['referred_columns'][0]
                                })
        
        print(f"Step 2 - Joins identified: {joins}")
        return joins
    
    def step3_select_columns(self, user_query: str, tables: List[str]) -> List[str]:
        """Step 3: Determine which columns to select based on user query"""
        if not tables:
            return []
            
        # Create column mapping for LLM
        column_info = ""
        for table in tables:
            columns = self.schema[table]['columns']
            column_info += f"{table}: {columns}\n"
        
        prompt = f"""
        Given these tables and their columns:
        {column_info}
        
        User query: "{user_query}"
        
        Identify which columns should be selected to answer this query. 
        IMPORTANT: Return ONLY the column names as a comma-separated list, nothing else.
        Example: first_name,last_name
        """
        
        response = self.llm.invoke(prompt).content
        # Extract column names from response - handle various formats
        response_clean = response.strip().replace('\n', ' ').replace('`', '')
        
        # Try to extract column names
        columns = []
        if ',' in response_clean:
            columns = [col.strip() for col in response_clean.split(',')]
        else:
            # If no comma, try to find column names in the response
            for table in tables:
                for col in self.schema[table]['columns']:
                    if col.lower() in response_clean.lower():
                        columns.append(col)
        
        # If no columns found, use common patterns
        if not columns and tables:
            query_lower = user_query.lower()
            if 'name' in query_lower:
                for table in tables:
                    for col in self.schema[table]['columns']:
                        if 'name' in col.lower():
                            columns.append(col)
            else:
                # Default to first few columns
                for table in tables:
                    table_cols = self.schema[table]['columns'][:3]  # First 3 columns
                    columns.extend(table_cols)
        
        print(f"Step 3 - Columns selected: {columns}")
        return columns
    
    def step4_build_sql(self, tables: List[str], joins: List[Dict], columns: List[str], user_query: str) -> str:
        """Step 4: Construct the SQL query step by step with multiple validation layers"""
        
        if not tables:
            raise ValueError("No tables identified for the query")
        
        # Build SELECT clause with proper table prefixes
        if columns:
            prefixed_columns = []
            for col in columns:
                # Find which table this column belongs to
                col_found = False
                for table in tables:
                    if col in self.schema[table]['columns']:
                        prefixed_columns.append(f"{table}.{col}")
                        col_found = True
                        break
                if not col_found:
                    # If column not found in any table, use as is (for aggregate functions)
                    prefixed_columns.append(col)
            select_clause = "SELECT " + ", ".join(prefixed_columns)
        else:
            select_clause = "SELECT *"
        
        # Build FROM clause - start with the first table
        from_clause = f"FROM {tables[0]}"
        
        # Build JOIN clauses with proper syntax
        join_clauses = []
        for join in joins:
            # Use proper JOIN syntax: JOIN table2 ON table1.column = table2.column
            join_clause = f"JOIN {join['to_table']} ON {join['from_table']}.{join['from_column']} = {join['to_table']}.{join['to_column']}"
            join_clauses.append(join_clause)
        
        # Build WHERE clause based on user query
        where_response = self._extract_where_conditions(user_query, tables)
        where_clause = extract_sql_condition(where_response)
        
        # Combine all clauses
        sql_parts = [select_clause, from_clause] + join_clauses
        if where_clause:
            sql_parts.append(f"WHERE {where_clause}")
        
        sql_query = " ".join(sql_parts)
        
        print(f"Step 4 - SQL Query: {sql_query}")
        
        # LAYER 1: Basic syntax validation
        is_valid, error_msg = validate_sql_syntax(sql_query)
        if not is_valid:
            raise ValueError(f"SQL syntax error: {error_msg}")
        
        # LAYER 2: Database-level validation (EXPLAIN)
        if not self._validate_sql_with_explain(sql_query):
            raise ValueError("SQL query failed database validation")
        
        # LAYER 3: Semantic validation
        if not self._validate_sql_semantics(sql_query, tables, columns):
            raise ValueError("SQL query failed semantic validation")
        
        return sql_query
    
    def _extract_where_conditions(self, user_query: str, tables: List[str]) -> str:
        """Extract WHERE conditions from user query using LLM"""
        if not tables:
            return ""
            
        prompt = f"""
        Given the user query: "{user_query}"
        And these tables: {tables}
        
        Extract any filtering conditions (WHERE clauses) from the query.
        IMPORTANT: Return ONLY the SQL WHERE conditions, or empty string if none.
        Examples:
        - "orders from January" -> "MONTH(order_date) = 1"
        - "customers from New York" -> "city = 'New York'"
        - "first_name starts with 'S'" -> "first_name LIKE 'S%'"
        """
        
        response = self.llm.invoke(prompt).content
        return response.strip()
    
    def _validate_sql_with_explain(self, sql_query: str) -> bool:
        """Validate SQL using EXPLAIN to check for syntax and semantic errors"""
        try:
            with self.engine.connect() as connection:
                # Use EXPLAIN to validate without executing
                explain_query = f"EXPLAIN {sql_query}"
                connection.execute(text(explain_query))
                print("✓ SQL passed EXPLAIN validation")
                return True
        except Exception as e:
            print(f"✗ SQL failed EXPLAIN validation: {e}")
            return False
    
    def _validate_sql_semantics(self, sql_query: str, tables: List[str], columns: List[str]) -> bool:
        """Validate SQL semantics - check if tables and columns exist"""
        try:
            # Check if all mentioned tables exist in schema
            mentioned_tables = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)|(\w+)\.', sql_query, re.IGNORECASE)
            mentioned_tables = [table for group in mentioned_tables for table in group if table]
            
            for table in mentioned_tables:
                if table not in self.schema:
                    print(f"✗ Table '{table}' not found in schema")
                    return False
            
            # Check if all mentioned columns exist in their respective tables
            mentioned_columns = re.findall(r'SELECT\s+(.+?)\s+FROM|(\w+)\.(\w+)', sql_query, re.IGNORECASE)
            for match in mentioned_columns:
                if match[1] and match[2]:  # table.column format
                    table, column = match[1], match[2]
                    if table in self.schema and column not in self.schema[table]['columns']:
                        print(f"✗ Column '{column}' not found in table '{table}'")
                        return False
            
            print("✓ SQL passed semantic validation")
            return True
        except Exception as e:
            print(f"✗ SQL semantic validation error: {e}")
            return False
    
    def step5_execute_query(self, sql_query: str) -> List[Dict]:
        """Step 5: Execute the SQL query and return results with final validation"""
        try:
            # FINAL LAYER: Execute with error handling
            with self.engine.connect() as connection:
                # Set a reasonable timeout
                connection.execute(text("SET SESSION MAX_EXECUTION_TIME = 30000"))  # 30 seconds
                
                result = connection.execute(text(sql_query))
                columns = result.keys()
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                
                print(f"✓ Step 5 - Query executed successfully. Found {len(rows)} rows.")
                return rows
        except Exception as e:
            print(f"✗ Step 5 - Error executing query: {e}")
            return []
    
    def process_query(self, user_query: str) -> Dict:
        """Main pipeline to process a user query step by step with comprehensive validation"""
        print(f"\n{'='*50}")
        print(f"Processing query: {user_query}")
        print(f"{'='*50}")
        
        try:
            # Step 1: Identify tables
            tables = self.step1_identify_tables(user_query)
            
            # Step 2: Analyze joins
            joins = self.step2_analyze_joins(tables)
            
            # Step 3: Select columns
            columns = self.step3_select_columns(user_query, tables)
            
            # Step 4: Build SQL with multiple validation layers
            sql_query = self.step4_build_sql(tables, joins, columns, user_query)
            
            # Step 5: Execute query with final validation
            results = self.step5_execute_query(sql_query)
            
            return {
                'tables': tables,
                'joins': joins,
                'columns': columns,
                'sql_query': sql_query,
                'results': results,
                'validation_passed': True
            }
        except Exception as e:
            print(f"✗ Error in pipeline: {e}")
            return {
                'tables': [],
                'joins': [],
                'columns': [],
                'sql_query': '',
                'results': [],
                'error': str(e),
                'validation_passed': False
            } 