# Advanced Natural Language to SQL Pipeline

A sophisticated conversational SQL assistant that transforms natural language questions into complex SQL queries and provides interactive data visualization capabilities.

## 🔥 Key Features

**Advanced SQL Generation**
- Complex multi-table JOINs (INNER, LEFT, RIGHT, FULL)
- Common Table Expressions (CTEs) and subqueries
- Window functions and advanced aggregations
- UNION operations and complex WHERE conditions
- MySQL-optimized query generation

**Interactive Data Visualization**
- Automatic chart type selection based on data structure
- Professional interactive charts using Plotly
- Support for bar charts, line plots, scatter plots, histograms, pie charts, heatmaps, and 3D visualizations
- Real-time data analysis and correlation matrices

**Enhanced Safety & Reliability**
- Multi-stage SQL validation (syntax, semantic, and database-level checks)
- Comprehensive schema awareness with foreign key relationships
- Strict security controls preventing data modification
- Intelligent table relationship mapping

**Professional User Experience**
- Chat history with persistent session management
- Auto-completion and smart suggestions
- Comprehensive schema exploration
- Export capabilities for charts and results

## Architecture Overview

The system consists of four main components:

1. **AdvancedSQLPipeline** (`advanced_pipeline.py`) - Core SQL generation engine with full database schema integration
2. **AdvancedDataVisualizer** (`visualization.py`) - Interactive visualization system using Plotly
3. **ChatHistory** (`history.py`) - Session management and conversation tracking
4. **Interactive Interface** (`adv_main.py`) - Command-line interface with visualization integration

## Technical Capabilities

**Supported Query Types**
```sql
-- Multi-table joins with complex relationships
SELECT c.name, o.order_date, p.product_name, cat.category_name
FROM customers c 
JOIN orders o ON c.id = o.customer_id
JOIN order_details od ON o.id = od.order_id
JOIN products p ON od.product_id = p.id
JOIN categories cat ON p.category_id = cat.id

-- Window functions and analytics
SELECT product_name, price, 
       ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY price DESC) as price_rank
FROM products

-- CTEs for complex data processing
WITH monthly_sales AS (
    SELECT DATE_FORMAT(order_date, '%Y-%m-01') as month,
           SUM(total_amount) as sales
    FROM orders 
    GROUP BY month
)
SELECT * FROM monthly_sales ORDER BY month
```

**Visualization Capabilities**
- Automatic data type detection and chart recommendation
- Interactive dashboards with multiple chart types
- Correlation analysis and statistical summaries
- Responsive design for all screen sizes
- Professional color schemes and hover interactions

## Setup & Installation

1. **Configure Database Connection**
   ```python
   # config.py
   GROQ_API_KEY = "your_groq_api_key"
   MYSQL_HOST = "localhost"
   MYSQL_USER = "root"
   MYSQL_PASSWORD = "your_password"
   MYSQL_DB = "your_database_name"
   ```

2. **Install Dependencies**
   ```bash
   pip install langchain-groq sqlalchemy pymysql pandas plotly numpy
   ```

3. **Run the Application**
   ```bash
   python adv_main.py
   ```

## Usage Examples

**Natural Language Queries**
```
"Show me sales by category for the last year"
"Which customers have ordered more than 5 different products?"
"Find the top 10 products by revenue with their suppliers"
"Compare monthly sales trends across different regions"
```

**Visualization Commands**
```
auto          - AI-powered chart selection
histogram     - Distribution analysis
scatter       - Correlation visualization  
pie          - Composition analysis
heatmap      - Correlation matrix
3d           - Multi-dimensional analysis
```

**System Commands**
```
schema       - Display database structure
history      - View conversation history
help         - Show all available commands
clear        - Reset session
```

## Security Features

- **Query Validation**: Multi-layer validation prevents SQL injection and unauthorized operations
- **Read-Only Access**: Strictly limited to SELECT operations
- **Schema Protection**: Safe exploration of database structure without exposing sensitive data
- **Error Isolation**: Comprehensive error handling with detailed logging

## Performance Optimizations

- **Intelligent Caching**: Schema information cached for improved response times
- **Query Optimization**: Automatic query plan analysis and optimization suggestions
- **Efficient Joins**: Foreign key relationship mapping for optimal join strategies
- **Result Streaming**: Large dataset handling with pagination support

## Advanced Features

**Schema Intelligence**
- Automatic foreign key relationship detection
- Smart table aliasing and join path optimization
- Column type awareness for proper data handling

**Data Analysis Integration**
- Statistical summary generation
- Correlation analysis and trend detection
- Anomaly detection in query results

**Export Capabilities**
- Chart export in multiple formats (HTML, PNG, SVG)
- Data export to CSV and Excel
- Query history export for documentation

## Development & Testing

**Run Tests**
```bash
python test_advanced.py
```

**Key Test Coverage**
- SQL generation accuracy across various query types
- Database connection stability
- Visualization rendering performance
- Error handling and recovery

## Future Enhancements

- Web-based dashboard interface
- Advanced analytics and machine learning integration
- Multi-database support (PostgreSQL, SQLite, Oracle)
- Collaborative features and sharing capabilities
- Advanced security with role-based access control

## Technical Requirements

- Python 3.8+
- MySQL 5.7+ or MariaDB 10.3+
- Internet connection for LLM API access
- Modern web browser for visualization display

This system represents a significant advancement in natural language database interaction, combining sophisticated SQL generation with professional-grade data visualization in an intuitive conversational interface.
