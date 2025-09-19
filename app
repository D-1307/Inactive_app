# SQL to CSV Converter for Jupyter Notebook
# Run each cell separately for better control and monitoring

import re
import csv
import os
from typing import Generator, List
import logging
from datetime import datetime
import time

# Cell 1: Setup and Configuration
print("=== SQL to CSV Converter Setup ===")

# Configuration - Modify these paths as needed
SQL_FILE_PATH = r"C:\Users\darshan\Downloads\Telegram Desktop\1win_users.sql"
OUTPUT_DIR = r"C:\Users\darshan\Downloads"
CHUNK_SIZE = 50000  # Rows per CSV file

# Setup logging for Jupyter
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Validate input file
if not os.path.exists(SQL_FILE_PATH):
    print(f"❌ ERROR: SQL file not found at: {SQL_FILE_PATH}")
    print("Please update the SQL_FILE_PATH variable above")
else:
    file_size_gb = os.path.getsize(SQL_FILE_PATH) / (1024**3)
    print(f"✅ SQL file found: {file_size_gb:.2f} GB")
    print(f"📁 Output directory: {OUTPUT_DIR}")
    print(f"📊 Chunk size: {CHUNK_SIZE:,} rows per CSV")

# Cell 2: Main Converter Class
class SQLToCSVConverter:
    def __init__(self, sql_file_path: str, output_dir: str, chunk_size: int = 50000):
        """Initialize the SQL to CSV converter"""
        self.sql_file_path = sql_file_path
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.headers = []
        self.total_rows = 0
        self.start_time = None
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
    
    def extract_column_names(self, create_table_line: str) -> List[str]:
        """Extract column names from CREATE TABLE statement"""
        column_pattern = r'`([^`]+)`\s+[^,\n]+(?:,|\n)'
        matches = re.findall(column_pattern, create_table_line, re.MULTILINE)
        return matches
    
    def parse_insert_values(self, values_part: str) -> Generator[List[str], None, None]:
        """Parse VALUES part of INSERT statement and yield individual rows"""
        values_part = values_part.strip()
        
        # Use delimiter approach for better reliability
        delimiter = "|||ROW_SEPARATOR|||"
        result = ""
        i = 0
        in_quotes = False
        quote_char = None
        
        while i < len(values_part):
            char = values_part[i]
            
            if char in ('"', "'") and (i == 0 or values_part[i-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
                result += char
            elif not in_quotes and char == ')' and i + 1 < len(values_part) and values_part[i + 1] == ',':
                if i + 2 < len(values_part) and values_part[i + 2] == '(':
                    result += ')' + delimiter + '('
                    i += 2
                else:
                    result += char
            else:
                result += char
            i += 1
        
        # Split and process rows
        rows = result.split(delimiter)
        for row in rows:
            row = row.strip()
            if row.startswith('(') and row.endswith(')'):
                row = row[1:-1]
            elif row.startswith('('):
                row = row[1:]
            elif row.endswith(')'):
                row = row[:-1]
                
            if row:
                row_values = self.parse_row_values(row)
                if row_values:
                    yield row_values
    
    def parse_row_values(self, row_data: str) -> List[str]:
        """Parse individual row data and return list of values"""
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        i = 0
        
        while i < len(row_data):
            char = row_data[i]
            
            if char in ('"', "'") and (i == 0 or row_data[i-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
                else:
                    current_value += char
            elif char == ',' and not in_quotes:
                values.append(self.clean_value(current_value.strip()))
                current_value = ""
            else:
                current_value += char
            i += 1
        
        if current_value or len(values) < len(self.headers):
            values.append(self.clean_value(current_value.strip()))
        
        return values
    
    def clean_value(self, value: str) -> str:
        """Clean and format a single value"""
        value = value.strip()
        
        if value.upper() == 'NULL':
            return ''
        
        if len(value) >= 2 and value[0] in ('"', "'") and value[-1] == value[0]:
            value = value[1:-1]
        
        value = value.replace("\\'", "'").replace('\\"', '"').replace('\\\\', '\\')
        return value
    
    def process_insert_statement(self, statement: str) -> Generator[List[str], None, None]:
        """Process a single INSERT statement and yield rows"""
        try:
            values_match = re.search(r'VALUES\s+(.+);?$', statement, re.DOTALL | re.IGNORECASE)
            if not values_match:
                return
            
            values_part = values_match.group(1).rstrip(';')
            
            for row_values in self.parse_insert_values(values_part):
                if len(row_values) == len(self.headers):
                    yield row_values
                else:
                    # Handle mismatched column counts
                    if len(row_values) < len(self.headers):
                        row_values.extend([''] * (len(self.headers) - len(row_values)))
                    else:
                        row_values = row_values[:len(self.headers)]
                    yield row_values
                    
        except Exception as e:
            print(f"⚠️ Warning: Error processing INSERT statement: {str(e)}")
    
    def process_sql_file(self):
        """Process the SQL file and create CSV chunks - with Jupyter-friendly progress"""
        print(f"\n🚀 Starting to process SQL file: {os.path.basename(self.sql_file_path)}")
        self.start_time = time.time()
        
        chunk_num = 0
        row_count = 0
        current_chunk = []
        current_csv_writer = None
        current_csv_file = None
        
        try:
            with open(self.sql_file_path, 'r', encoding='utf-8', errors='ignore') as file:
                in_insert_statement = False
                current_statement = ""
                
                for line_num, line in enumerate(file, 1):
                    # Progress update every 50K lines for Jupyter
                    if line_num % 50000 == 0:
                        elapsed = time.time() - self.start_time
                        speed = line_num / elapsed if elapsed > 0 else 0
                        print(f"📊 Lines: {line_num:,} | Data rows: {row_count:,} | Speed: {speed:.0f} lines/sec | Time: {elapsed/60:.1f}min")
                    
                    line = line.strip()
                    
                    if not line or line.startswith('--') or line.startswith('/*'):
                        continue
                    
                    # Extract column names from CREATE TABLE
                    if line.startswith('CREATE TABLE') and '`ma_users`' in line:
                        print("🔍 Found CREATE TABLE statement, extracting column names...")
                        create_statement = line
                        while not create_statement.rstrip().endswith(';'):
                            next_line = next(file, '').strip()
                            create_statement += ' ' + next_line
                        
                        self.headers = self.extract_column_names(create_statement)
                        print(f"✅ Extracted {len(self.headers)} columns: {', '.join(self.headers[:5])}...")
                        continue
                    
                    # Process INSERT statements
                    if line.startswith('INSERT INTO') and '`ma_users`' in line:
                        in_insert_statement = True
                        current_statement = line
                        
                        if line.endswith(';'):
                            # Process single-line INSERT
                            rows_processed = list(self.process_insert_statement(current_statement))
                            
                            for row_data in rows_processed:
                                if len(current_chunk) == 0:
                                    chunk_num += 1
                                    csv_filename = os.path.join(self.output_dir, f'ma_users_chunk_{chunk_num:04d}.csv')
                                    current_csv_file = open(csv_filename, 'w', newline='', encoding='utf-8')
                                    current_csv_writer = csv.writer(current_csv_file)
                                    current_csv_writer.writerow(self.headers)
                                    print(f"📝 Started chunk {chunk_num}: {os.path.basename(csv_filename)}")
                                
                                current_csv_writer.writerow(row_data)
                                current_chunk.append(row_data)
                                row_count += 1
                                
                                if len(current_chunk) >= self.chunk_size:
                                    current_csv_file.close()
                                    print(f"✅ Completed chunk {chunk_num} with {len(current_chunk):,} rows")
                                    current_chunk = []
                                    current_csv_writer = None
                                    current_csv_file = None
                            
                            in_insert_statement = False
                            current_statement = ""
                        continue
                    
                    # Continue building multi-line INSERT statement
                    if in_insert_statement:
                        current_statement += ' ' + line
                        
                        if line.endswith(';'):
                            rows_processed = list(self.process_insert_statement(current_statement))
                            
                            for row_data in rows_processed:
                                if len(current_chunk) == 0:
                                    chunk_num += 1
                                    csv_filename = os.path.join(self.output_dir, f'ma_users_chunk_{chunk_num:04d}.csv')
                                    current_csv_file = open(csv_filename, 'w', newline='', encoding='utf-8')
                                    current_csv_writer = csv.writer(current_csv_file)
                                    current_csv_writer.writerow(self.headers)
                                    print(f"📝 Started chunk {chunk_num}: {os.path.basename(csv_filename)}")
                                
                                current_csv_writer.writerow(row_data)
                                current_chunk.append(row_data)
                                row_count += 1
                                
                                if len(current_chunk) >= self.chunk_size:
                                    current_csv_file.close()
                                    print(f"✅ Completed chunk {chunk_num} with {len(current_chunk):,} rows")
                                    current_chunk = []
                                    current_csv_writer = None
                                    current_csv_file = None
                            
                            in_insert_statement = False
                            current_statement = ""
            
            # Close the last chunk
            if current_csv_file:
                current_csv_file.close()
                print(f"✅ Completed final chunk {chunk_num} with {len(current_chunk):,} rows")
            
            # Final summary
            total_time = time.time() - self.start_time
            print(f"\n🎉 CONVERSION COMPLETED!")
            print(f"📊 Total rows processed: {row_count:,}")
            print(f"📁 Created {chunk_num} CSV files")
            print(f"⏱️ Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
            print(f"🚀 Average speed: {row_count/total_time:.0f} rows/second")
            
            self.total_rows = row_count
            return chunk_num, row_count
            
        except KeyboardInterrupt:
            print(f"\n⏸️ Process interrupted by user")
            if current_csv_file:
                current_csv_file.close()
            return chunk_num, row_count
        except Exception as e:
            print(f"\n❌ Error occurred: {str(e)}")
            if current_csv_file:
                current_csv_file.close()
            raise

print("✅ SQLToCSVConverter class loaded successfully!")

# Cell 3: Create converter instance
print("🔧 Creating converter instance...")
converter = SQLToCSVConverter(SQL_FILE_PATH, OUTPUT_DIR, CHUNK_SIZE)
print("✅ Converter ready!")

# Cell 4: Run the conversion (This is the main processing cell)
print("🚀 Starting conversion process...")
print("Note: This will take several hours for a 27GB file.")
print("You can interrupt with Kernel -> Interrupt if needed.\n")

try:
    chunks_created, total_rows = converter.process_sql_file()
    
    # Show final results
    print(f"\n📋 FINAL SUMMARY:")
    print(f"   • Source file: {os.path.basename(SQL_FILE_PATH)}")
    print(f"   • Total chunks: {chunks_created}")
    print(f"   • Total rows: {total_rows:,}")
    print(f"   • Output location: {OUTPUT_DIR}")
    
    # List created files
    csv_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith('ma_users_chunk_') and f.endswith('.csv')]
    if csv_files:
        print(f"\n📁 Created CSV files:")
        for i, filename in enumerate(sorted(csv_files), 1):
            filepath = os.path.join(OUTPUT_DIR, filename)
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            print(f"   {i:2d}. {filename} ({size_mb:.1f} MB)")
    
except Exception as e:
    print(f"❌ Conversion failed: {str(e)}")
    import traceback
    print(f"Full error: {traceback.format_exc()}")

print("\n🏁 Script execution completed!")

# Cell 5: Optional - Quick verification of first chunk
print("🔍 Quick verification of first chunk (if available)...")

first_chunk_path = os.path.join(OUTPUT_DIR, "ma_users_chunk_0001.csv")
if os.path.exists(first_chunk_path):
    try:
        import pandas as pd
        
        # Read first few rows
        df_sample = pd.read_csv(first_chunk_path, nrows=5)
        print(f"✅ First chunk verified!")
        print(f"Columns: {list(df_sample.columns)}")
        print(f"Shape: {df_sample.shape}")
        print(f"Sample data:")
        print(df_sample.head())
        
    except ImportError:
        print("📝 pandas not available, showing raw CSV sample:")
        with open(first_chunk_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i < 3:  # Show first 3 lines
                    print(f"  {line.strip()}")
                else:
                    break
    except Exception as e:
        print(f"⚠️ Could not verify chunk: {e}")
else:
    print("❌ No chunks found yet - conversion may not have started or completed")
