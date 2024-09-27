import psycopg2
import json

# Database connection parameters
db_host = 'your_host'
db_port = 'your_port'  # Usually 5432
db_name = 'your_database'
db_user = 'your_username'
db_password = 'your_password'

# Table and column details
table_name = 'your_table_name'
json_column = 'your_json_column_name'  # The text column containing the JSON string
primary_key_column = 'your_primary_key_column'  # The primary key column of your table

# The target value to look for and the new value to set
target_form_path = 'old_form_path_value'
new_form_path = 'new_form_path_value'

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    conn.autocommit = False  # Enable manual transaction control
    cur = conn.cursor()

    # Select all rows from the table
    select_query = f"""
    SELECT {primary_key_column}, {json_column}
    FROM {table_name}
    """
    cur.execute(select_query)
    rows = cur.fetchall()

    for row in rows:
        primary_key = row[0]
        json_text = row[1]

        try:
            # Parse the JSON string into a Python dictionary
            json_data = json.loads(json_text)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON for primary key {primary_key}: {e}")
            continue  # Skip this row if JSON is invalid

        # Check if 'formPath' exists and equals the target value
        if json_data.get('formPath') == target_form_path:
            # Update the 'formPath' field
            json_data['formPath'] = new_form_path

            # Convert the dictionary back to a JSON string
            new_json_text = json.dumps(json_data)

            # Update the row in the database
            update_query = f"""
            UPDATE {table_name}
            SET {json_column} = %s
            WHERE {primary_key_column} = %s
            """
            try:
                cur.execute(update_query, (new_json_text, primary_key))
                print(f"Updated primary key {primary_key}")
            except Exception as e:
                print(f"Error updating primary key {primary_key}: {e}")
                conn.rollback()  # Roll back on error
                continue

    # Commit the transaction after all updates
    conn.commit()
    print("All updates committed successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the database connection
    if cur:
        cur.close()
    if conn:
        conn.close()
