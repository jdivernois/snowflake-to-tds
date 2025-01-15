import tableauserverclient as TSC
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_and_publish_datasource(datasource_name=None):
    # Get credentials from environment variables
    snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
    snowflake_user = os.getenv('SNOWFLAKE_USER')
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
    snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
    snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
    snowflake_table = os.getenv('SNOWFLAKE_TABLE')
    snowflake_service = os.getenv('SNOWFLAKE_SERVICE')
    snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    
    if not datasource_name:
        datasource_name = "Snowflake_Datasource"

    try:
        # Create TDS file content
        tds_content = f"""<?xml version='1.0' encoding='utf-8' ?>
        <datasource formatted-name='{datasource_name}' inline='true' version='10.0'>
            <connection class='snowflake' dbname='{snowflake_database}' schema='{snowflake_schema}' 
                server='{snowflake_account}' service='{snowflake_service}' warehouse='{snowflake_warehouse}' username='{snowflake_user}' password='{snowflake_password}'>
                <relation name='{snowflake_table}' table='[{snowflake_database}].[{snowflake_schema}].[{snowflake_table}]' type='table' />
            </connection>
        </datasource>
        """

        # Create temporary TDS file
        temp_tds_file = f"{datasource_name}.tds"
        with open(temp_tds_file, 'w') as f:
            f.write(tds_content)

        print(f"Datasource '{datasource_name}' created successfully!")
        return 1

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    datasource = create_and_publish_datasource()
