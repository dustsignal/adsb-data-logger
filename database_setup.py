import mysql.connector
from mysql.connector import errorcode

# --- DATABASE CONFIGURATION ---
# Replace with your MySQL database credentials. This script will create the necessary table and indexes.
DB_CONFIG = {
    'user': 'your_db_user',
    'password': 'your_db_password',
    'host': '127.0.0.1',
    'database': 'adsb_data'
}

TABLES = {}

# Define the table structure in a dictionary
TABLES['tracked_aircraft'] = (
    "CREATE TABLE `tracked_aircraft` ("
    "  `hex` varchar(10) NOT NULL,"
    "  `flight` varchar(20) DEFAULT NULL,"
    "  `alt_baro` int(11) DEFAULT NULL,"
    "  `alt_geom` int(11) DEFAULT NULL,"
    "  `gs` float DEFAULT NULL,"
    "  `track` int(11) DEFAULT NULL,"
    "  `baro_rate` int(11) DEFAULT NULL,"
    "  `squawk` varchar(4) DEFAULT NULL,"
    "  `category` varchar(10) DEFAULT NULL,"
    "  `mlat` tinyint(1) DEFAULT 0,"
    "  `tisb` tinyint(1) DEFAULT 0,"
    "  `messages` int(11) DEFAULT NULL,"
    "  `seen` float DEFAULT NULL,"
    "  `rssi` float DEFAULT NULL,"
    "  `seen_count` int(11) DEFAULT 1,"
    "  `first_seen` datetime DEFAULT NULL,"
    "  `last_seen` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),"
    "  PRIMARY KEY (`hex`)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
)

# Define the indexes
INDEXES = {
    'idx_last_seen': "CREATE INDEX `idx_last_seen` ON `tracked_aircraft` (`last_seen`)",
    'idx_first_seen': "CREATE INDEX `idx_first_seen` ON `tracked_aircraft` (`first_seen`)"
}

def setup_database():
    """Connects to the database and creates the table and indexes."""
    cnx = None
    try:
        print("Connecting to the database...")
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor()
        print("Connection successful.")

        # Create the table
        table_name = 'tracked_aircraft'
        table_description = TABLES[table_name]
        print(f"Creating table `{table_name}`...")
        try:
            cursor.execute(table_description)
            print(f"Table `{table_name}` created successfully.")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print(f"Table `{table_name}` already exists.")
            else:
                print(err.msg)

        # Create the indexes
        for index_name, index_description in INDEXES.items():
            print(f"Creating index `{index_name}`...")
            try:
                cursor.execute(index_description)
                print(f"Index `{index_name}` created successfully.")
            except mysql.connector.Error as err:
                # It's okay if the index already exists
                if "Duplicate key name" in err.msg:
                     print(f"Index `{index_name}` already exists.")
                else:
                    print(err.msg)
        
        print("\nDatabase setup process complete.")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    finally:
        if cnx and cnx.is_connected():
            cursor.close()
            cnx.close()
            print("Database connection closed.")

if __name__ == "__main__":
    setup_database()
