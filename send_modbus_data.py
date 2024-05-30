import minimalmodbus
import time
import mysql.connector
from mysql.connector import Error
import random
from datetime import datetime

dataInterval = 60
data_batch_size = 30
db_delay = 1

# MySQL database connection
def init_db_connection(delay=db_delay):
    host = "192.168.1.13"
    user = "root"
    passwd = "Bc8574"
    database = "db_sensor"
    print(f"Trying to connect server ip {host} and database {database}")
    while True:
        try:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                passwd=passwd,
                database=database,
                connection_timeout=5 # Set timeout connection
            )
            if connection.is_connected():
                print("Successfully connected to the database\n")
                return connection
        except KeyboardInterrupt:
            print("Database connection attempt interrupted by user.")
            raise
        except Error as e:
            print(f"Error: {e}")
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)

# Insert data into MySQL
def save_data_to_mysql(cursor, db_connection, data_array):
    if len(data_array) >= data_batch_size:
        print("Inserting data into MySQL...")
        print("")
        for data in data_array:
            cursor.execute("INSERT INTO t_kwh (plant, bagian, frekuensi, tegangan_RS, tegangan_ST, tegangan_TR, tegangan_AVG, arus_A, arus_B, arus_C, arus_AVG, power_factor_total, active_energy_delivered, active_power_total, created_at) VALUES ('2201', 'WORKSHOP', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data)
        db_connection.commit()
        print("Insert Data Success")
        data_array = []  # Clear array after inserting into MySQL
        return data_array
    return data_array

# MySQL connection initialisation
try:
    db_connection = init_db_connection()
    cursor = db_connection.cursor()

    # Modbus initialisation
    pi = minimalmodbus.Instrument('/dev/ttyUSB0', 1)  # port name, slave address (in decimal)
    pi.serial.baudrate = 19200      # BaudRate
    pi.serial.bytesize = 8          # Number of data bits to be requested
    pi.serial.parity = minimalmodbus.serial.PARITY_NONE # Parity Setting here is NONE but can be ODD or EVEN
    pi.serial.stopbits = 1          # Number of stop bits
    pi.serial.timeout  = 0.5        # Timeout time in seconds
    pi.mode = minimalmodbus.MODE_RTU  # Mode to be used (RTU or ascii mode)

    # save array into data_array
    data_array = []
    reading_counter = 0

    # Main loop
    while True:
        try:
            # Read register from Modbus
            frequency = pi.read_float(3109, 3)
            energy = pi.read_long(3203, 3, False, 0, 4)
            voltageRS = pi.read_float(3019, 3)
            voltageST = pi.read_float(3021, 3)
            voltageTR = pi.read_float(3023, 3)
            voltageAVG = pi.read_float(3025, 3)
            currentA = pi.read_float(2999, 3)
            currentB = pi.read_float(3001, 3)
            currentC = pi.read_float(3003, 3)
            currentAVG = pi.read_float(3009, 3)
            ActivePowerAll = pi.read_float(3059, 3)
            PowerFactorAll = pi.read_float(3083, 3)

            # Convert energy into Kwh
            realEnergy = energy /1000

            # Save current time
            current_time = datetime.now()

            # Tambahkan data ke dalam array
            data_array.append((frequency, voltageRS, voltageST, voltageTR, voltageAVG, currentA, currentB, currentC, currentAVG, PowerFactorAll, realEnergy, ActivePowerAll, current_time))
            reading_counter += 1

            # Print data
            print("==============================")
            print(f"Pembacaan data ke: {reading_counter}")
            print("==============================")
            print(f"Frequency: {frequency}")
            print(f"Energy: {energy}")
            print(f"Real Energy (Kwh): {realEnergy}")
            print(f"Voltage RS: {voltageRS}")
            print(f"Voltage ST: {voltageST}")
            print(f"Voltage TR: {voltageTR}")
            print(f"Voltage AVG: {voltageAVG}")
            print(f"Current A: {currentA}")
            print(f"Current B: {currentB}")
            print(f"Current C: {currentC}")
            print(f"Current AVG: {currentAVG}")
            print(f"Active Power All: {ActivePowerAll}")
            print(f"Power Factor All: {PowerFactorAll}")
            print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=============================")
            print("")

            # Data read inteval in second
            time.sleep(dataInterval)

            # save data after batch_size reached
            data_array = save_data_to_mysql(cursor, db_connection, data_array)

            if not data_array:
                reading_counter = 0

        except mysql.connector.Error as err:
            print(f"Error: {err}")
            db_connection = init_db_connection()  # reconnecting into MySQL
            cursor = db_connection.cursor()

        except KeyboardInterrupt:
            print("Exiting due to user interruption...")
            break

finally:
    # Close database connection if connection is open
    if 'cursor' in locals():
        cursor.close()
    if 'db_connection' in locals():
        db_connection.close()
    print("Database connection closed.")
