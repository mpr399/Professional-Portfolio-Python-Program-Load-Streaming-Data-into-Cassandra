import logging
import multiprocessing
import os
import subprocess
import time

import config
import tools


def load_data_by_cqlsh(csv_file, date):
    pname = multiprocessing.current_process().name
    try:
        csv_file_full_path = f"{config.PATHS['CSV_PATH']}/{date}/{csv_file}"
        dummy_file_full_path = f"{config.PATHS['DUMMY_PATH']}/{date}/{csv_file}"
        error_file_full_path = f"{config.PATHS['LOG_PATH']}/cqlsh.{csv_file}.log"

        # Define the cqlsh command to execute COPY from CSV
        columns = ",".join([column for column in config.CSV_HEADER])
        command = f"COPY CHP_KEYSPACE.CHP_DATA_TABLE ({columns}) FROM '{csv_file_full_path}' WITH HEADER = true " \
                  f"AND ERRFILE = '{error_file_full_path}';"

        # Build the final command to use
        cqlsh_command = f'cqlsh --connect-timeout={config.CQLSH_TIMEOUT_IN_SECONDS} {config.CASSANDRA_HOST_FOR_LOADING} -e "{command}" '

        # Execute the cqlsh command and capture output and error
        result = subprocess.run(cqlsh_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, text=True,
                                shell=True)

        # Log the output and error
        log_queue.put((f"Subprocess output for {csv_file}: {result.stdout}", logging.INFO))
        if len(result.stderr) > 0:
            log_queue.put((f"Subprocess error for {csv_file}: {result.stderr}", logging.ERROR))
        else:
            # Create a dummy file to indicate success
            with open(dummy_file_full_path, 'w'):
                pass
            log_queue.put((f"{pname}:Successfully loaded file: {csv_file}", logging.INFO))

    except subprocess.CalledProcessError as e:
        log_queue.put((f"{pname}:Error loading file using subprocess: {csv_file} : {str(e)}", logging.ERROR))

    except Exception as e:
        log_queue.put((f"{pname}:Error loading file: {csv_file} : {str(e)}", logging.ERROR))


if __name__ == "__main__":

    while True:
        try:
            # Create the directories needed
            tools.create_directories(config.PATHS.values())

            # Start separate process for log queue
            log_queue = multiprocessing.Queue(-1)
            log_process = multiprocessing.Process(target=tools.setup_logging_queue_processor,
                                                  args=(log_queue, "load.log"))
            log_process.start()

            # Run the Loading Function with multiprocessing pool
            while True:
                dates = tools.get_recent_dates(config.LOAD_PERIOD_IN_DAYS)
                log_queue.put((f"Scanning these date folders: {dates}", logging.INFO))

                for date in dates:
                    try:
                        local_dir_csv = f"{config.PATHS['CSV_PATH']}/{date}"
                        local_dir_dummy = f"{config.PATHS['DUMMY_PATH']}/{date}"

                        tools.create_directories([local_dir_csv, local_dir_dummy])

                        csv_files = sorted([f for f in os.listdir(local_dir_csv) if f.endswith(".csv")])
                        dummy_files = sorted([f for f in os.listdir(local_dir_dummy) if f.endswith(".csv")])

                        files_to_process = [file for file in csv_files if file not in dummy_files]

                        with multiprocessing.Pool(processes=config.LOAD_PROCESS_POOL_SIZE) as pool:
                            pool.starmap(load_data_by_cqlsh, zip(files_to_process, [date] * len(files_to_process)))

                    except Exception as e:
                        log_queue.put((f"Error working on this date: {date} : {str(e)}", logging.ERROR))

                time.sleep(config.LOAD_SLEEP_IN_SECONDS)

        except Exception as e:
            print(f"Base Exception : {str(e)}")

        time.sleep(config.MAIN_LOOP_SLEEP_IN_SECONDS)
