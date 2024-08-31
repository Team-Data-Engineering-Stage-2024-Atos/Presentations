# app2

## BASIC JOBS

  ```bash
  ./run_jobs.sh
  ```

## ADVANCED JOBS - WORK IN PROGRESS

- **Run Jobs Sequentially:**

  ```bash
  ./run_ext_jobs.sh -c src/main/resources/application.conf
  ```

- **Run Jobs in Parallel:**

  ```bash
  ./run_ext_jobs.sh -c src/main/resources/application.conf -p
  ```

- **Set Maximum Parallel Jobs:**

  ```bash
  ./run_ext_jobs.sh -c src/main/resources/application.conf -p -P 3
  ```

- **Set Retry Count:**

  ```bash
  ./run_ext_jobs.sh -c src/main/resources/application.conf -r 5
  ```

- **Example:**

  ```bash
  ./run_ext_jobs.sh -c src/main/resources/application.conf -p -P 3 -r 2 -s "--executor-memory 2G"
  ```
