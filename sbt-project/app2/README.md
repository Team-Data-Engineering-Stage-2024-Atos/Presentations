# app2

- **Run Jobs Sequentially:**

  ```bash
  ./run_jobs.sh -c src/main/resources/application.conf
  ```

- **Run Jobs in Parallel:**

  ```bash
  ./run_jobs.sh -c src/main/resources/application.conf -p
  ```

- **Set Maximum Parallel Jobs:**

  ```bash
  ./run_jobs.sh -c src/main/resources/application.conf -p -P 3
  ```

- **Set Retry Count:**

  ```bash
  ./run_jobs.sh -c src/main/resources/application.conf -r 5
  ```

- **Example:**

  ```bash
  ./run_jobs.sh -c src/main/resources/application.conf -p -P 3 -r 2 -s "--executor-memory 2G"
  ```
