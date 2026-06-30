# Local Spark Setup Checklist

Use this page when a machine is new, Spark jobs fail before user code runs, or a learner cannot reproduce an example.

## 1. Check Python

This repo targets Python 3.10+.

```bash
python --version
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

On Windows, activate with:

```powershell
.venv\Scripts\activate
```

## 2. Check Java

Apache Spark runs on the JVM. Spark 3.5 works well with Java 8, 11, or 17. Java 11 or 17 is the usual choice.

```bash
java -version
```

If Java is missing on Ubuntu or Debian:

```bash
sudo apt update
sudo apt install openjdk-17-jdk
```

If Java is missing on macOS:

```bash
brew install openjdk@17
```

## 3. Run The Smallest Job

Run this from the repo root:

```bash
python 00_setup/examples/01_hello_spark.py
```

Then run a slightly more realistic job:

```bash
python 00_setup/examples/02_word_count.py
```

## 4. Confirm Spark UI

While a Spark job is running, open:

```text
http://localhost:4040
```

If the page does not open, the job may have already finished. Add a temporary `input("Press Enter to stop...")` at the end of the script while studying the UI.

## 5. Common Fixes

### `JAVA_HOME is not set`

Find Java:

```bash
readlink -f "$(which java)"
```

Set `JAVA_HOME` to the JDK folder, not the `bin/java` file.

Example on Linux:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

### Port `4040` Is Already In Use

Spark will usually try `4041`, `4042`, and so on. Check the driver logs for the exact UI URL.

### PySpark Uses The Wrong Python

Force Spark to use the active virtual environment:

```bash
export PYSPARK_PYTHON="$(which python)"
export PYSPARK_DRIVER_PYTHON="$(which python)"
```

## 6. Clean Local Spark Artifacts

Spark examples can create local metadata and output folders. These are ignored by git.

```bash
rm -rf metastore_db spark-warehouse output tmp derby.log
```
