# 03 — Local setup

Goal: have `pyspark` importable in Python and a Spark UI you can open at `http://localhost:4040`.

## Prereqs

| Thing | Version | Why |
|---|---|---|
| Python | 3.10–3.12 | PySpark 3.5 supports 3.8–3.12. Pick 3.11. |
| Java | 17 (or 11) | Spark runs on the JVM. PySpark just wraps it. |
| pip / venv | latest | Install isolation. |

Check Java first — it's the failure point most people hit:

```bash
java -version
# Expected: openjdk version "17.0.x" ...
```

If you don't have Java 17:

- **macOS**: `brew install openjdk@17` and follow the symlink instructions brew prints.
- **Ubuntu / Debian**: `sudo apt install openjdk-17-jdk`
- **Windows**: install [Eclipse Temurin 17](https://adoptium.net/), then add `bin/` to `PATH`.

Then set `JAVA_HOME` so Spark finds it:

```bash
# macOS / Linux — add to ~/.zshrc or ~/.bashrc
export JAVA_HOME=$(/usr/libexec/java_home -v 17)   # macOS
# or
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Ubuntu

# Windows (PowerShell, one time)
[Environment]::SetEnvironmentVariable("JAVA_HOME","C:\Program Files\Eclipse Adoptium\jdk-17", "User")
```

## Install PySpark in a venv

From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate         # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
```

This installs PySpark 3.5.1, Delta, pandas, pyarrow.

## Smoke test

```bash
python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[*]').appName('smoke').getOrCreate(); print(s.range(5).collect()); s.stop()"
```

Expected (the warnings are normal on first run):

```text
[Row(id=0), Row(id=1), Row(id=2), Row(id=3), Row(id=4)]
```

If you see `Java gateway process exited`, `JAVA_HOME` is wrong or Java is missing.

If you see `Could not locate executable winutils.exe` (Windows only), [grab winutils](https://github.com/cdarlint/winutils) for Hadoop 3.3.x, drop it in `C:\hadoop\bin\`, and set `HADOOP_HOME=C:\hadoop`.

## Open the Spark UI

While a Spark job is running, point your browser at:

```
http://localhost:4040
```

If you have multiple SparkSessions running, the next ones use 4041, 4042, etc. The UI dies when the SparkSession stops, which is why most examples in this repo finish with `input("Press Enter to stop Spark...")` so you can poke around the UI before it goes away.

Tabs you'll use constantly:

- **Jobs** — every action you ran.
- **Stages** — shuffle-separated chunks within a job.
- **SQL / DataFrame** — the visual query plan. This is gold.
- **Executors** — memory, GC time, shuffle read/write per executor.
- **Storage** — what's cached and how big it is.

You will spend more time in the SQL tab than in your editor once you start optimizing. Open it now and click around so it's familiar.

## Optional: Jupyter / VS Code

```bash
pip install jupyterlab
jupyter lab
```

In a notebook cell:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("lab").getOrCreate()
spark
```

Hit `Shift+Enter`. The HTML repr renders a clickable link to the Spark UI.

In VS Code: install the **Python** and **Jupyter** extensions, pick your `.venv` interpreter, and `.py` files run with `Shift+Enter` line-by-line.

## Failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `Java gateway process exited before sending its port number` | No Java, or `JAVA_HOME` wrong | Install Java 17, set `JAVA_HOME`. |
| `JAVA_HOME is not set` | Same as above. | Same as above. |
| `winutils.exe` missing (Windows) | Hadoop expects this binary. | Download winutils for Hadoop 3.3.x, set `HADOOP_HOME`. |
| Spark hangs at "Initial job has not accepted any resources" | All cores already used by another SparkSession. | Stop the other session (`spark.stop()`), restart kernel. |
| `WARN NativeCodeLoader: Unable to load native-hadoop library` | Cosmetic. | Ignore. |
| `Connection refused` to Spark UI | Job finished, SparkSession stopped. | Keep the session alive with `input()` or run a long job. |

## References

- [LS Ch.2 §"Step 1: Downloading Apache Spark"]
- Spark docs — [Quick Start](https://spark.apache.org/docs/latest/quick-start.html)
- 📺 [PySpark Tutorial — freeCodeCamp.org](https://www.youtube.com/watch?v=_C8kWso4ne4) — first 30 minutes cover install and first program.
