# dlt-snowflake 🏔️

> Run [dlt](https://dlthub.com/) pipelines natively inside Snowflake stored procedures

[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![dlt](https://img.shields.io/badge/dlt-Data_Load_Tool-FF6B6B)](https://dlthub.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://python.org)

---

## 📖 Background

This repository was created in response to discussions around running dlt inside Snowflake:

- **LinkedIn Post**: [Martin Seifert's post on dlt + Snowflake](https://www.linkedin.com/posts/martinseifert_so-dlt-by-dlthub-runs-where-python-runs-activity-7407696729548165120-HRPx)
- **Blog Series**: 
  - [Can you run dlt inside Snowflake?](https://www.sfrt.io/can-you-run-dlt-inside-snowflake/)
  - [Can you run dlt inside Snowflake? Part 2 – SPCS](https://www.sfrt.io/can-you-run-dlt-inside-snowflake-part-2-2-spcs/)

> 💡 *Check the comments on both the LinkedIn post and blog articles for additional context*

---

## 🔍 The Problem

Out of the box, **dlt does not understand that it runs within a Snowpark stored procedure environment**.

Code executed inside stored procedures in any database is inherently limited due to safety, security, performance, and convenience reasons. The default dlt behavior:

1. **Treats Snowflake as an external system** — attempts to connect using the Snowflake Python connector with external credentials, instead of using the session automatically injected into the stored procedure
2. **Uses `PUT` and `COPY INTO` commands** — these operations are restricted when running inside a stored procedure

---

## ✅ The Solution

**Create a custom dlt destination using `@dlt.destination`**

The [`dlt_snowpark.py`](./dlt_snowpark.py) module provides a wrapper that handles all the complexities of using Snowflake Snowpark, making it easy to use dlt within Snowflake stored procedures.

---

## 🚀 Quick Start

### 1. Upload the module to Snowflake

Upload `dlt_snowpark.py` to a Snowflake stage accessible by your stored procedures (e.g., using [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index)):

```bash
snow stage copy dlt_snowpark.py @YOUR_DB.YOUR_SCHEMA.YOUR_STAGE/
```

### 2. Configure your stored procedure

Add the required packages and imports to your stored procedure definition:

```sql
PACKAGES = ('snowflake-snowpark-python', 'dlt')
IMPORTS = ('@YOUR_DB.YOUR_SCHEMA.YOUR_STAGE/dlt_snowpark.py')
```

### 3. Use in your Python code

```python
from dlt_snowpark import DltSnowparkLoader

# Initialize the loader with your Snowpark session
loader = DltSnowparkLoader(snowpark_session, pipeline_name="test_pipeline")

# Now use dlt as usual!
```

---

## 📁 Examples

- **Complete stored procedure example**: [`sfrt_stored_procedures.sql`](./sfrt_stored_procedures.sql)
- **Additional examples**: [`examples/`](./examples/)

---

## 📚 Documentation

For detailed documentation, see: [`docs/dlt-in-snowpark.md`](./docs/dlt-in-snowpark.md)

---

## 🛠️ How It Was Built

This code was generated primarily using **Claude Code**, with human guidance to connect the dots and produce a working, pleasant-to-use solution.

---

## 📄 License

MIT

---

<p align="center">
  <sub>Built with ❤️ for the Snowflake & dlt community</sub>
</p>

