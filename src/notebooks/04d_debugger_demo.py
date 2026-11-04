# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04d_debugger_demo - using the built-in notebook debugger (PDF: "including a built-in debugger")
# MAGIC
# MAGIC **How to use (serverless notebook):**
# MAGIC 1. Click in the gutter (left of a line number) in the next code cell to set a **breakpoint** on
# MAGIC    `result = to_snake_case(name)`.
# MAGIC 2. Open the cell menu (or the arrow next to Run) and choose **Debug cell**.
# MAGIC 3. Execution stops at the breakpoint. Use **Step into** to walk through each regex in `to_snake_case`,
# MAGIC    watch `s` change in the **Variables** pane, and type expressions in the **Debug console**
# MAGIC    (e.g. `name`, `s`).
# MAGIC 4. **Continue** to finish. Take screenshots of the paused breakpoint and the Variables pane as evidence.
# MAGIC
# MAGIC The example names are the tricky real headers: acronyms, digits, a UTF-8 BOM.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from vstone_framework.headers import to_snake_case

tricky = ["ZHVIPerSqft_AllHomes", "MedianRentalPrice_MultiFamilyResidence5PlusUnits", "ZHVI_1bedroom",
          "﻿CountyName", "CBSACode"]
for name in tricky:
    result = to_snake_case(name)          # <- set the breakpoint on this line, then "Debug cell"
    print(f"{name!r:55} -> {result}")
