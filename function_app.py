import azure.functions as func
import datetime
import json
import logging
import os
import requests
from dotenv import load_dotenv

# ---------------- Load .env ----------------
load_dotenv()

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_WORKSPACE = os.getenv("DATABRICKS_WORKSPACE")
DATABRICKS_QUERY = "SELECT * FROM my_table"  # Replace with your table/query

app = func.FunctionApp()

# ---------------- Databricks HTTP Function ----------------
@app.route(route="GetDatabricksData", auth_level=func.AuthLevel.ANONYMOUS)
def GetDatabricksData(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Databricks HTTP trigger function processed a request.")

    try:
        if not DATABRICKS_TOKEN or not DATABRICKS_WORKSPACE:
            raise Exception("Databricks credentials are missing in .env")

        # Execute Databricks SQL query
        response = requests.post(
            f"https://{DATABRICKS_WORKSPACE}/api/2.0/sql/statements/execute",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            json={"statement": DATABRICKS_QUERY}
        )
        response.raise_for_status()
        data = response.json()

        # Return all rows as JSON
        return func.HttpResponse(json.dumps(data), mimetype="application/json")

    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error fetching Databricks data: {e}")
        return func.HttpResponse(f"Error fetching Databricks data: {e}", status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse(f"Unexpected error: {e}", status_code=500)

# ---------------- ADX Placeholder Function ----------------
@app.route(route="GetADXData", auth_level=func.AuthLevel.ANONYMOUS)
def GetADXData(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ADX HTTP trigger function processed a request.")
    return func.HttpResponse(
        "ADX integration coming soon. This function will fetch data from ADX.",
        status_code=200
    )

# ---------------- Chatbot Placeholder Function ----------------
@app.route(route="ChatbotADXFunction", auth_level=func.AuthLevel.ANONYMOUS)
def ChatbotADXFunction(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Chatbot HTTP trigger function processed a request.")
    return func.HttpResponse(
        "Chatbot integration coming soon. This function will interact with ADX data.",
        status_code=200
    )
