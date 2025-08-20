#!/usr/bin/env python3
"""
data_extractor_no_filters_token_retry.py

Fetch report data for each group->field, with robust login + automatic re-login on 401.
"""
import os
import time
import json
import argparse
import logging
import traceback
from typing import Optional, Any

import requests
from requests.exceptions import RequestException

# -------------------- Config --------------------
BASE_URL =  "https://unifymedicraft.net/laravel/api"
REPORT_URL = f"{BASE_URL}/reports/paymentAdHocReport"
DEFAULT_EMAIL =  "subtest3mostafa@medicraft.com" 
DEFAULT_PASSWORD = "SubTest3@mostafaTest"

GROUPS = {
    "payment": ['id','check_number','is_posted','invoice_id','receipt_id','deposit_id','patient_id','created_by','created_at','age_bucket','posted_by','amount','pay_date','pay_date_bucket','post_date','payments_description','denial_description','type','reason','denial_payment'],
    "receipts": ['id','receipt_type','check_reference','note','receipt_amount'],
    "deposits": ['payment_date','reference','description','posted','date_posted','amount'],
    "patient": ['delivery_address_1','delivery_address_2','delivery_city_name','delivery_state_name','delivery_country_name','delivery_delivery_phone','delivery_delivery_fax','delivery_zipcode','id','first_name','middle_name','last_name','full_name','month_of_birth','day_of_birth','birthday','weight_lbs','gender','ssn','suffix','hipaa_signature','customer_type','facility','pos','account_on_hold','created_by','created_at','prior_system_key','account_no','account_group','user_1','user_2','user_3','user_4','branch','address1','city','state','country','postal_code','state_code','policy_id','insurance_id','payor_level','address','policy_city ','policy_state','policy_country','policy_zip_code','payor_contact','claim_form','ins_submisssion','plan_type','pay_pct'],
    "payor": ['pri_name','group'],
    "meta_key": ['customer_type','account_on_hold'],
    "policy": ['zipcode','country','group_name'],
    "invoice_items": ['item_sku','item_name','proc_code','item_group','qty','allow','charge','balance','created_at','modifier_1','modifier_2','modifier_3','modifier_4','original_dos','dos_to'],
    "ordering_doctor": ['first_name','last_name','middle_name','pecos','npi','country','status'],
    "item_location": ['available','on_hand','on_rent_qty','on_order_qty','out_of_stock'],
    "marketing_rep": ['first_name','last_name'],
    "diagnosis": ['Dx ICD-9 Code #01','Dx ICD-9 Description #01','Dx ICD-9 Code #02','Dx ICD-9 Description #02','Dx ICD-9 Code #03','Dx ICD-9 Description #03','Dx ICD-9 Code #04','Dx ICD-9 Description #04','Dx ICD-9 Code #05','Dx ICD-9 Description #05','Dx ICD-9 Code #06','Dx ICD-9 Description #06','Dx ICD-9 Code #07','Dx ICD-9 Description #07','Dx ICD-9 Code #08','Dx ICD-9 Description #08','Dx ICD-9 Code #09','Dx ICD-9 Description #09']
}

# -------------------- Helpers --------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def setup_logging(logfile: str):
    logdir = os.path.dirname(logfile) or '.'
    os.makedirs(logdir, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)7s  %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(logfile, encoding="utf-8")
        ]
    )

def safe_json(res: requests.Response) -> Any:
    try:
        return res.json()
    except Exception:
        return {"_raw_text": res.text[:4000], "_status_code": res.status_code}

def post_with_retry(session: requests.Session, url: str, headers: dict, data: dict, retries: int = 2, timeout: int = 30):
    delay = 1
    for attempt in range(1, retries + 1):
        try:
            return session.post(url, headers=headers, data=data, timeout=timeout)
        except RequestException as e:
            logging.warning("Request failed (attempt %d/%d): %s", attempt, retries, e)
            if attempt == retries:
                raise
            time.sleep(delay)
            delay *= 2

# -------------------- Token manager + API --------------------
class TokenManager:
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.token: Optional[str] = None

    def login(self, session: requests.Session, timeout: int = 10) -> Optional[str]:
        url = f"{BASE_URL}/login"
        payload = {"email": self.email, "password": self.password}
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        try:
            res = session.post(url, json=payload, headers=headers, timeout=timeout)
        except RequestException as e:
            logging.error("Network error during login: %s", e)
            return None

        # accept any 2xx
        if not res.ok:
            logging.error("Login failed: status=%s body=%s", res.status_code, safe_json(res))
            return None

        body = safe_json(res)
        token = None
        if isinstance(body, dict):
            token = body.get("data", {}).get("access_token")
        if not token:
            logging.error("Login returned no access_token. Full body: %s", body)
            return None

        self.token = token
        logging.info("Login succeeded; token len=%d", len(token))
        return token

    def get(self, session: requests.Session) -> Optional[str]:
        if not self.token:
            return self.login(session)
        return self.token

    def refresh(self, session: requests.Session) -> Optional[str]:
        self.token = None
        return self.login(session)

def fetch_field_data(session: requests.Session, token_manager: TokenManager, group: str, field: str, timeout: int = 30) -> Optional[requests.Response]:
    token = token_manager.get(session)
    if not token:
        logging.error("No token available for request to %s.%s", group, field)
        return None

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {
        "filterData": 100,
        "show": 1,
        "columns_count": 50,
        f"{group}[{field}]": f"{group} {field}"
    }

    try:
        res = post_with_retry(session, REPORT_URL, headers=headers, data=payload, retries=2, timeout=timeout)
    except RequestException as e:
        logging.error("Network error fetching %s.%s: %s", group, field, e)
        return None

    # if token expired/invalid, re-login once and retry
    if res.status_code == 401:
        logging.warning("401 Unauthorized for %s.%s — refreshing token and retrying once", group, field)
        new_token = token_manager.refresh(session)
        if not new_token:
            logging.error("Re-login failed while handling 401 for %s.%s", group, field)
            return res
        headers["Authorization"] = f"Bearer {new_token}"
        try:
            res = post_with_retry(session, REPORT_URL, headers=headers, data=payload, retries=1, timeout=timeout)
        except RequestException as e:
            logging.error("Network error on retry for %s.%s: %s", group, field, e)
            return None

    return res

# -------------------- Run --------------------
def run(args):
    ensure_dir(args.output)
    setup_logging(os.path.join(args.output, "run.log"))

    session = requests.Session()
    email = args.email or DEFAULT_EMAIL
    password = args.password or DEFAULT_PASSWORD
    if not email or not password:
        logging.error("Provide credentials via env vars or --email/--password")
        return

    token_manager = TokenManager(email, password)
    token = token_manager.get(session)
    if not token:
        logging.error("Login failed — aborting")
        return

    logging.info("Logged in as %s", email)

    groups = GROUPS.copy()
    if args.groups:
        wanted = [g.strip() for g in args.groups.split(",") if g.strip()]
        groups = {k: v for k, v in groups.items() if k in wanted}
        if not groups:
            logging.error("No matching groups for %s", wanted)
            return

    for group, fields in groups.items():
        logging.info("Processing group: %s (fields: %d)", group, len(fields))
        for field in fields:
            if args.field and args.field != field:
                continue

            logging.info("Fetching %s.%s", group, field)
            time.sleep(args.delay)
            res = fetch_field_data(session, token_manager, group, field)
            out_dir = os.path.join(args.output, group)
            ensure_dir(out_dir)
            out_file = os.path.join(out_dir, f"{field}.json")

            if res is None:
                payload = {
                    "group": group,
                    "field": field,
                    "error": "network_failed"
                }
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                continue

            body = safe_json(res)
            result = {
                "group": group,
                "field": field,
                "status_code": res.status_code,
                "response": body
            }
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, default=str)

    logging.info("All done. Results in %s", os.path.abspath(args.output))

# -------------------- CLI --------------------
def parse_args():
    p = argparse.ArgumentParser(description="Medicraft — fetch data per group/field without running filter tests")
    p.add_argument("--email", help="login email (or set MEDICRAFT_EMAIL env var)")
    p.add_argument("--password", help="login password (or set MEDICRAFT_PASSWORD env var)")
    p.add_argument("--groups", help="comma-separated groups to run (default: all)")
    p.add_argument("--field", help="only this field within groups")
    p.add_argument("--delay", type=float, default=0.8, help="delay between requests (seconds)")
    p.add_argument("--output", default="results", help="output directory")
    return p.parse_args()

if __name__ == "__main__":
    try:
        args = parse_args()
        run(args)
    except Exception:
        traceback.print_exc()
        logging.error("Unhandled exception; aborting")
