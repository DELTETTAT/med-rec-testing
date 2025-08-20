#!/usr/bin/env python3
"""
filter_extractor.py

Enhanced version of data_extractor2 that extracts both raw data and filtered data.
For each field, it first gets raw data, picks a sample value, then applies all filters.
"""
import os
import time
import json
import argparse
import logging
import traceback
from typing import Optional, Any, Union

import requests
from requests.exceptions import RequestException

# -------------------- Config --------------------
BASE_URL = "https://unifymedicraft.net/laravel/api"
REPORT_URL = f"{BASE_URL}/reports/paymentAdHocReport"
DEFAULT_EMAIL = "subtest3mostafa@medicraft.com"
DEFAULT_PASSWORD = "SubTest3@mostafaTest"

FILTERS = ["equal", "not_equal", "greater", "less", "like", "in"]

GROUPS = {
    "payment": [
        'id', 'check_number', 'is_posted', 'invoice_id', 'receipt_id',
        'deposit_id', 'patient_id', 'created_by', 'created_at', 'age_bucket',
        'posted_by', 'amount', 'pay_date', 'pay_date_bucket', 'post_date',
        'payments_description', 'denial_description', 'type', 'reason',
        'denial_payment'
    ],
    "receipts":
    ['id', 'receipt_type', 'check_reference', 'note', 'receipt_amount'],
    "deposits": [
        'payment_date', 'reference', 'description', 'posted', 'date_posted',
        'amount'
    ],
    "patient": [
        'delivery_address_1', 'delivery_address_2', 'delivery_city_name',
        'delivery_state_name', 'delivery_country_name',
        'delivery_delivery_phone', 'delivery_delivery_fax', 'delivery_zipcode',
        'id', 'first_name', 'middle_name', 'last_name', 'full_name',
        'month_of_birth', 'day_of_birth', 'birthday', 'weight_lbs', 'gender',
        'ssn', 'suffix', 'hipaa_signature', 'customer_type', 'facility', 'pos',
        'account_on_hold', 'created_by', 'created_at', 'prior_system_key',
        'account_no', 'account_group', 'user_1', 'user_2', 'user_3', 'user_4',
        'branch', 'address1', 'city', 'state', 'country', 'postal_code',
        'state_code', 'policy_id', 'insurance_id', 'payor_level', 'address',
        'policy_city ', 'policy_state', 'policy_country', 'policy_zip_code',
        'payor_contact', 'claim_form', 'ins_submisssion', 'plan_type',
        'pay_pct'
    ],
    "payor": ['pri_name', 'group'],
    "meta_key": ['customer_type', 'account_on_hold'],
    "policy": ['zipcode', 'country', 'group_name'],
    "invoice_items": [
        'item_sku', 'item_name', 'proc_code', 'item_group', 'qty', 'allow',
        'charge', 'balance', 'created_at', 'modifier_1', 'modifier_2',
        'modifier_3', 'modifier_4', 'original_dos', 'dos_to'
    ],
    "ordering_doctor": [
        'first_name', 'last_name', 'middle_name', 'pecos', 'npi', 'country',
        'status'
    ],
    "item_location":
    ['available', 'on_hand', 'on_rent_qty', 'on_order_qty', 'out_of_stock'],
    "marketing_rep": ['first_name', 'last_name'],
    "diagnosis": [
        'Dx ICD-9 Code #01', 'Dx ICD-9 Description #01', 'Dx ICD-9 Code #02',
        'Dx ICD-9 Description #02', 'Dx ICD-9 Code #03',
        'Dx ICD-9 Description #03', 'Dx ICD-9 Code #04',
        'Dx ICD-9 Description #04', 'Dx ICD-9 Code #05',
        'Dx ICD-9 Description #05', 'Dx ICD-9 Code #06',
        'Dx ICD-9 Description #06', 'Dx ICD-9 Code #07',
        'Dx ICD-9 Description #07', 'Dx ICD-9 Code #08',
        'Dx ICD-9 Description #08', 'Dx ICD-9 Code #09',
        'Dx ICD-9 Description #09'
    ]
}


# -------------------- Helpers --------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def setup_logging(logfile: str):
    logdir = os.path.dirname(logfile) or '.'
    os.makedirs(logdir, exist_ok=True)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)7s  %(message)s",
                        handlers=[
                            logging.StreamHandler(),
                            logging.FileHandler(logfile, encoding="utf-8")
                        ])


def safe_json(res: requests.Response) -> Any:
    try:
        return res.json()
    except Exception:
        return {"_raw_text": res.text[:4000], "_status_code": res.status_code}


def post_with_retry(session: requests.Session,
                    url: str,
                    headers: dict,
                    data: dict,
                    retries: int = 2,
                    timeout: int = 30):
    delay = 1
    for attempt in range(1, retries + 1):
        try:
            return session.post(url,
                                headers=headers,
                                data=data,
                                timeout=timeout)
        except RequestException as e:
            logging.warning("Request failed (attempt %d/%d): %s", attempt,
                            retries, e)
            if attempt == retries:
                raise
            time.sleep(delay)
            delay *= 2


def is_numeric(value: Any) -> bool:
    """Check if a value is numeric (int, float, or numeric string)"""
    if value is None:
        return False
    try:
        float(str(value))
        return True
    except (ValueError, TypeError):
        return False


def extract_sample_value(response_data: dict, group: str, field: str, for_comparison: bool = False) -> Optional[Any]:
    """Extract a sample value from API response data - preserving exact raw values

    Args:
        response_data: The API response data
        group: The group name
        field: The field name
        for_comparison: If True, try to pick a value from middle range for better comparison results
    """
    try:
        if 'response' in response_data and 'data' in response_data['response']:
            data_rows = response_data['response']['data']
            if isinstance(data_rows, list) and len(data_rows) > 0:
                values = []

                # Collect all non-null, non-empty values for this field - preserve exact values
                for row in data_rows:
                    if isinstance(row, dict):
                        # Look for the field value in various possible key formats
                        possible_keys = [
                            field,
                            f"{group}.{field}",
                            f"{group}_{field}",
                            f"{group} {field}"
                        ]
                        for key in possible_keys:
                            if key in row and row[key] is not None and str(row[key]).strip() != "":
                                # Store the exact raw value without any modification
                                values.append(row[key])
                                break
                        else:
                            # If no exact match, try any key that contains the field name
                            for key, value in row.items():
                                if field.lower() in key.lower() and value is not None and str(value).strip() != "":
                                    # Store the exact raw value without any modification
                                    values.append(value)
                                    break

                if not values:
                    return None

                # For comparison filters, try to pick a value from the middle range
                if for_comparison and len(values) > 1:
                    numeric_values = []
                    original_to_numeric = {}  # Map to preserve original values
                    
                    for val in values:
                        if is_numeric(val):
                            try:
                                numeric_val = float(str(val))
                                numeric_values.append(numeric_val)
                                original_to_numeric[numeric_val] = val  # Keep original value
                            except (ValueError, TypeError):
                                continue

                    if len(numeric_values) > 1:
                        # Sort and pick a value from the middle 50% range
                        numeric_values.sort()
                        start_idx = len(numeric_values) // 4  # Start from 25%
                        end_idx = 3 * len(numeric_values) // 4  # End at 75%
                        middle_range = numeric_values[start_idx:end_idx]
                        if middle_range:
                            # Pick the middle value from this range
                            middle_idx = len(middle_range) // 2
                            selected_numeric = middle_range[middle_idx]
                            # Return the original raw value, not the converted one
                            selected_value = original_to_numeric[selected_numeric]
                            logging.info("Selected middle-range value %s (original: %s) for comparison filters on %s.%s",
                                       selected_numeric, selected_value, group, field)
                            return selected_value

                # For non-comparison filters or if no middle range found, return first valid value
                # Return the exact raw value as received from API
                return values[0]

        return None
    except Exception as e:
        logging.warning("Error extracting sample value for %s.%s: %s", group, field, e)
        return None


def get_applicable_filters(sample_value: Any) -> list:
    """Get list of applicable filters based on value type"""
    if sample_value is None:
        return FILTERS  # Try all filters

    if is_numeric(sample_value):
        return FILTERS  # All filters applicable for numeric data
    else:
        # For string data, exclude greater and less
        return [f for f in FILTERS if f not in ["greater", "less"]]


# -------------------- Token manager + API --------------------
class TokenManager:

    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.token: Optional[str] = None

    def login(self,
              session: requests.Session,
              timeout: int = 10) -> Optional[str]:
        url = f"{BASE_URL}/login"
        payload = {"email": self.email, "password": self.password}
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        try:
            res = session.post(url,
                               json=payload,
                               headers=headers,
                               timeout=timeout)
        except RequestException as e:
            logging.error("Network error during login: %s", e)
            return None

        if not res.ok:
            logging.error("Login failed: status=%s body=%s", res.status_code,
                          safe_json(res))
            return None

        body = safe_json(res)
        token = None
        if isinstance(body, dict):
            token = body.get("data", {}).get("access_token")
        if not token:
            logging.error("Login returned no access_token. Full body: %s",
                          body)
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


def fetch_field_data(session: requests.Session,
                     token_manager: TokenManager,
                     group: str,
                     field: str,
                     filter_operator: Optional[str] = None,
                     filter_value: Optional[Union[str, int, float]] = None,
                     timeout: int = 30) -> Optional[requests.Response]:
    token = token_manager.get(session)
    if not token:
        logging.error("No token available for request to %s.%s", group, field)
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {
        "filterData": 100,
        "show": 1,
        "columns_count": 50,
        f"{group}[{field}]": f"{group} {field}"
    }

    # Add filter if specified
    if filter_operator and filter_value is not None:
        payload.update({
            f"filters[{group}][0][field]": field,
            f"filters[{group}][0][group]": group,
            f"filters[{group}][0][operator]": filter_operator,
            f"filters[{group}][0][value]": filter_value
        })

    try:
        res = post_with_retry(session,
                              REPORT_URL,
                              headers=headers,
                              data=payload,
                              retries=2,
                              timeout=timeout)
    except RequestException as e:
        logging.error("Network error fetching %s.%s: %s", group, field, e)
        return None

    # Handle 401 unauthorized
    if res.status_code == 401:
        logging.warning(
            "401 Unauthorized for %s.%s — refreshing token and retrying once",
            group, field)
        new_token = token_manager.refresh(session)
        if not new_token:
            logging.error("Re-login failed while handling 401 for %s.%s",
                          group, field)
            return res
        headers["Authorization"] = f"Bearer {new_token}"
        try:
            res = post_with_retry(session,
                                  REPORT_URL,
                                  headers=headers,
                                  data=payload,
                                  retries=1,
                                  timeout=timeout)
        except RequestException as e:
            logging.error("Network error on retry for %s.%s: %s", group, field,
                          e)
            return None

    return res


def process_field(session: requests.Session,
                  token_manager: TokenManager,
                  group: str,
                  field: str,
                  output_dir: str,
                  delay: float):
    """Process a single field: fetch raw data, then apply filters"""
    field_dir = os.path.join(output_dir, group, field)
    ensure_dir(field_dir)

    # Step 1: Fetch raw data (no filters)
    logging.info("Fetching raw data for %s.%s", group, field)
    time.sleep(delay)

    res = fetch_field_data(session, token_manager, group, field)
    raw_file = os.path.join(field_dir, "raw_data.json")

    if res is None:
        payload = {
            "group": group,
            "field": field,
            "error": "network_failed",
            "filter": None
        }
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        return

    body = safe_json(res)
    raw_result = {
        "group": group,
        "field": field,
        "status_code": res.status_code,
        "filter": None,
        "response": body
    }

    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(raw_result, f, indent=2, ensure_ascii=False)

    # Step 2: Extract sample values from raw data
    # Get regular sample value for most filters
    sample_value = extract_sample_value(raw_result, group, field, for_comparison=False)
    # Get middle-range value for comparison filters
    comparison_value = extract_sample_value(raw_result, group, field, for_comparison=True)

    if sample_value is None:
        logging.warning("No sample value found for %s.%s, skipping filters", group, field)
        return

    logging.info("Regular sample value for %s.%s: %s", group, field, sample_value)
    if comparison_value != sample_value:
        logging.info("Comparison sample value for %s.%s: %s", group, field, comparison_value)

    # Step 3: Apply filters using appropriate sample values
    applicable_filters = get_applicable_filters(sample_value)
    comparison_filters = ["greater", "less"]

    for filter_op in applicable_filters:
        # Use comparison value for greater_than/less_than, regular value for others
        filter_value = comparison_value if filter_op in comparison_filters and comparison_value is not None else sample_value

        logging.info("Applying filter %s to %s.%s with value %s", filter_op, group, field, filter_value)
        time.sleep(delay)

        res = fetch_field_data(session, token_manager, group, field, filter_op, filter_value)
        filter_file = os.path.join(field_dir, f"{filter_op}.json")

        if res is None:
            payload = {
                "group": group,
                "field": field,
                "error": "network_failed",
                "filter": filter_op,
                "filter_value": filter_value
            }
            with open(filter_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            continue

        body = safe_json(res)
        filter_result = {
            "group": group,
            "field": field,
            "status_code": res.status_code,
            "filter": filter_op,
            "filter_value": filter_value,
            "response": body
        }

        with open(filter_file, "w", encoding="utf-8") as f:
            json.dump(filter_result, f, indent=2, ensure_ascii=False)


# -------------------- Run --------------------
def run(args):
    ensure_dir(args.output)
    setup_logging(os.path.join(args.output, "filter_check.log"))

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

    total_fields = sum(len(fields) for fields in groups.values())
    current_field = 0

    for group, fields in groups.items():
        logging.info("Processing group: %s (fields: %d)", group, len(fields))
        for field in fields:
            current_field += 1
            if args.field and args.field != field:
                continue

            logging.info("Processing field %d/%d: %s.%s", current_field, total_fields, group, field)
            try:
                process_field(session, token_manager, group, field, args.output, args.delay)
            except Exception as e:
                logging.error("Error processing %s.%s: %s", group, field, e)
                logging.error("Traceback: %s", traceback.format_exc())

    logging.info("All done. Results in %s", os.path.abspath(args.output))


# -------------------- CLI --------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Medicraft — fetch raw data and filtered data for each group/field")
    p.add_argument("--email",
                   help="login email (or set MEDICRAFT_EMAIL env var)")
    p.add_argument("--password",
                   help="login password (or set MEDICRAFT_PASSWORD env var)")
    p.add_argument("--groups",
                   help="comma-separated groups to run (default: all)")
    p.add_argument("--field", help="only this field within groups")
    p.add_argument("--delay",
                   type=float,
                   default=2.0,
                   help="delay between requests (seconds)")
    p.add_argument("--output", default="results", help="output directory")
    return p.parse_args()


if __name__ == "__main__":
    try:
        args = parse_args()
        run(args)
    except Exception:
        traceback.print_exc()
        logging.error("Unhandled exception; aborting")