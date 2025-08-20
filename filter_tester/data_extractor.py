import requests
import time
import os
import json

BASE_URL = "https://unifymedicraft.net/laravel/api"

# ✅ Working login
def login():
    login_url = f"{BASE_URL}/login"
    login_payload = {"email": "subtest3mostafa@medicraft.com", "password": "SubTest3@mostafaTest"}
    login_headers = {"Accept": "application/json", "Content-Type": "application/json"}
    
    res = requests.post(login_url, headers=login_headers, json=login_payload)
    res.raise_for_status()
    return res.json()["data"]["access_token"]

# ✅ Groups + fields
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

FILTERS = ["equal","not_equal","greater_than","less_than","like","in"]

REPORT_URL = f"{BASE_URL}/reports/paymentAdHocReport"

def fetch_report(token, group, field, operator):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {
        "filterData": 100,
        "show": 1,
        "columns_count": 20,
        f"filters[{group}][0][field]": field,
        f"filters[{group}][0][group]": group,
        f"filters[{group}][0][operator]": operator,
        f"filters[{group}][0][value]": "test" if operator in ["like","in"] else "1",  # dummy value
        f"{group}[{field}]": f"{group} {field}"
    }

    res = requests.post(REPORT_URL, headers=headers, data=payload)
    return res

def main():
    token = login()
    print("✅ Logged in successfully")

    for group, fields in GROUPS.items():
        for field in fields:
            for operator in FILTERS:
                print(f"▶ Fetching {group} → {field} → {operator}")
                time.sleep(2)  # ⏳ 2-sec delay

                res = fetch_report(token, group, field, operator)

                folder_path = os.path.join("results", group, field)
                os.makedirs(folder_path, exist_ok=True)

                file_path = os.path.join(folder_path, f"{operator}.json")
                try:
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(res.json(), f, indent=2)
                except Exception:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(res.text)

if __name__ == "__main__":
    main()
