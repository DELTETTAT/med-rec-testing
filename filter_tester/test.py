import requests

BASE_URL = "https://unifymedicraft.net/laravel/api"

login_url = f"{BASE_URL}/login"
login_payload = {"email": "subtest3mostafa@medicraft.com", "password": "SubTest3@mostafaTest"}
login_headers = {"Accept": "application/json", "Content-Type": "application/json"}

login_response = requests.post(login_url, headers=login_headers, json=login_payload)
login_data = login_response.json()
access_token = login_data["data"]["access_token"]
print("✅ Login successful, token:", access_token)

report_url = f"{BASE_URL}/reports/paymentAdHocReport"
report_headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/x-www-form-urlencoded"  
}

report_payload = {
    "filterData": 100,
    "show": 1,
    "columns_count": 20,

    # ✅ Filter example: Payment Id != 273
    "filters[payment][0][field]": "id",
    "filters[payment][0][group]": "payment",
    "filters[payment][0][operator]": "not_equal",
    "filters[payment][0][value]": "273",

    # ✅ Fields to fetch
    "payment[id]": "Payment Id",
    "payment[check_number]": "Payment Check Number",
    "payment[is_posted]": "Payment Status",
    "payment[invoice_id]": "Payment Invoice Id",
    "payment[receipt_id]": "Payment Receipt Id",
    "payment[deposit_id]": "Payment Deposit Id",
    "payment[patient_id]": "Payment Patient Id",
    "payment[created_by]": "Payment Created By",
    "payment[created_at]": "Payment Created Date",
    "payment[age_bucket]": "Created Age Bucket",
    "payment[posted_by]": "Payment Posted By",
    "payment[amount]": "Payment Amount",
    "payment[pay_date]": "Payment Pay Date",
    "payment[pay_date_bucket]": "Pay Age Bucket",
    "payment[post_date]": "Payment Post Date",
    "payment[payments_description]": "Payment Description",
    "payment[denial_description]": "Denial Description",
    "payment[type]": "Payment Type",
    "payment[reason]": "Payment Reason",
    "payment[denial_payment]": "Denial Amount",
}



report_response = requests.post(report_url, headers=report_headers, data=report_payload)

print("📊 Report Status:", report_response.status_code)
try:
    print("📊 Report Response:", report_response.json())
except Exception:
    print("Raw Response:", report_response.text)
