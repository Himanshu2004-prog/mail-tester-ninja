import requests
import functions_framework
import os
import time

# MailTester Ninja API key (env override supported)
API_KEY = os.getenv("MAILTESTER_API_KEY", "sub_1STNPEAJu6gy4fiY2Pe1D7H8")
# Conservative delay to respect MailTester rate limits (Pro: ~1 per 860ms)
RATE_LIMIT_DELAY_SECONDS = float(os.getenv("MAILTESTER_RATE_DELAY_SECONDS", "0.9"))

def validate_email(email):
    url = "https://happy.mailtester.ninja/ninja"
    params = {"email": email, "key": API_KEY}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        try:
            result = response.json()
            return {
                "is_valid": result.get("code") == "ok",
                "details": result
            }
        except requests.exceptions.JSONDecodeError:
            return {
                "is_valid": False,
                "error": "Failed to decode API response"
            }

    except requests.exceptions.RequestException as e:
        return {
            "is_valid": False,
            "error": f"API request failed: {str(e)}"
        }

def extract_root_domain(company_website):
    domain_parts = company_website.replace("http://", "").replace("https://", "").replace("www.", "").split("/")[0]
    return domain_parts

def generate_prioritized_email_patterns(first_name, last_name, domain):
    first_initial = first_name[0].lower()
    email_patterns = []

    if last_name:
        last_initial = last_name[0].lower()
        email_patterns.extend([
            f"{first_name}.{last_name}@{domain}",
            f"{first_name}{last_initial}@{domain}",
            f"{first_initial}{last_name}@{domain}",
            f"{last_name}{first_name}@{domain}",
            f"{first_name}@{domain}",
            f"{last_name}@{domain}",
            f"{first_name}{last_name}@{domain}",
        ])
    else:
        email_patterns.extend([
            f"{first_name}@{domain}",
        ])

    # Optional: remove duplicates while preserving order
    email_patterns = list(dict.fromkeys(email_patterns))
    return email_patterns

def find_valid_email(first_name, last_name, company_website):
    domain = extract_root_domain(company_website)
    email_patterns = generate_prioritized_email_patterns(first_name.lower(), last_name.lower() if last_name else None, domain)
    attempts = 0

    for email in email_patterns:
        attempts += 1
        result = validate_email(email)

        if result["is_valid"]:
            return {
                "email_found": email,
                "validation_result": result["details"],
                "total_credits_used": attempts
            }
        # Respect MailTester rate limits between attempts
        time.sleep(RATE_LIMIT_DELAY_SECONDS)

    return {
        "email_found": "no valid email",
        "validation_result": None,
        "total_credits_used": attempts
    }

@functions_framework.http
def find_email(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
    Returns:
        JSON response (dict, status_code)
    """
    request_json = request.get_json(silent=True)

    # Check if required parameters are present
    if not request_json or "first_name" not in request_json or "company_website" not in request_json:
        return {
            "error": "Missing required parameters. Please provide first_name and company_website"
        }, 400

    # Extract parameters
    first_name = request_json.get("first_name")
    last_name = request_json.get("last_name")  # Optional
    company_website = request_json["company_website"]

    # Find valid email
    result = find_valid_email(first_name, last_name, company_website)

    # Return JSON response
    return result
