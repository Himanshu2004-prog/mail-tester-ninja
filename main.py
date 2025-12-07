import requests
import functions_framework
import os
import time

API_KEY = os.getenv("MAILTESTER_API_KEY", "")

RATE_LIMIT_DELAY_SECONDS = float(os.getenv("MAILTESTER_RATE_DELAY_SECONDS", "0.9"))


VALID_CODES = {"ok", "catch_all", "catchall"}


def validate_email(email):
    """
    Validates a single email address using the MailTester Ninja API.
    Returns a dict with:
      - is_valid: bool (ok/catch_all treated as valid)
      - status_code: str (e.g. "ok", "catch_all", "invalid", etc.)
      - details: full API response (if available)
      - error: optional error message
    """
    url = "https://happy.mailtester.ninja/ninja"
    params = {"email": email, "key": API_KEY}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        try:
            result = response.json()
            status_code = (result.get("code") or "").lower()

            return {
                "is_valid": status_code in VALID_CODES,
                "status_code": status_code,
                "details": result,
                "error": None,
            }
        except requests.exceptions.JSONDecodeError:
            return {
                "is_valid": False,
                "status_code": "json_error",
                "details": None,
                "error": "Failed to decode API response",
            }

    except requests.exceptions.RequestException as e:
        return {
            "is_valid": False,
            "status_code": "request_error",
            "details": None,
            "error": f"API request failed: {str(e)}",
        }


def extract_root_domain(company_website):
    """
    Extracts the root domain from a company website URL.
    """
    domain = (
        company_website.replace("http://", "")
        .replace("https://", "")
        .replace("www.", "")
        .split("/")[0]
        .strip()
    )
    return domain


def generate_prioritized_email_patterns(first_name, last_name, domain):
    """
    Generates a list of email patterns based on the top 5 requested formats.
    first_name and last_name are treated as lowercase.
    """
    if not first_name:
        return []

    first_name = first_name.lower()
    first_initial = first_name[0].lower()

    email_patterns = []

    if last_name:
        last_name = last_name.lower()
        last_initial = last_name[0].lower()

        email_patterns.extend(
            [
                f"{first_name}@{domain}",             
                f"{first_initial}{last_name}@{domain}",  
                f"{first_name}.{last_name}@{domain}",    
                f"{first_name}{last_name}@{domain}",     
                f"{first_initial}.{last_name}@{domain}", 
            ]
        )

        email_patterns.extend(
            [
                f"{first_initial}{last_initial}@{domain}",
                f"{first_name}{last_initial}@{domain}",
                f"{last_name}{first_name}@{domain}",
                f"{last_name}@{domain}",
            ]
        )

    else:
       
        email_patterns.append(f"{first_name}@{domain}")

    email_patterns = list(dict.fromkeys(email_patterns))
    return email_patterns


def find_valid_email(first_name, last_name, company_website):
    """
    Attempts to find a valid email by checking the prioritized patterns
    for a single person (single lead).
    """
    domain = extract_root_domain(company_website)

    # Normalize names
    first = (first_name or "").strip().lower()
    last = (last_name or "").strip().lower() if last_name else None

    email_patterns = generate_prioritized_email_patterns(first, last, domain)
    attempts = 0
    last_result = None

    for email in email_patterns:
        attempts += 1
        result = validate_email(email)
        last_result = result

        if result["is_valid"]:
            return {
                "email_found": email,
                "status_code": result.get("status_code"),
                "validation_result": result.get("details"),
                "total_credits_used": attempts,
                "error": result.get("error"),
            }

        # Respect MailTester rate limits between attempts
        time.sleep(RATE_LIMIT_DELAY_SECONDS)

    # No valid/catch_all email found
    return {
        "email_found": "no valid email",
        "status_code": last_result.get("status_code") if last_result else None,
        "validation_result": last_result.get("details") if last_result else None,
        "total_credits_used": attempts,
        "error": last_result.get("error") if last_result else "No pattern returned as valid",
    }


@functions_framework.http
def find_email(request):
    """HTTP Cloud Function to find a professional email.
    Args:
        request (flask.Request): The request object.
    Returns:
        JSON response (dict, status_code)
    """
    request_json = request.get_json(silent=True)

    if not request_json or "first_name" not in request_json or "company_website" not in request_json:
        return {
            "error": "Missing required parameters. Please provide first_name and company_website"
        }, 400

    first_name = request_json.get("first_name")
    last_name = request_json.get("last_name")  # Optional
    company_website = request_json["company_website"]

    if not first_name or not company_website:
        return {
            "error": "first_name and company_website cannot be empty"
        }, 400

    result = find_valid_email(first_name, last_name, company_website)

    return result
