import time
import os
from supabase import create_client
from dotenv import load_dotenv
import requests
from google import genai

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SERP_API_KEY = os.getenv("SERP_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
client = genai.Client(api_key=GEMINI_API_KEY)


def get_next_job():
    return supabase.rpc("infy_pick_next_job").execute()


def mark_status(job_id, status, error=None):
    data = {"enrichment_status": status}
    if error:
        data["enrichment_error"] = error

    supabase.table("infy_raw_leads").update(data).eq("id", job_id).execute()


def serp_search(domain):
    r = requests.get("https://serpapi.com/search", params={
        "engine": "google",
        "q": domain,
        "api_key": SERP_API_KEY
    }, timeout=30)
    return r.json()


def gemini_enrich(lead, serp):
    prompt = f"""
    We have a business lead.

    Name: {lead['first_name']} {lead['last_name']}
    Email: {lead['email']}
    Company: {lead['firm_name']}
    Declared title: {lead['declared_title']}
    Website: {lead['website_url']}

    SERP data:
    {serp}

    Return strict JSON with:
    normalized_title
    seniority
    department
    revenue_estimate
    buying_intent (0-100)
    confidence_score (0-100)
    """

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt
    )

    return response.text




def main():
    print("INFY Vault Worker started")

    while True:
        try:
            job = get_next_job()

            if not job.data:
                time.sleep(2)
                continue

            lead = job.data[0]
            job_id = lead["id"]

            print("Processing", lead["job_id"])

            mark_status(job_id, "running")

            serp = serp_search(lead["website_url"])
            ai = gemini_enrich(lead, serp)

            supabase.table("infy_enriched_leads").insert({
                "raw_lead_id": job_id,
                "job_id": lead["job_id"],
                "tenant_id": lead["tenant_id"],
                "ai_output": ai
            }).execute()

            mark_status(job_id, "completed")

        except Exception as e:
            print("ERROR", e)
            if "job_id" in locals():
                mark_status(job_id, "failed", str(e))
            time.sleep(3)


if __name__ == "__main__":
    main()
