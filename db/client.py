import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()


def get_supabase_client() -> Client:
    url: str = os.environ["SUPABASE_URL"]
    key: str = os.environ["SUPABASE_SECRET_KEY"]

    if not url.startswith("http"):
        url = f"https://{url}"

    return create_client(url, key)
