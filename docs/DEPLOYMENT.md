# Deployment (Tier-1 template)

This project is a reusable Tier-1 control plane:
Ingest → Normalize → Decide → Act → Observe → Improve

This doc describes the minimal steps to run it locally or in a container.

---

## Environment variables
Copy `.env.example` to `.env` and fill values:

- `PORT` (default 8080)
- `OPS_API_KEY` (used by protected /ops endpoints; added in Group 3)

Do not commit `.env`.

---

## Local run (venv)
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080
