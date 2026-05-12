# 💊 Pharma Intelligence ChatBot (AI Agent)

A premium, enterprise-grade AI agent designed for **Pharmaceutical Business Intelligence**. This application allows users to query complex SQL databases using natural language (Roman Urdu/English) and receive strategic insights, automated charts, and interactive tables.

---

## 🚀 Key Features

- **Strategic SQL Generation**: Automatically converts complex business questions into optimized PostgreSQL queries with self-correction capabilities.
- **Specialized Map Expert Agent**: A dedicated sub-agent for geospatial intelligence that mandates 'address', 'latitude', and 'longitude' extraction for high-precision Mapbox and Globe visualizations.
- **RAG-Powered Memory**: Uses a custom "Knowledge Base" (`knowledge/`) to understand pharmaceutical jargon, specific database mappings, and regional sales logic.
- **PKR Currency Enforcement**: All financial outputs and LLM summaries are strictly formatted using **PKR / Rs.** currency standards.
- **Premium UI/UX**: Overhauled Streamlit interface featuring glassmorphism, modern typography (Outfit/Inter), and a minimalist sidebar for professional usage.
- **Automated Visualization**: Smart charting logic that automatically selects the best visualization (Bar, Line, etc.) while filtering out non-metric data.
- **Persistent & Readable History**: Chat logs are saved in human-readable, pretty-printed JSON (`indent=4`) under the central `admin@gmail.com` directory, ensuring easy debugging and data persistence.
- **Enterprise Security**: 
  - User-specific chat isolation (consolidated under administrator profiles).
  - Secure connection retry logic (3 attempts) with 30s timeouts for robust database interaction.
- **Advanced Geocoding Cache**: Persistent disk-based caching (`geo_cache.json`) ensures zero-latency address lookups after the first retrieval.

---

## 🛠️ Project Structure

```text
├── app.py                # Main Streamlit Dashboard (Enforced admin session)
├── agent_core.py         # AI Logic, Map Expert Prompt, and SQL Generation
├── db.py                 # Database Pool & Parameterized Auth
├── requirements.txt      # Python Dependencies
├── knowledge/            # Intelligence Layer (Business Terms & SQL Patterns)
├── chats/                # Structured Chat History (Consolidated under admin@gmail.com)
└── prisma/               # Database Architecture & Schema
```

---

## ⚙️ Setup & Installation

### 1. Prerequisites
- Python 3.9+
- PostgreSQL Database (Neon.tech or similar)

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the root directory with the following keys:
```env
DATABASE_URL=postgresql://user:pass@host/dbname
LLM_API_KEY=your_openrouter_api_key
LLM_BASE_URL=https://openrouter.ai/api/v1
LLM_MODEL=meta-llama/llama-3.3-70b-instruct
```

---

## 💻 How to Run

Start the web dashboard:
```bash
streamlit run app.py
```

---

## 💡 Strategic Recommendations
- **Naming Conventions**: The agent accurately maps "Antibiotics" to `Product Group 1` and handles complex area names via `ILIKE`.
- **Currency Standards**: Always expect reports in **Rs.** as the system is optimized for Pakistani market analysis.
- **Map Queries**: Use specific prompts like "show on map" to trigger the Map Expert Agent for the best coordinate accuracy.

---

Developed for **Advanced Pharma Sales Analysis & Strategic Intelligence**. 🚀👔🛠️📈
