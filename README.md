# 💊 Pharma Intelligence ChatBot (AI Agent)

A premium, enterprise-grade AI agent designed for **Pharmaceutical Business Intelligence**. This application allows users to query complex SQL databases using natural language (Roman Urdu/English) and receive strategic insights, automated charts, and interactive tables.

---

## 🚀 Key Features

- **Strategic SQL Generation**: Automatically converts complex business questions into optimized PostgreSQL queries.
- **RAG-Powered Memory**: Uses a custom "Knowledge Base" (`knowledge/`) to understand pharmaceutical jargon and specific database mappings.
- **Premium UI/UX**: Overhauled Streamlit interface with glassmorphism, custom Google Fonts (Outfit/Inter), and smooth animations.
- **Automated Visualization**: Smart charting logic that automatically selects the best visualization (Bar, Line, etc.) for your data while blacklisting non-metric columns.
- **Enterprise Security**: 
  - Database-backed authentication for Managers and Users.
  - User-specific chat isolation (each user sees only their own history).
  - Secure "Master Key" fallback for administrators.
- **Reliable Message Management**: Integrated 🗑️ deletion system that removes both the question AND its specific output from memory.

---

## 🛠️ Project Structure

```text
├── app.py                # Main Streamlit Application (Web Portal)
├── agent.py              # CLI Version for quick testing
├── requirements.txt      # Python Dependencies
├── knowledge/            # Intelligence Layer
│   ├── business_terms.md # Dictionary of industry terminology
│   └── query_library.md  # Reference SQL patterns for the LLM
├── chats/                # Encrypted/User-Specific Chat History
│   └── {user_email}/     # Isolated session storage & query cache
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

Run a quick query via CLI:
```bash
python agent.py "What were the total sales in Karachi last month?"
```

---

## 💡 Strategic Recommendations
- **Naming Conventions**: The agent currently maps "Antibiotics" to `Product Group 1` as per the available database labels.
- **Deletion**: Use the 🗑️ icon to clean up conversation pairs. This helps in keeping the LLM context clear for long sessions.
- **Custom Bricks**: Always use `ILIKE` for geographical searches (already handled by the agent).

---

Developed for **Advanced Pharma Sales Analysis**. 🚀👔🛠️📈
