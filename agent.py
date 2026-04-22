"""
agent.py — CLI version of the Pharma Intelligence Agent
Fixes / Features:
  - RAG context now loaded via agent_core (was missing before)
  - Conversation memory for follow-up questions
  - Streaming output to terminal
  - Self-correction retry loop via agent_core
"""

import sys
from agent_core import generate_and_run_sql, summarise_results_stream, suggest_followups

def run_interactive():
    """Interactive REPL with conversation memory."""
    print("\n💊 Pharma Intelligence Agent (CLI)")
    print("Type your question, or 'exit' to quit.\n")

    history = []   # list of {"role": ..., "content": ...}

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            print("Goodbye.")
            break

        result = generate_and_run_sql(question, history=history)

        if result["retries"] > 1:
            print(f"\n[Agent] ⚠  Needed {result['retries']} attempt(s) to generate valid SQL.")

        print(f"\n[SQL]\n{result['sql']}\n")

        if result["error"]:
            print(f"[Error] {result['error']}\n")
            # Still update history so follow-ups have context
            history.append({"role": "user",      "content": question})
            history.append({"role": "assistant",  "content": f"SQL failed: {result['error']}"})
            continue

        rows = result["results"]
        print(f"[Rows returned] {len(rows)}\n")
        print("[Answer] ", end="", flush=True)

        # Streaming output to terminal
        full_answer = ""
        for chunk in summarise_results_stream(question, rows):
            print(chunk, end="", flush=True)
            full_answer += chunk
        print("\n")

        # Follow-up suggestions
        followups = suggest_followups(question, rows)
        if followups:
            print("🔍 Suggested follow-ups:")
            for i, fq in enumerate(followups, 1):
                print(f"  {i}. {fq}")
            print()

        # Update conversation history (keep last 6 turns to avoid token bloat)
        history.append({"role": "user",     "content": question})
        history.append({"role": "assistant", "content": full_answer})
        if len(history) > 12:
            history = history[-12:]


def run_single(question: str):
    """Single-shot query (original CLI behaviour)."""
    print(f"\n💊 Pharma Intelligence Agent")
    print(f"Query: {question}\n")

    result = generate_and_run_sql(question)

    if result["retries"] > 1:
        print(f"⚠  Self-corrected after {result['retries']} attempt(s).")

    print(f"[SQL]\n{result['sql']}\n")

    if result["error"]:
        print(f"[Error] {result['error']}")
        return

    print(f"[Rows] {len(result['results'])}\n[Answer] ", end="", flush=True)
    for chunk in summarise_results_stream(question, result["results"]):
        print(chunk, end="", flush=True)
    print()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_single(" ".join(sys.argv[1:]))
    else:
        run_interactive()
