from backend.rag.ingest import build_index
from backend.rag.config import get_settings
from backend.rag.service import answer_question


def chat_loop():
    print("\nChat mode started. Type /exit to return to menu.\n")
    chat_history = []

    while True:
        question = input("You: ").strip()

        if not question:
            continue

        if question.lower() in ["/exit", "exit", "quit", "/quit"]:
            print("Returning to main menu...\n")
            break

        try:
            result = answer_question(question=question, chat_history=chat_history, return_metadata=True)

            if isinstance(result, dict):
                answer = result.get("answer", "")
                sources = result.get("sources", [])
                standalone_query = result.get("standalone_query", question)
            else:
                answer = str(result)
                sources = []
                standalone_query = question

            if get_settings().debug_rag:
                print(f"[debug] rewritten query: {standalone_query}")

            print("\nBot:")
            print(answer)

            if sources:
                print("\nSources:")
                for source in sources:
                    print(f"- {source}")

            print()

            chat_history.append({"role": "user", "content": question})
            chat_history.append({"role": "assistant", "content": answer})
            chat_history = chat_history[-10:]
        except Exception as exc:
            print(f"\nError: {exc}\n")


def main():
    print("=" * 50)
    print("MENTAL HEALTH RAG SYSTEM")
    print("=" * 50)

    while True:
        print("\nOptions:")
        print("1. Build RAG Index")
        print("2. Start Chat")
        print("3. Exit")

        choice = input("\nSelect option: ").strip()

        if choice == "1":
            build_index()
        elif choice == "2":
            chat_loop()
        elif choice == "3":
            print("\nExiting system...")
            break
        else:
            print("\nInvalid option")


if __name__ == "__main__":
    main()
