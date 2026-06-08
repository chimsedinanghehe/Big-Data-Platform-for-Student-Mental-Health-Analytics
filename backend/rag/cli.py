from backend.rag.ingest import build_index
from backend.rag.config import get_settings
from backend.rag.service import answer_question


def chat_loop():
    print("\nĐã bắt đầu chế độ trò chuyện. Nhập /exit để quay lại menu.\n")
    chat_history = []

    while True:
        question = input("Bạn: ").strip()

        if not question:
            continue

        if question.lower() in ["/exit", "exit", "quit", "/quit"]:
            print("Đang quay lại menu chính...\n")
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
                print(f"[gỡ lỗi] truy vấn đã viết lại: {standalone_query}")

            print("\nTrợ lý:")
            print(answer)

            if sources:
                print("\nNguồn tham khảo:")
                for source in sources:
                    print(f"- {source}")

            print()

            chat_history.append({"role": "user", "content": question})
            chat_history.append({"role": "assistant", "content": answer})
            chat_history = chat_history[-10:]
        except Exception as exc:
            print(f"\nLỗi: {exc}\n")


def main():
    print("=" * 50)
    print("HỆ THỐNG RAG HỖ TRỢ SỨC KHỎE TINH THẦN")
    print("=" * 50)

    while True:
        print("\nTùy chọn:")
        print("1. Xây dựng chỉ mục RAG")
        print("2. Bắt đầu trò chuyện")
        print("3. Thoát")

        choice = input("\nChọn tùy chọn: ").strip()

        if choice == "1":
            build_index()
        elif choice == "2":
            chat_loop()
        elif choice == "3":
            print("\nĐang thoát hệ thống...")
            break
        else:
            print("\nTùy chọn không hợp lệ")


if __name__ == "__main__":
    main()
