from pathlib import Path


TARGET_DIR = Path(r"C:\Users\xty\Desktop\grokzhuce\GEMINI\gemini_accounts")
OUTPUT_FILE = "all_account.json"


def collect_json_contents(folder: Path):
    json_files = sorted(
        p for p in folder.glob("*.json") if p.is_file() and p.name != OUTPUT_FILE
    )

    contents = []
    for file_path in json_files:
        content = file_path.read_text(encoding="utf-8").strip()
        if content:
            contents.append(content)
    return contents


def insert_before_last_bracket(output_path: Path, content: str, add_comma: bool):
    current = output_path.read_text(encoding="utf-8")
    idx = current.rfind("]")
    if idx == -1:
        raise ValueError(f"{output_path} 内容不合法，未找到 ']'")

    suffix = "," if add_comma else ""
    updated = current[:idx] + content + suffix + current[idx:]
    output_path.write_text(updated, encoding="utf-8")


def main():
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    output_path = TARGET_DIR / OUTPUT_FILE

    # all_account 初始为 []
    output_path.write_text("[]", encoding="utf-8")

    contents = collect_json_contents(TARGET_DIR)
    total = len(contents)

    for i, content in enumerate(contents):
        is_last = i == total - 1
        insert_before_last_bracket(
            output_path=output_path,
            content=content,
            add_comma=not is_last,
        )

    print(f"完成：已合并 {total} 个 json 到 {output_path}")


if __name__ == "__main__":
    main()
