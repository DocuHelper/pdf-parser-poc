from pprint import pprint

import pymupdf4llm
import pathlib

# llama_reader = pymupdf4llm.LlamaMarkdownReader()
# llama_docs = llama_reader.load_data("./자료집_20240226_좌담회_송파세모녀10주기.pdf")
# print(llama_docs)

md_text = pymupdf4llm.to_markdown(
    doc="./자료집_20240226_좌담회_송파세모녀10주기.pdf",
    write_images=True,
    # page_chunks=True
)
# print(len(md_text))
# for current in md_text:
#     if len(current["images"]) != 0:
#         pprint(current)
#
#     print("==========================================")
#
# print(md_text)
pathlib.Path("output.md").write_bytes(md_text.encode())

# print(md_text)