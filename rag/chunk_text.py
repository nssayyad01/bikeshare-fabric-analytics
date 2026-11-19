import json

from pypdf import PdfReader

PDF_PATH = "rag/reports/maintenance_tickets_summary.pdf"
OUT_PATH = "rag/chunks.json"
CHUNK_SIZE = 500
OVERLAP = 100

reader = PdfReader(PDF_PATH)
chunks = []

for page_no, page in enumerate(reader.pages, start=1):
    text = (page.extract_text() or "").strip()
    start = 0
    while start < len(text):
        piece = text[start:start + CHUNK_SIZE]
        chunks.append({
            "id": len(chunks),
            "source": PDF_PATH,
            "page": page_no,
            "text": piece,
        })
        start += CHUNK_SIZE - OVERLAP

with open(OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(chunks, f, indent=2, ensure_ascii=False)

print("Chunks:", len(chunks))
print("Saved", OUT_PATH)