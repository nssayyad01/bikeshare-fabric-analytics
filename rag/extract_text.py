from pypdf import PdfReader

reader = PdfReader("rag/reports/maintenance_tickets_summary.pdf")
print("Pages:", len(reader.pages))
for i, page in enumerate(reader.pages, start=1):
    print(f"--- page {i} ---")
    print(page.extract_text())