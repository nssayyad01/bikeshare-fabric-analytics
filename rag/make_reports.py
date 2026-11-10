import os

import psycopg2
from dotenv import load_dotenv
from fpdf import FPDF

load_dotenv()

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM maintenance_tickets")
total = cur.fetchone()[0]

cur.execute("""
    SELECT issue_category, status, COUNT(*)
    FROM maintenance_tickets
    GROUP BY issue_category, status
    ORDER BY issue_category, status
""")
rows = cur.fetchall()
conn.close()

pdf = FPDF()
pdf.add_page()
pdf.set_font("Helvetica", "B", 16)
pdf.cell(0, 10, "Maintenance Tickets Summary (simulated data)", new_x="LMARGIN", new_y="NEXT")
pdf.set_font("Helvetica", "", 11)
pdf.cell(0, 8, f"Total tickets: {total}", new_x="LMARGIN", new_y="NEXT")
pdf.ln(4)

pdf.set_font("Helvetica", "B", 11)
pdf.cell(70, 8, "Issue category", border=1)
pdf.cell(40, 8, "Status", border=1)
pdf.cell(30, 8, "Count", border=1, new_x="LMARGIN", new_y="NEXT")

pdf.set_font("Helvetica", "", 11)
for category, status, n in rows:
    pdf.cell(70, 8, str(category), border=1)
    pdf.cell(40, 8, str(status), border=1)
    pdf.cell(30, 8, str(n), border=1, new_x="LMARGIN", new_y="NEXT")

os.makedirs(os.path.join("rag", "reports"), exist_ok=True)
out_path = os.path.join("rag", "reports", "maintenance_tickets_summary.pdf")
pdf.output(out_path)
print("Saved", out_path)