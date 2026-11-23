import csv
import sys

import numpy as np
from sentence_transformers import SentenceTransformer

CSV_PATH = "rag/feedback.csv"
MODEL_NAME = "all-MiniLM-L6-v2"

with open(CSV_PATH, encoding="utf-8-sig", newline="") as f:
    rows = list(csv.DictReader(f))

if not rows or "text" not in rows[0]:
    print("Columns mil nahi rahe:", list(rows[0].keys()) if rows else "file khali hai")
    sys.exit(1)

model = SentenceTransformer(MODEL_NAME)
vectors = model.encode([r["text"] for r in rows], normalize_embeddings=True)

query = " ".join(sys.argv[1:]) or "app login problem"
qv = model.encode([query], normalize_embeddings=True)[0]

scores = vectors @ qv
top = np.argsort(-scores)[:5]

for i in top:
    r = rows[i]
    print(f"score={scores[i]:.3f} doc={r['doc_id']} category={r['predicted_category']}")
    print(r["text"][:200])
    print("---")