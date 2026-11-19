import json
import sys

import numpy as np
from sentence_transformers import SentenceTransformer

EMB_PATH = "rag/embeddings.json"
MODEL_NAME = "all-MiniLM-L6-v2"

with open(EMB_PATH, encoding="utf-8") as f:
    items = json.load(f)

vectors = np.array([it["vector"] for it in items])
model = SentenceTransformer(MODEL_NAME)

query = " ".join(sys.argv[1:]) or "how many battery tickets are open?"
qv = model.encode([query], normalize_embeddings=True)[0]

scores = vectors @ qv
top = np.argsort(-scores)[:3]

for i in top:
    it = items[i]
    print(f"score={scores[i]:.3f} page={it['page']} id={it['id']}")
    print(it["text"][:200].replace("\n", " | "))
    print("---")