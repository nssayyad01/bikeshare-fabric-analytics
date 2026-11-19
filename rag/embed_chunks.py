import json

from sentence_transformers import SentenceTransformer

CHUNKS_PATH = "rag/chunks.json"
OUT_PATH = "rag/embeddings.json"

with open(CHUNKS_PATH, encoding="utf-8") as f:
    chunks = json.load(f)

model = SentenceTransformer("all-MiniLM-L6-v2")
vectors = model.encode([c["text"] for c in chunks], normalize_embeddings=True)

out = [{**c, "vector": v.tolist()} for c, v in zip(chunks, vectors)]
with open(OUT_PATH, "w", encoding="utf-8") as f:
    json.dump(out, f)

print("Embedded chunks:", len(out))
print("Vector size:", len(out[0]["vector"]))
print("Saved", OUT_PATH)