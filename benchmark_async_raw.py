import os
import time
import base64
import asyncio
import httpx
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
MODEL_URL = "http://127.0.0.1:30000/v1/chat/completions"
MODEL_NAME = "google/gemma-4-26B-A4B-it"
IMG_FOLDER = "1000x"
PROMPT = "Describe this image in complete detail within 500 words. Include every visible element, text, color, layout, and context."
MAX_TOKENS = 700
TEMPERATURE = 0.0

CONCURRENT_LIMIT = 80  # তোমার GPU যত handle করতে পারে
TIMEOUT = 120

# ═══════════════════════════════════════════════════════════════

def get_images(folder):
    exts = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
    return sorted([f for f in Path(folder).iterdir() if f.suffix.lower() in exts])

def encode_image(image_path):
    with open(image_path, "rb") as f:
        img_b64 = base64.b64encode(f.read()).decode("utf-8")
    ext = image_path.suffix.lower().lstrip('.')
    mime_map = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "gif": "gif", "webp": "webp"}
    mime = mime_map.get(ext, "jpeg")
    return f"data:image/{mime};base64,{img_b64}"

async def process_one(client, sem, image_path):
    async with sem:
        start = time.time()
        try:
            data_url = encode_image(image_path)
            payload = {
                "model": MODEL_NAME,
                "messages": [{
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": data_url}},
                        {"type": "text", "text": PROMPT}
                    ]
                }],
                "max_tokens": MAX_TOKENS,
                "temperature": TEMPERATURE,
            }

            response = await client.post(MODEL_URL, json=payload, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            usage = data.get("usage", {})
            elapsed = time.time() - start
            return True, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0), elapsed

        except Exception as e:
            elapsed = time.time() - start
            return False, 0, 0, elapsed

async def run_async_raw():
    images = get_images(IMG_FOLDER)
    total_images = len(images)
    print(f"Total images: {total_images}")
    print(f"Concurrent: {CONCURRENT_LIMIT} (NO rate limiter — raw speed)")
    print(f"{'═' * 60}")

    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    total_success = 0
    total_fail = 0
    total_input_tokens = 0
    total_output_tokens = 0
    completed = 0
    overall_start = time.time()

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=CONCURRENT_LIMIT + 20, max_keepalive_connections=CONCURRENT_LIMIT),
    ) as client:

        async def _worker(img_path):
            nonlocal total_success, total_fail, total_input_tokens, total_output_tokens, completed
            success, inp, out, elapsed = await process_one(client, sem, img_path)
            if success:
                total_success += 1
                total_input_tokens += inp
                total_output_tokens += out
            else:
                total_fail += 1
            completed += 1

            if completed % 50 == 0:
                elapsed_total = time.time() - overall_start
                rate = (total_success / elapsed_total) * 60
                tok_rate = ((total_input_tokens + total_output_tokens) / elapsed_total) * 60
                print(f"  [{completed}/{total_images}] ok={total_success} err={total_fail} "
                      f"| {rate:.0f} samples/min | {tok_rate:,.0f} tok/min")

        tasks = [asyncio.create_task(_worker(img)) for img in images]
        await asyncio.gather(*tasks)

    overall_elapsed = time.time() - overall_start
    overall_samples_per_min = (total_success / overall_elapsed) * 60
    overall_tokens_per_min = ((total_input_tokens + total_output_tokens) / overall_elapsed) * 60

    print(f"\n{'═' * 60}")
    print(f"  FINAL RESULTS (PURE ASYNC — NO RATE LIMIT)")
    print(f"{'═' * 60}")
    print(f"  Total images:      {total_images}")
    print(f"  Success:           {total_success}")
    print(f"  Failed:            {total_fail}")
    print(f"  Total time:        {overall_elapsed:.1f}s ({overall_elapsed/60:.1f} min)")
    print(f"  Input tokens:      {total_input_tokens:,}")
    print(f"  Output tokens:     {total_output_tokens:,}")
    print(f"  Total tokens:      {total_input_tokens + total_output_tokens:,}")
    print(f"  ─────────────────────────────────")
    print(f"  Samples/min:       {overall_samples_per_min:.1f}")
    print(f"  Tokens/min:        {overall_tokens_per_min:,.0f}")
    print(f"  Avg tokens/sample: {(total_input_tokens+total_output_tokens)/max(total_success,1):.0f}")
    print(f"{'═' * 60}\n")

if __name__ == "__main__":
    asyncio.run(run_async_raw())
