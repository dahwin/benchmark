import os
import time
import base64
import asyncio
import concurrent.futures
from openai import OpenAI
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
MODEL_URL = "http://127.0.0.1:30000/v1"
MODEL_NAME = "google/gemma-4-26B-A4B-it"
IMG_FOLDER = "50x"
PROMPT = "Describe this image in complete detail within 500 words. Include every visible element, text, color, layout, and context."
MAX_TOKENS = 700
TEMPERATURE = 0.0

BATCH_SIZE = 500
CONCURRENT_LIMIT = 50
BATCH_PAUSE_SECONDS = 5
MAX_WORKERS = 200

# ═══════════════════════════════════════════════════════════════
# SETUP
# ═══════════════════════════════════════════════════════════════
client = OpenAI(api_key="not-needed", base_url=MODEL_URL)

def get_images(folder):
    exts = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
    imgs = [f for f in Path(folder).iterdir() if f.suffix.lower() in exts]
    return sorted(imgs)

def process_one_image(image_path):
    """Single image → API call → return (success, input_tokens, output_tokens, elapsed)"""
    start = time.time()
    try:
        with open(image_path, "rb") as f:
            img_b64 = base64.b64encode(f.read()).decode("utf-8")

        ext = image_path.suffix.lower().lstrip('.')
        mime_map = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "gif": "gif", "webp": "webp"}
        mime = mime_map.get(ext, "jpeg")

        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/{mime};base64,{img_b64}"}},
                    {"type": "text", "text": PROMPT}
                ]
            }],
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
        )

        usage = response.usage
        elapsed = time.time() - start
        return True, usage.prompt_tokens, usage.completion_tokens, elapsed

    except Exception as e:
        elapsed = time.time() - start
        return False, 0, 0, elapsed

async def run_batched():
    images = get_images(IMG_FOLDER)
    total_images = len(images)
    print(f"Total images: {total_images}")
    print(f"Batch size: {BATCH_SIZE}, Concurrent: {CONCURRENT_LIMIT}")
    print(f"{'═' * 60}")

    loop = asyncio.get_running_loop()
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))

    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    total_success = 0
    total_fail = 0
    total_input_tokens = 0
    total_output_tokens = 0
    overall_start = time.time()

    batch_num = 0
    idx = 0

    while idx < total_images:
        batch_num += 1
        batch_images = images[idx:idx + BATCH_SIZE]
        idx += BATCH_SIZE

        batch_start = time.time()
        batch_success = 0
        batch_fail = 0
        batch_input = 0
        batch_output = 0

        async def _worker(img_path):
            async with sem:
                return await loop.run_in_executor(None, process_one_image, img_path)

        tasks = [asyncio.create_task(_worker(img)) for img in batch_images]

        for coro in asyncio.as_completed(tasks):
            success, inp_tok, out_tok, elapsed = await coro
            if success:
                batch_success += 1
                batch_input += inp_tok
                batch_output += out_tok
            else:
                batch_fail += 1

        batch_elapsed = time.time() - batch_start
        total_success += batch_success
        total_fail += batch_fail
        total_input_tokens += batch_input
        total_output_tokens += batch_output

        samples_per_min = (batch_success / batch_elapsed) * 60 if batch_elapsed > 0 else 0
        tokens_per_min = ((batch_input + batch_output) / batch_elapsed) * 60 if batch_elapsed > 0 else 0

        print(f"\n{'━' * 60}")
        print(f"  BATCH {batch_num} | {len(batch_images)} samples | {batch_elapsed:.1f}s")
        print(f"  Success: {batch_success} | Failed: {batch_fail}")
        print(f"  Input tokens:  {batch_input:,}")
        print(f"  Output tokens: {batch_output:,}")
        print(f"  Throughput:    {samples_per_min:.1f} samples/min")
        print(f"  Token rate:    {tokens_per_min:,.0f} tokens/min")
        print(f"{'━' * 60}")

        if idx < total_images:
            print(f"  ⏸️  Pausing {BATCH_PAUSE_SECONDS}s...")
            await asyncio.sleep(BATCH_PAUSE_SECONDS)

    overall_elapsed = time.time() - overall_start
    overall_samples_per_min = (total_success / overall_elapsed) * 60
    overall_tokens_per_min = ((total_input_tokens + total_output_tokens) / overall_elapsed) * 60

    print(f"\n{'═' * 60}")
    print(f"  FINAL RESULTS (BATCHED METHOD)")
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
    asyncio.run(run_batched())
