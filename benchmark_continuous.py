import os
import time
import base64
import asyncio
import concurrent.futures
import collections
import threading
from openai import OpenAI
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
MODEL_URL = "http://127.0.0.1:30000/v1"
MODEL_NAME = "google/gemma-4-31B-it"
IMG_FOLDER = "1000x"
PROMPT = "Describe this image in complete detail within 500 words. Include every visible element, text, color, layout, and context."
MAX_TOKENS = 700
TEMPERATURE = 0.0

MAX_REQUESTS_PER_MINUTE = 1000
CONCURRENT_LIMIT = 50
MAX_WORKERS = 200

# ═══════════════════════════════════════════════════════════════
# RATE LIMITER
# ═══════════════════════════════════════════════════════════════
class RateLimiter:
    def __init__(self, max_per_minute):
        self.max_per_minute = max_per_minute
        self.window = 60.0
        self._timestamps = collections.deque()
        self._lock = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now = time.time()
                while self._timestamps and self._timestamps[0] < now - self.window:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_per_minute:
                    self._timestamps.append(now)
                    return
                wait_time = self._timestamps[0] + self.window - now + 0.05
            time.sleep(wait_time)

rate_limiter = RateLimiter(MAX_REQUESTS_PER_MINUTE)

# ═══════════════════════════════════════════════════════════════
# SETUP
# ═══════════════════════════════════════════════════════════════
client = OpenAI(api_key="not-needed", base_url=MODEL_URL)

def get_images(folder):
    exts = {'.jpg', '.jpeg', '.png', '.webp', '.gif'}
    imgs = [f for f in Path(folder).iterdir() if f.suffix.lower() in exts]
    return sorted(imgs)

def process_one_image(image_path):
    rate_limiter.acquire()
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
        print(f"  ❌ {image_path.name}: {e}")
        return False, 0, 0, elapsed

async def run_continuous():
    images = get_images(IMG_FOLDER)
    total_images = len(images)
    print(f"Total images: {total_images}")
    print(f"Rate limit: {MAX_REQUESTS_PER_MINUTE}/min, Concurrent: {CONCURRENT_LIMIT}")
    print(f"NO batching — continuous flow")
    print(f"{'═' * 60}")

    loop = asyncio.get_running_loop()
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))

    sem = asyncio.Semaphore(CONCURRENT_LIMIT)
    total_success = 0
    total_fail = 0
    total_input_tokens = 0
    total_output_tokens = 0
    completed = 0
    overall_start = time.time()

    async def _worker(img_path):
        nonlocal total_success, total_fail, total_input_tokens, total_output_tokens, completed
        async with sem:
            success, inp_tok, out_tok, elapsed = await loop.run_in_executor(
                None, process_one_image, img_path)
            if success:
                total_success += 1
                total_input_tokens += inp_tok
                total_output_tokens += out_tok
            else:
                total_fail += 1
            completed += 1

            # Print progress every 50 samples
            if completed % 50 == 0:
                elapsed_total = time.time() - overall_start
                rate = (total_success / elapsed_total) * 60 if elapsed_total > 0 else 0
                tok_rate = ((total_input_tokens + total_output_tokens) / elapsed_total) * 60
                print(f"  [{completed}/{total_images}] ok={total_success} err={total_fail} "
                      f"| {rate:.0f} samples/min | {tok_rate:,.0f} tok/min")

    tasks = [asyncio.create_task(_worker(img)) for img in images]
    await asyncio.gather(*tasks)

    overall_elapsed = time.time() - overall_start
    overall_samples_per_min = (total_success / overall_elapsed) * 60
    overall_tokens_per_min = ((total_input_tokens + total_output_tokens) / overall_elapsed) * 60

    print(f"\n{'═' * 60}")
    print(f"  FINAL RESULTS (CONTINUOUS METHOD)")
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
    asyncio.run(run_continuous())
