# %%
import os
import glob
import pandas as pd
import torch

from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline

# %%
device = "cuda:0" if torch.cuda.is_available() else "cpu"
torch_dtype = torch.float32 # torch.float16 if torch.cuda.is_available() else torch.float32

# %%
model_id = "distil-whisper/distil-medium.en"

model = AutoModelForSpeechSeq2Seq.from_pretrained(
    model_id, low_cpu_mem_usage=True, use_safetensors=True
)
model.to(device)

# %%
# using alignment heads from whisper-medium.en 
# note that this is potentially suboptimal---see discussion at:
# https://github.com/huggingface/distil-whisper/issues/3
alignment_heads = [[11, 4], [14, 1], [14, 12], [14, 14], [15, 4], [16, 0], [16, 4], [16, 9], [17, 12], [17, 14], [18, 7], [18, 10], [18, 15], [20, 0], [20, 3], [20, 9], [20, 14], [21, 12]]
model.generation_config.alignment_heads = alignment_heads

# %%
processor = AutoProcessor.from_pretrained(model_id)

# %%
pipe = pipeline(
    "automatic-speech-recognition",
    model=model,
    tokenizer=processor.tokenizer,
    feature_extractor=processor.feature_extractor,
    max_new_tokens=128,
    chunk_length_s=15,
    batch_size=16,
    torch_dtype=torch_dtype,
    device=device,
)

# %%
DATA_PATH = "/data/babyview/audio/"

dataset = sorted(glob.glob(f"{DATA_PATH}**/*.mp3", recursive=True))

# %%
for f in dataset:
    file_name = os.path.splitext(f)[0]
    print(file_name)

    out_path = file_name.replace("audio", "transcripts")
    if os.path.isfile(f"{out_path}.csv"):
        continue
    
    res = pipe(f"{file_name}.mp3", return_timestamps=True)
    res_df = pd.DataFrame({
        'start_time': [c['timestamp'][0] for c in res['chunks']],
        'end_time': [c['timestamp'][1] for c in res['chunks']],
        'text': [c['text'] for c in res['chunks']]
    })
    os.makedirs(os.path.split(out_path)[0], exist_ok=True)
    res_df.to_csv(f"{out_path}.csv", index_label="utterance_no")
