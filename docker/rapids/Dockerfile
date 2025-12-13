# Custom RAPIDS notebook image with pinned PyTorch CUDA 12.x wheels
FROM rapidsai/notebooks:25.10a-cuda12.9-py3.10

RUN set -euo pipefail && \
    pip install --no-cache-dir \
    --extra-index-url https://download.pytorch.org/whl/cu124 \
    torch==2.5.1+cu124 torchvision==0.20.1+cu124 torchaudio==2.5.1+cu124 \
    mlflow \
    papermill
