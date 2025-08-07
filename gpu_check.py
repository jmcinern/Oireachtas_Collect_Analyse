import torch
if torch.cuda.is_available():
    print("CUDA is available! GPU is ready to use.")
    print(f"Number of GPUs available: {torch.cuda.device_count()}")