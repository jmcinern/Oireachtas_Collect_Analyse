from huggingface_hub import HfApi, login
import os

def push_csv_to_hub(csv_path, repo_id, filename=None):
    api = HfApi()
    if filename is None:
        filename = os.path.basename(csv_path)
    api.upload_file(
        path_or_fileobj=csv_path,
        path_in_repo=filename,
        repo_id=repo_id,
        repo_type="dataset"
    )
    print(f"Pushed {filename} to {repo_id} on Hugging Face Hub.")

if __name__ == "__main__":
    # Authenticate with your HF token (set as env variable)
    login(token=os.environ["HF_TOKEN"])
    csv_path = "debates_all_with_lang.csv"
    repo_id = "jmcinern/Oireachtas_XML"
    push_csv_to_hub(csv_path, repo_id)