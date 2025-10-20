import os
import gdown

# Folder ID from your link:
# https://drive.google.com/drive/folders/1-ybcSvjPf6pYHIMzBj-hiAhKPIxn6Isn?usp=sharing
FOLDER_ID = "1-ybcSvjPf6pYHIMzBj-hiAhKPIxn6Isn"

RAW_DIR = "../data/raw"

def download_from_drive_folder():
    os.makedirs(RAW_DIR, exist_ok=True)

    print(f"⬇️ Downloading all files from Google Drive folder ({FOLDER_ID})...")
    gdown.download_folder(
        id=FOLDER_ID,
        output=RAW_DIR,
        quiet=False,
        use_cookies=False
    )

    print(f"✅ All raw files downloaded into {RAW_DIR}")
