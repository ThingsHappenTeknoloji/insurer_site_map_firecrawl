scp ../.env sami@89.169.182.188:/home/sami/.env
scp ../embed_insurer_pages.py sami@89.169.182.188:/home/sami/embed_insurer_pages.py
scp ../requirements.txt sami@89.169.182.188:/home/sami/requirements.txt

ssh -l sami 89.169.182.188

cd /home/sami

pip install -r requirements.txt

python embed_insurer_pages.py
