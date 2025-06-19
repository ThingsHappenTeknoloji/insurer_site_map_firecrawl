#!/bin/bash

# You need to create public private keypair and add to Yandex compute machine the public key file

# copy source code to Yandex compute machine
scp ../.env sami@89.169.182.188:/home/sami/.env
scp ../embed_insurer_pages.py sami@89.169.182.188:/home/sami/embed_insurer_pages.py
scp ../requirements.txt sami@89.169.182.188:/home/sami/requirements.txt

#start an ssh session
ssh -l sami 89.169.182.188
#inside the ssh session
#install dependencies
cd /home/sami
sudo apt update
sudo apt install python3 python3-pip
sudo pip install psycopg2=binary
sudo pip install aiohttp
sudo pip install qdrant-client==1.2
sudo pip install tiktoken
sudo pip install -r requirements.txt
#inside the ssh session
#run the script
python embed_insurer_pages.py
#end the ssh session
exit

