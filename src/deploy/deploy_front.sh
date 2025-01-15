#!/bin/bash

cd gst-front

git pull origin feature/web-front
echo "pull success"

cd src/frontend

export NVM_DIR=~/.nvm
source ~/.nvm/nvm.sh
echo "source success"

npm install
echo "package install success"

npm run build
echo "static build success"

sudo systemctl restart nginx
echo "nginx restart complete"

sudo systemctl status nginx