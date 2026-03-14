#!/bin/zsh

ln -s ../app/.env .env

docker-compose -f docker-compose.dev.yml down && docker-compose -f docker-compose.dev.yml up --build