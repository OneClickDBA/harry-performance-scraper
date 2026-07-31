#!/bin/bash
NOCACHE=""
if [[ "$1" = "-F" ]] ; then
    NOCACHE="--no-cache"
fi
echo "## rebuild"
docker compose --env-file .env -f docker-compose/compose.yaml build ${NOCACHE} harry-scraper
echo "## restarting"
docker compose --env-file .env -f docker-compose/compose.yaml up -d --no-deps --force-recreate harry-scraper
echo "## Following logs"
docker compose --env-file .env -f docker-compose/compose.yaml logs -f harry-scraper
