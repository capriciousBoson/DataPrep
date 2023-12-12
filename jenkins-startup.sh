#!/bin/sh
ls 
docker build -t backend .
docker run -p 8000:8000 backend