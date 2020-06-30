#!/bin/sh

kaggle datasets download mkechinov/ecommerce-behavior-data-from-multi-category-store -f 2019-Oct.csv
kaggle datasets download mkechinov/ecommerce-behavior-data-from-multi-category-store -f 2019-Nov.csv
unzip *.zip
gdown https://drive.google.com/uc?id=1qZIwMbMgMmgDC5EoMdJ8aI9lQPsWA3-P
gdown https://drive.google.com/uc?id=1x5ohrrZNhWQN4Q-zww0RmXOwctKHH9PT
gdown https://drive.google.com/uc?id=1-Rov9fFtGJqb7_ePc6qH-Rhzxn0cIcKB
gdown https://drive.google.com/uc?id=1zr_RXpGvOWN2PrWI6itWL8HnRsCpyqz8
gdown https://drive.google.com/uc?id=1g5WoIgLe05UMdREbxAjh0bEFgVCjA1UL
gunzip *.gz
gsutil mv * gs://$1/data/
