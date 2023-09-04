## Master's thesis

---

The repository contains the master's project which main goal is to create a DNN model which classifies Twitter's user account whether it is a bot or a human user account.

## Structure of the repository

```
├── classifiers/ -------------------------------- # Prepared Jupyter Notebooks to test trained best models for specific group of data
├── data-analysis/ ------------------------------ # Copy of notebooks run in JypyterLab to process, analyse data and create DNN models
│   └── models/
│   └── users_and_tweets_data_based/
│   └── users_data_based/
│   └── README.md
├── data-preprocessing/ ------------------------- # Scripts used for preprocessing large dataset on GCP VMs
│   └── schema/
│   └── scripts/
│       └── tweets/
│       └── users/
│   └── common_preprocessing_utils.py/
│   └── requirements.txt/
├── data/ --------------------------------------- # Orginal dataset
├── .gitignore.md
├── README.md
└── gcp_env.py ---------------------------------- # Env variable for GCP project
```

## Test trained models on own data or examples

To test one of the best trained models for a specific group of data (e.g. based on user data) go to [classifiers/](classifiers/) where are stored notebooks prepared for testing trained models.<br>
For each one, there is an online version stored in Google Disk to be run on Google Collab directly.
