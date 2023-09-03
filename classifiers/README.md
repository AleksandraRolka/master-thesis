<!-- # Classification model based console app -->

# Jupyter notebooks for _bot / human_ Twitter user account classification using trained models

To test models on examples or own data Jupyter Notebooks were prepared.

Using Google Collab instead of a local environment users do not need to bother about setup, only a Google account is required.

Prepared notebooks to classify a Twitter user account, whether it is a **bot** or a **human** user account use prepared and trained deep neural network models.

> NOTE: Due to changes in access to the Twitter API, the script does not allow for automatic data download (for example by user id/username). Free API is no longer available. The data must be prepared in advance in JSON file. Or just to test the model prepared example data can be used.

---

---

<br>

Currently, only models based on only Twitter user data reach> 0.70 accuracy, that's why for testing only one model from that group is available:

- [Twitter user's data based model](twitter_user_data_based_model/)
  - Google colab notebook available online [HERE](https://drive.google.com/file/d/1eFb4RME37gZZRIZhXusohbCp_zMS2YyF/view?usp=sharing)