# SECP3133 Assignment 2 — Mastering Big Data Handling

## Group Information

| Field | Details |
|---|---|
| **Group Name** | Byte |
| **Member 1** | *NURAISYAH BINTI MOHD ZIKRE - A23CS0160* |
| **Member 2** | *NUR FIRZANA BINTI BADRUS HISHAM - A23CS0156* |
| **Course** | SECP3133 High Performance Data Processing |
| **Colab Notebook** | [big_data.ipynb](https://colab.research.google.com/drive/1S1jJ90-6aZbTFaGhTaAVDVe2teSbpHNp?usp=sharing) |

---

## Dataset

**Name:** Airline Delay and Cancellation Data, 2009–2018 (2018.csv)  
**Source:** [Airline Delay and Cancellation Data](https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018)  
**Size:** ~700 MB | ~7.2 million rows | 28 columns  
**Domain:** Aviation / Transportation

---

## Libraries Used

| # | Library | Purpose |
|---|---|---|
| 1 | Pandas | Baseline comparison |
| 2 | Dask | Parallel processing (multi-core) |
| 3 | Polars | High-performance Rust engine |

---

## Files in This Folder

| File | Description |
|---|---|
| [`big_data.md`](.Group_Byte/big_data.md) | Main Markdown report covering all 5 tasks and comparative analysis |
| [`big_data.ipynb`](.Group_Byte/big_data.ipynb) | Annotated Google Colab notebook with all working code and outputs |
| [`readme.md`](.Group_Byte/readme.md) | This file, where group introduction and navigation guide |

---

## Strategy Used

| # | Strategy Name | 
|---|---|
| 1 | Less Load Data | 
| 2 | Chunking | 
| 3 | Data Type Optimization |
| 4 | Sampling |
| 5 | Parallel Processing with Scalable Libraries |

---

## Libraries Used

 - Dask 
 - Polars 
 - Pandas 

---


## How to Run the Notebook

1. Upload `2018.csv` to your Google Drive under `MyDrive/`.
2. Open `big_data.ipynb` in [Google Colab](https://colab.research.google.com/).
3. Run **Cell 0** (`Environment Setup`) first to install Dask and Polars.
4. Run **Mount Drive** cell and authorise access.
5. Run all remaining cells from top to bottom in order.

> All cells should execute without errors. Re-run the full notebook before submission to confirm all outputs are visible.

