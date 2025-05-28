# 🏥 Big Data Healthcare Pipeline - MIMIC-III Dataset

![Header](https://capsule-render.vercel.app/api?type=waving&color=0:00c651,100:0072ff&height=280&section=header&text=MIMIC-III%20Healthcare%20Analytics&fontSize=60&fontColor=ffffff&animation=fadeIn&fontAlign=center&fontAlignY=40&desc=Hadoop%20•%20Hive%20•%20MapReduce%20•%20Docker%20&descSize=22&descAlign=middle&descAlignY=65)

[![Platform](https://img.shields.io/badge/Platform-Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Storage](https://img.shields.io/badge/Storage-HDFS-66ccff?logo=apachehadoop&logoColor=white)](https://hadoop.apache.org/)
[![Batch](https://img.shields.io/badge/Batch-Hive-FDEE21?logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![Dataset](https://img.shields.io/badge/Dataset-MIMIC--III-7c4dff)](https://mimic.mit.edu/)
[![Compute](https://img.shields.io/badge/Compute-MapReduce-ED8B00?logo=apache&logoColor=white)](https://hadoop.apache.org/)
[![Format](https://img.shields.io/badge/Format-Parquet-50C878?logo=apacheparquet&logoColor=white)](https://parquet.apache.org/)

> **A scalable big data pipeline for healthcare analytics using MIMIC-III clinical data with Hadoop ecosystem components**

---

## 🎯 Purpose
This project implements an end-to-end **big data pipeline** for processing and analyzing the MIMIC-III Clinical Database.The pipeline involves data cleaning using **Pandas**, conversion to **Parquet** format, storage in a containerized **HDFS** environment, and analysis using Apache **Hive**.

---

## 🎯 Objectives
1. Implement distributed storage of MIMIC-III data using HDFS
2. Perform batch analytics with Hive for clinical insights
3. Demonstrate parallel processing with MapReduce
4. Containerize the big data environment using Docker
5. Generate healthcare analytics on:
   - Patient length of stay prediction
   - ICU readmission risk analysis
   - Demographic-based mortality rates
   - Diagnosis-related treatment patterns




---

## ⚙️ Key Components & Technologies
| Component | Technology | Purpose |
|-----------|-------------|---------|
| **Distributed Storage** | Hadoop HDFS | Scalable data storage |
| **Batch Processing** | Apache Hive | SQL-based analytics |
| **Data Processing** | MapReduce | Parallel computation |
| **Containerization** | Docker | Environment packaging |
| **Data Format** | Parquet | Columnar storage optimization |
| **Version Control** | Git/GitHub | Code management |

---

## 🏗️ System Architecture
<div align="center">
  <img src="DOC/Architechture.png" alt="Big Data Pipeline Architecture" width="100%">
  <p><em>Complete data pipeline from MIMIC-III dataset to healthcare insights</em></p>
</div>

