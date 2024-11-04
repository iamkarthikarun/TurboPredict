# Aircraft Engine RUL Prediction System

A machine learning system that predicts Remaining Useful Life (RUL) of aircraft turbofan engines using NASA's Turbofan Engine Degradation Simulation Dataset and proprietary data models.

## Project Overview

This project implements multiple machine learning models (PyTorch, XGBoost, Random Forest) to predict the remaining useful life of aircraft engines. The system includes:
- Real-time prediction capabilities
- AWS TimeStream integration for time-series data storage
- Web dashboard for visualization
- Multiple trained models for different engine conditions

## Tech Stack

- **Machine Learning**: PyTorch, XGBoost, Scikit-learn
- **Backend**: Python, Flask
- **Data Storage**: AWS TimeStream
- **Data Processing**: Pandas, NumPy
- **Visualization**: Flask-based dashboard

## Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/engine-rul-prediction.git
cd engine-rul-prediction
