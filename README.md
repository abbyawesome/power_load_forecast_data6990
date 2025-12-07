# Overview
This is the code for HW3 and the final for DATA6990 in the MTSU master's program.

# Datasets
Data is from 1/1/2023 to 12/31/2024 in the TVA region. 

## Power Data
Data is from the EIA (https://www.eia.gov/opendata/) and pulled using the API

## Weather Data
Weather data is from Open-Meteo (https://open-meteo.com/en/docs) and from Nashville, TN, Memphis, TN, and Knoxville, TN


# How to Run
1. Run get_weather_data.py and read_energy_api.py. Make sure for read_energy_api.py that you have a correct API key (see example_api_key.txt for format)
2. Run combine_data.py to combine datasets
3. Run visualizations.ipynb to see prelimiary visualizations
4. Run xgboost_regression.ipynb to recreate the XGBoost model
5. Run regression_power_model.ipynb to recreate the neural network
