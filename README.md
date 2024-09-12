# NDP Pre-Built JupyterHub Images


### Minimal NDP Starter Jupyter Lab
Minimal image with no additional content provided.

### NDP Catalog Search
Minimal image with a notebook describing how to search programatically in the NDP catalog.

### Physics Guided Machine Learning Starter Code
Next-generation fire models provide the basis to understand fire physics in detail, leading the way for emulators to model potential fire behavior. The Physics Guided Machine Learning (PGML) project uses data from hundreds of coupled fire-atmosphere simulations produced by a physics-based coupled fire-atmospheric modeling model (QUIC-Fire) to develop a reduced-order emulator. Such an emulator can be used to predict the wildfire spread, which can help the fire agencies take necessary steps to reduce the damage. Additionally, the predictions can be utilized to mitigate the risk of controlled fires escalating into wildfires.

This dataset was generated particularly for the Physics Guided Machine Learning (PGML) research and educational tasks. It is an ensemble of prescribed fire simulations generated by the QUIC-Fire coupled fire-atmospheric modeling tool. Each simulation run is represented by a Zarr file, containing the outputs created by QUIC-Fire through the BurnPro3D web interface. To model a burn, users upload the polygon for their burn unit and BurnPro3D uses a 3D fuels model for that location created by FastFuels and an ignition file with a user-defined ignition pattern created in DripTorch. Those files are included here as well. In addition, users define the environmental conditions they would like to model in terms of fuel moisture, wind direction, and wind speed.

### SAGE Pilot Streaming Data Starter Code

Streaming Data from SAGE Pilot


### EarthScope Consortium Streaming Data Starter Code

The EarthScope Consortium (www.earthscope.org) streams three-dimensional Global Navigation Satellite System (GNSS) high rate (1hz) position time series from nearly a thousand EarthScope and related GNSS stations. These high precision ground-motion time series are used to study a range of geophysical phenomena including earthquakes, volcanos, tsunamis, hydrologic loads, and glaciers. EarthScope is dedicated to supporting transformative global geophysical research and education through operation of the National Science Foundation’s (NSF) Geodetic GAGE and Seismic SAGE facilities. As part of the National Data Platform (NDP) EarthScope pilot project, the EarthScope GNSS position time series streams are being stored and made available from Data Collaboratory Kafka servers at the University of Utah. This Jupyter Notebook provides tools for access and plotting of sample real time streams and is the foundation for additional services being developed that will facilitate time series analysis including machine learning.

### NAIRR Pilot - NASA Harmonized Landsat Sentinel-2 (HLS) Starter Code

The Harmonized Landsat and Sentinel-2 (HLS) project is a NASA initiative aiming to produce a seamless surface reflectance record from the Operational Land Imager (OLI) and Multi-Spectral Instrument (MSI) aboard Landsat-8/9 and Sentinel-2A/B remote sensing satellites, respectively.

As part of collection of the collection of resources of the NAIRR Pilot, NASA in partnership with IBM has developed Prithvi-100M, a temporal Vision transformer pre-trained on contiguous HLS data There are 3 examples of finetuning the model for image segmentation using the mmsegmentation library available through HuggingFace: burn scars segmentation, flood mapping, and multi temporal crop classification, with the code used for the experiments available on GitHub.

### LLM Training

An LLM, or Large Language Model, is a type of artificial intelligence designed to understand and generate human-like text based on the data it’s been trained on. By adding a vast amount of text from different sources and context’s (web, books, papers, among others) LLM’s are capable to identify the patterns of the human language under different contexts and provide responses to complex questions.

LLM’s posses a huge potential in both research and education, given their capability to quickly process and summarize a vast amount of information. LLM’s can bee seen as a powerful tool to accelerate learning, facilitate the sharing of knowledge, process and generate big amounts of data, generate new hypothesis questions, among other uses.

This image can be used to serve and train LLM with FastChat.


### LLM Service Client
This image can be used to query existing LLM deployment.

### TLS Fuel-Size Segmentation 2023
The increasing potential for catastrophic wildfires due to climate change and overstocked forests has resulted in increased loss of life, property damage, and ecological damage, particularly in the western U.S. One potential solution is to implement fuel treatments to manage overstocked forests and reduce the potential for wildfire and its severity. Choosing the optimal fuel treatment strategy is critical, and new 3D fire modeling tools are available to help determine the best strategy. Simply put, we need to know the vegetation characteristics within the fire environment to understand what contributes to increased wildfire risk and what needs to be removed.  

In addition, the development of these tools requires access to high-quality data, computational resources, and AI workflows that are essential for generating new knowledge and refining strategies.  

Given that labeled vegetation fuels are a crucial input to fire models, this demo project aims to model the classification of vegetation fuels by effectively segmenting them by category (live and dead) and size class.

### NOAA-GOES Analysis

The Geostationary Operational Environmental Satellites (GOES) operated by NOAA are essential for continuous observation of atmospheric conditions, severe weather events, and environmental changes. The GOES satellites provide high-resolution imagery and atmospheric measurements, which are crucial for accurate weather forecasting, climate monitoring, and disaster management. Their geostationary position allows them to monitor the same area continuously, providing real-time data that is vital for tracking the development and movement of storms, hurricanes, and other weather phenomena.

This project focuses on harnessing the real-time data streaming and analysis capabilities of the NOAA GOES satellites. By leveraging advanced data processing techniques and state-of-the-art analytical tools, the project aims to enhance our understanding of weather patterns and environmental changes. This initiative not only supports scientific research but also contributes to public safety by improving the accuracy and timeliness of weather predictions and alerts. The collaboration with the Scientific Computing and Imaging Institute at the University of Utah ensures the integration of cutting-edge computational methods to optimize the use of GOES data for various applications.
