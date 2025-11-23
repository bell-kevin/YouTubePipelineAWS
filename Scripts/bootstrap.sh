#!/bin/bash

# Update package lists
sudo yum update -y

# Install Python 3 and pip (if not already present or to ensure latest versions)
sudo yum install -y python3 python3-pip

# Install pandas and numpy using pip3
sudo pip3 install pandas numpy plotly ipywidgets nbformat