# Development Documentation

## Environment Setup

### AzCopy

_Notes:_
- Windows 11 system
- AzCopy v10.26.0

_Process:_
1. Follow the download and setup instructions [here](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10?tabs=dnf). This should involve (for Windows) downloading the program and extracting the zipped files.
2. Add the executable file location to the system path to be able to access AzCopy from any folder. Do this by navigating to 'Environment Variables' in settings then selecting the 'Path' variable to edit. Add a new line and paste the path to the folder that contains the program file (azcopy.exe). Do not include the program name in the path. Here is a resource for adding something to the Windows PATH environment variables (https://helpdeskgeek.com/windows-10/add-windows-path-environment-variable/). 
3. You should now be able to call azcopy in the command prompt from any directory. Test this by typing `azcopy`.

## Environment and Module Issues

### ModuleNotFoundError/ImportError for keras

_Notes:_ 
- Using the miniconda3 prompt
- Tensorflow version 2.17.0
- Keras version 3.5.0
- Python version 3.10.14

_Process:_  
After I installed the utopia_pipeline_tools module locally with pip, my notebooks were unable to find the keras library, so `from tensorflow import keras` gave me an error. I tried uninstalling tensorflow then re-installing it with pip (`pip uninstall tensorflow` and `pip install tensorflow`). That didn't solve the issue, so next I tried installing tensorflow with conda (`conda install -c conda-forge tensorflow`). That alone didn't work, and I thought maybe the pip install and the conda install were interfering with each other, so I uninstalled tensorflow with pip then re-ran the conda install command. Once again, this was unsuccessful. I uninstalled tensorflow with `conda uninstall tensorflow` and tried a new method. This time, I checked my python version (`python --version`), which was 3.10.14 and tried `pip3.10 install tensorflow`. Unfortunately, this also did not work.

I did a bit of investigation in my site_packages folder and found the tensorflow folder. This folder did not contain the keras module for some reason. I thought it might be an issue with the new version of tensorflow, so I once again uninstalled tensorflow with pip and re-installed it with the previous version (`pip install tensorflow==2.16.2`) which finally worked!

### AttributeError('module 'matlab' has no attribute 'engine'')

_Notes:_
- miniconda3 prompt
- Marimo version 0.8.3
- Matlab version 0.1
- Python version 3.10.14

_Process:_
In my `process_raw_ifcb.py` notebook, I was getting this attribute error. I found this (https://www.mathworks.com/matlabcentral/answers/362824-no-module-named-matlab-engine-matlab-is-not-a-package) thread on mathworks so I followed the advice of one comment that said to install the matlabengine package with `pip install matlabengine`. I successfully installed matlabengine version 24.2.1 and re-tried my notebook. I can now import `matlab.engine` in my notebook!

