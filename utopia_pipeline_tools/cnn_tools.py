"""CNN Tools Module

Functions to work with Keras CNNs used to classify IFCB phytoplankton images. 

- preprocess_input(image): Prepares an image or a list of images to be input
  into the CNN.
- load_local_model(json_file_path, h5_file_path): Loads a Keras CNN from local 
  .json and .h5 files. 
- load_cloud_model(): NOT YET WRITTEN
- predict_labels(): NOT YET WRITTEN
"""

# imports
import numpy as np
import cv2
from tensorflow import keras

# functions

def preprocess_input(image):
    """ Takes in an IFCB .png (or list of IFCB .pngs) and resizes it to fit the 
    input dimensions of the CNN.

    :param image: IFCB .png file or list of files
    :type image: list

    :output rescaled_image: Rescaled IFCB image(s)
    :type rescaled_image: list
    """
    fixed_size = 128 # Final image should be 128 x 128
    image_size = image.shape[:2] # Gets the (y_dim, x_dim) for each image

    # The ratio needed to make the longest side of the image 128 pixels
    ratio = float(fixed_size)/max(image_size)

    # Calculates the new size by multiplying each dimension by the ratio
    new_size = tuple([int(x*ratio) for x in image_size])

    # Resizes the image to the new size
    img = cv2.resize(image, (new_size[1], new_size[0]))

    # Calculates the possible padding needed for the x and y dimensions
    delta_w = fixed_size - new_size[1]
    delta_h = fixed_size - new_size[0]
    top, bottom = delta_h//2, delta_h-(delta_h//2)
    left, right = delta_w//2, delta_w-(delta_w//2)

    # Makes a black border of 128x128 pixels around the image, so either
    # dimension less than 128 would be padded to 128
    color = [0, 0, 0] # RGB = 0,0,0 -> Black
    rescaled_image = cv2.copyMakeBorder(img, top, bottom, left, right, 
                                        cv2.BORDER_CONSTANT, value=color)
    return(rescaled_image)

def load_local_model(json_file_path, h5_file_path):
    """
    Loads locally saved model as Tensorflow keras model.

    :param json_file_path: Path to local .json model file
    :type json_file_path: str
    :param h5_file_path: Path to local .h5 model weights file
    :type h5_file_path: str

    :output loaded_model: Loaded keras model
    :type loaded_model: tf.keras.Model
    """
    # Load the model architecture from JSON file
    with open(json_file_path, 'r', encoding="utf-8") as json_file:
        loaded_model_json = json_file.read()

    loaded_model = keras.models.model_from_json(loaded_model_json)

    # Load the model weights from H5 file
    loaded_model.load_weights(h5_file_path)

    return loaded_model

def load_cloud_model():
    # write function here :) ##################################################
    pass

def predict_labels():
    # write function here; use dictionary in init? ############################
    # need to add a option to specify origin filepath if predicting labels of 
    # locally stored images. Something like:
    # Images stored locally? Enter the full filepath to where the ml folder is
    # located. 
    # string = 'C:/filepath/to/location/'
    # if string[-1] == /:
    #   pass
    # else:
    #   string.append('/')
    # png_df['filepath'] = string + png_df['filepath']
    pass