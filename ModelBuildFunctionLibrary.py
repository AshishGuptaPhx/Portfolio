import numpy             as np
import pandas            as pd
import sklearn           as sk
import statistics        as st
import pprint            as pp
import math              as mth
import seaborn           as sns
import matplotlib        as mpl
import os                as os
import matplotlib.pyplot as plt

from sklearn.datasets      import load_digits
from sklearn.datasets      import fetch_olivetti_faces

from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import PolynomialFeatures
from sklearn.preprocessing import minmax_scale

from sklearn.linear_model  import LinearRegression
from sklearn.linear_model  import LogisticRegression
from sklearn.linear_model  import LogisticRegressionCV
from sklearn.linear_model  import SGDClassifier

from sklearn.linear_model  import Ridge
from sklearn.linear_model  import Lasso

from sklearn.svm           import SVC
from sklearn.ensemble      import RandomForestClassifier
from sklearn.ensemble      import GradientBoostingClassifier

from sklearn.decomposition import PCA
from sklearn.decomposition import NMF

from sklearn.discriminant_analysis import LinearDiscriminantAnalysis    as LDA 
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis as QDA

from sklearn.model_selection import KFold 
from sklearn.model_selection import GridSearchCV 
from sklearn.model_selection import cross_validate 
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_predict

from sklearn.metrics         import mean_squared_error
from sklearn.metrics         import mean_absolute_error
from sklearn.metrics         import confusion_matrix
from sklearn.metrics         import accuracy_score
from sklearn.metrics         import recall_score
from sklearn.metrics         import precision_score
from sklearn.metrics         import roc_curve
from sklearn.metrics         import f1_score
from sklearn.metrics         import roc_auc_score
from sklearn.metrics         import classification_report

from tensorflow                    import keras
from tensorflow.keras.datasets     import cifar10
from tensorflow.keras              import Sequential

from tensorflow.keras.layers       import Dense
from tensorflow.keras.layers       import Conv2D
from tensorflow.keras.layers       import Flatten
from tensorflow.keras.layers       import MaxPooling2D

from tensorflow.keras.utils        import to_categorical

# --------------------------------------------------------------------
# Function for flattening an image dataset
# --------------------------------------------------------------------
def f_flatten_img_ds(arg_ds):
    # Getting dimensions of the array
    n = len(arg_ds.shape) 
    
    # Getting first dimension size
    x = arg_ds.shape[0]

    # Calculating second dimension size
    y = 1
    for i in range(n - 1):
        y = y * arg_ds.shape[i+1]
        
    # Flattening the argument data set into 2 dimensional value             
    ret_ds = arg_ds.reshape(x, y)  
    return ret_ds
# --------------------- END OF FUNCTION --------------------------	
        

# --------------------------------------------------------------------
# Function for unflattening an image dataset
# --------------------------------------------------------------------
def f_unflatten_img_ds(arg_ds, arg_imgsize):
    # Getting dimensions of the array
    x = arg_ds.shape[0]
    ret_ds = arg_ds.reshape(x, arg_imgsize[0], arg_imgsize[1], arg_imgsize[2])
    return ret_ds
# --------------------- END OF FUNCTION --------------------------	
	
#------------------------------------------------------------------------
# Function for generating Decision Tree based Model
#------------------------------------------------------------------------
def f_build_RF_CV(arg_X_train, arg_y_train, 
                  arg_X_valid, arg_y_valid, 
                  arg_X_test, arg_y_test,
                  arg_model_type, arg_random_state, arg_fold):

    #------------------------------------------------------------------------
    # Initiating the Random Forest Model
    #------------------------------------------------------------------------
    if arg_model_type == 'DecisionTree':
        None
    elif arg_model_type == 'RF':
        model = RandomForestClassifier(random_state = arg_random_state)
    elif arg_model_type == 'GBD':
        model = GradientBoostingClassifier()
    elif arg_model_type == 'SGD':
        model = SGDClassifier(random_state = arg_random_state)
    else:
        None

    #------------------------------------------------------------------------
    # Fitting the Decision Tree Type Model
    #------------------------------------------------------------------------
    model.fit(arg_X_train, arg_y_train)    
    
    #------------------------------------------------------------------------
    # Doing the Cross-validation on Valdation dataset
    #------------------------------------------------------------------------
    cv_val = cross_validate(model, arg_X_valid, arg_y_valid, cv = arg_fold)
   
    #------------------------------------------------------------------------
    # Doing Predictions on Test dataset
    #------------------------------------------------------------------------
    y_test_pred = cross_val_predict(model, arg_X_test, arg_y_test, 
                                    cv = arg_fold, method = "predict")

    #------------------------------------------------------------------------
    # Creating a dictionary to store model metrics
    #------------------------------------------------------------------------
    model_metrics = {}

    #------------------------------------------------------------------------
    # Collecting metrics
    #------------------------------------------------------------------------
    model_accuracy      = accuracy_score (arg_y_test, y_test_pred)
    model_recall        = recall_score   (arg_y_test, y_test_pred)
    model_precision     = precision_score(arg_y_test, y_test_pred)
    model_roc_score     = roc_curve      (arg_y_test, y_test_pred)
    model_f1_score      = f1_score       (arg_y_test, y_test_pred)
    
    #------------------------------------------------------------------------
    # Storing metrics in the dictionary
    #------------------------------------------------------------------------
    model_metrics['accuracy_score']  = model_accuracy
    model_metrics['recall_score']    = model_recall
    model_metrics['precision_score'] = model_precision
    model_metrics['roc_score']       = model_roc_score
    model_metrics['f1_score']        = model_f1_score
    
    return model_metrics
# --------------------- END OF FUNCTION --------------------------	


#--------------------------------------------------------------------------------
# Function for building PCA Components for all X training, validation and 
# test datasets
#--------------------------------------------------------------------------------
def f_build_PC_model(arg_X_train, arg_X_valid, arg_X_test, arg_exp_var):

    #Build a default PCA model
    pca_model = PCA()
    pca_model.fit_transform(arg_X_train)

    # Calculating optimal k to have x% (say) variance 
    k = 0
    total = sum(pca_model.explained_variance_)
    current_sum = 0

    while(current_sum / total < arg_exp_var):
        current_sum += pca.explained_variance_[k]
        k += 1

    ## Applying PCA with k calculated above
    pca_model2 = PCA(n_components = k, whiten = True)

    X_train_pca = pca_model2.fit_transform(arg_X_train)
    X_valid_pca = pca_model2.transform(arg_X_valid)
    X_test_pca  = pca_model2.transform(arg_X_test)
    
    return (X_train_pca, X_valid_pca, X_test_pca, k)	
# --------------------- END OF FUNCTION --------------------------	

# --------------------------------------------------------------------
# Creating a function for building a CNN based on arguments supplied
# --------------------------------------------------------------------
def f_build_ANN(arg_X_train, arg_y_train, arg_X_test, arg_y_test, 
                arg_in_layers,
                arg_out_layers, 
                arg_compile_parms,
                arg_fit_parms):
    # --------------------------------------------------------------------
    # Initializing the model
    # --------------------------------------------------------------------
    model = Sequential()
    # ----------------------------------------------------------------------------------------------
    # Neural Netwoek Architecture
    # ----------------------------------------------------------------------------------------------

    for in_layer in arg_in_layers:    
        model.add(Dense(units = in_layer[0], activation = in_layer[1], input_shape = in_layer[2])) 

    model.add(Flatten())  
    # --------------------------------------------------------------------
    # Adding the output layer
    # --------------------------------------------------------------------
    for out_layer in arg_out_layers:    
        model.add(Dense(out_layer[0], activation = out_layer[1]))    

    # ----------------------------------------------------------------------------------------------
    # Generating Model Summary
    # ----------------------------------------------------------------------------------------------
    model.summary()

    # ----------------------------------------------------------------------------------------------
    # Neural Netwoek Model Compilation
    # ----------------------------------------------------------------------------------------------
    model.compile(optimizer = arg_compile_parms[0][0], 
                  loss      = arg_compile_parms[0][1], 
                  metrics   = arg_compile_parms[0][2])
    # --------------------------------------------------------------------
    # Fitting the Model
    # --------------------------------------------------------------------
    model_history = model.fit(arg_X_train, 
                              arg_y_train, 
                              epochs          = arg_fit_parms[0][0], 
                              batch_size      = arg_fit_parms[0][1], 
                              validation_data = (arg_X_test, arg_y_test))

    # --------------------------------------------------------------------
    # Printing the model Accuracy
    # --------------------------------------------------------------------
    model_accuracy = model.evaluate(arg_X_test, arg_y_test, verbose = 0)[1]
    print('Model Accuracy is ', model_accuracy)

    return model
    # --------------------- END OF FUNCTION --------------------------	



# --------------------------------------------------------------------
# Creating a function for building a CNN based on arguments supplied
# --------------------------------------------------------------------
def f_build_CNN(arg_X_train, arg_y_train, arg_X_test, arg_y_test, 
                arg_conv_layers,
                arg_pool_layers, 
                arg_out_layers, 
                arg_compile_parms,
                arg_fit_parms):
    # --------------------------------------------------------------------
    # Initializing the model
    # --------------------------------------------------------------------
    model = Sequential()
    
    # --------------------------------------------------------------------
    # Adding Convolution and Pool Layers alternatively
    # --------------------------------------------------------------------
    conv_layers = len(arg_conv_layers)
    pool_layers = len(arg_pool_layers)
    all_layers = conv_layers + pool_layers

    conv_used = 0
    pool_used = 0
    prev_layer = None
    curr_layer = None

    for i in range(all_layers):
        if i == 0 and prev_layer is None:
            if conv_layers > 0:
                curr_layer = 'CONV' 
                conv_used = conv_used + 1
            else:    
                curr_layer = 'POOL' 
                pool_used = pool_used + 1
        elif prev_layer == 'POOL':
            if conv_used < conv_layers:
                curr_layer = 'CONV'
                conv_used = conv_used + 1
            else:
                curr_layer = 'POOL'
                pool_used = pool_used + 1
        elif prev_layer == 'CONV':
            if pool_used < pool_layers:
                curr_layer = 'POOL'
                pool_used = pool_used + 1
            else:
                curr_layer = 'CONV'
                conv_used = conv_used + 1
        else:
            None

        if curr_layer == 'CONV':
            conv_layer = arg_conv_layers[conv_used - 1]
            model.add(Conv2D(conv_layer[0], 
                             kernel_size = conv_layer[1], 
                             strides     = conv_layer[2], 
                             activation  = conv_layer[3], 
                             padding     = conv_layer[4], 
                             input_shape = conv_layer[5]))

            print('Current CONV Item : ', arg_conv_layers[conv_used - 1])
        else:
            pool_layer = arg_pool_layers[pool_used - 1]
            model.add(MaxPooling2D(pool_size = pool_layer[0], 
                                   strides   = pool_layer[1]))
            print('Current POOL Item : ', arg_pool_layers[pool_used - 1])

        prev_layer = curr_layer
        curr_layer = None
    
    # --------------------------------------------------------------------
    # Flattening the image data 
    # --------------------------------------------------------------------
    model.add(Flatten())

    # --------------------------------------------------------------------
    # Adding the output layer
    # --------------------------------------------------------------------
    for out_layer in arg_out_layers:    
        model.add(Dense(out_layer[0], activation = out_layer[1]))
    
    # --------------------------------------------------------------------
    # Printing Model Structure
    # --------------------------------------------------------------------
    print(model.summary())

    # --------------------------------------------------------------------
    # Compiling the Model
    # --------------------------------------------------------------------
    model.compile(optimizer = arg_compile_parms[0][0], 
                  loss      = arg_compile_parms[0][1], 
                  metrics   = arg_compile_parms[0][2])

    # --------------------------------------------------------------------
    # Fitting the Model
    # --------------------------------------------------------------------
    model_history = model.fit(arg_X_train, 
                              arg_y_train, 
                              epochs          = arg_fit_parms[0][0], 
                              batch_size      = arg_fit_parms[0][1], 
                              validation_data = (arg_X_test, arg_y_test))

    # --------------------------------------------------------------------
    # Printing the model metrics
    # --------------------------------------------------------------------
    model_accuracy = model.evaluate(arg_X_test, arg_y_test, verbose = 0)[1]
    print('Model Accuracy is ', model_accuracy)
    
    return(model)
    # --------------------- END OF FUNCTION --------------------------	

def f_CNN_feature_maps(arg_model, arg_X_train, arg_y_train, arg_X_test, arg_y_test, arg_img_id):
    """ This plots the features maps from the output of either a 
        convolutional or pooling layer with 16 feature maps (neurons).
        
        output_predictions should be a 4 dimensional array of predictions from a layer. 
        The first dimension should be 1 (it's just a single image).
        The last dimension is for the indices of the feature maps (0-15). 
    """
    # Create list of layer outputs
    layer_outputs = [layer.output for layer in arg_model.layers] 
    print(layer_outputs)

    # Create a model that will return the outputs at each layer
    layers_model = keras.Model(inputs = arg_model.input, outputs = layer_outputs) 
    print(layers_model)
    
    # Get predictions for each layer of the network
    outputs = layers_model.predict(arg_X_test[arg_img_id].reshape(1,64,64,1)) 
    conv_output = outputs[0]
    pooling_output = outputs[1]    
    
    n_col = 4
    n_row = 4 
    plt.figure(figsize=(n_col, n_row))
    
    for j in range(n_row * n_col):
        plt.subplot(n_row, n_col, j + 1)
        plt.imshow(conv_output[0, :, :, j], cmap=plt.cm.binary)
        plt.xticks(())
        plt.yticks(())
    plt.show

    plt.figure(figsize=(n_col, n_row))
    for j in range(n_row * n_col):
        plt.subplot(n_row, n_col, j + 1)
        plt.imshow(pooling_output[0, :, :, j], cmap=plt.cm.binary)
        plt.xticks(())
        plt.yticks(())
    plt.show
    return None
    # --------------------- END OF FUNCTION --------------------------	
    
#--------------------------------------------------------------------------------
# End Of Library Load Process
#--------------------------------------------------------------------------------
print('Library COMMON loaded.')
