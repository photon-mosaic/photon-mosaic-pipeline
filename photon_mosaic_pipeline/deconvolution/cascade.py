import math
import os
import time
import numpy as np
import warnings
from . import config, utils


def train_model(
    model_name, model_folder="Pretrained_models", ground_truth_folder="Ground_truth"
):

    """Train neural network with parameters specified in the config.yaml file in the model folder

    In this function, a model is configured (defined in the input 'model_name': frame rate, noise levels, ground truth datasets, etc.).
    The ground truth is resampled (function 'preprocess_groundtruth_artificial_noise_balanced', defined in "utils.py").
    The network architecture is defined (function 'define_model', defined in "utils.py").
    The thereby defined model is trained with the resampled ground truth data.
    The trained model with its weight and configuration details is saved to disk.

    Parameters
    ----------
    model_name : str
        Name of the model, e.g. 'Universal_30Hz_smoothing100ms'
        This name has to correspond to the folder with the config.yaml file which defines the model parameters

    model_folder: str
        Absolute or relative path, which defines the location of the specified model_name folder
        Default value 'Pretrained_models' assumes a current working directory in the Cascade folder

    ground_truth_folder : str
        Absolute or relative path, which defines the location of the ground truth datasets
        Default value 'Ground_truth'  assumes a current working directory in the Cascade folder

    Returns
    --------
    None
        All results are saved in the folder model_name as .h5 files containing the trained model

    """
    import torch
    import torch.nn as nn
    import torch.optim as optim

    model_path = os.path.join(model_folder, model_name)
    cfg_file = os.path.join(model_path, "config.yaml")

    # check if configuration file can be found
    if not os.path.isfile(cfg_file):
        m = (
            'The configuration file "config.yaml" can not be found at the location "{}".\n'.format(
                os.path.abspath(cfg_file)
            )
            + 'You have provided the model "{}" at the absolute or relative path "{}".\n'.format(
                model_name, model_folder
            )
            + 'Please check if there is a folder for model "{}" at the location "{}".'.format(
                model_name, os.path.abspath(model_folder)
            )
        )
        print(m)
        raise Exception(m)

    # load cfg dictionary from config.yaml file
    cfg = config.read_config(cfg_file)
    verbose = cfg["verbose"]

    if verbose:
        print(
            "Used configuration for model fitting (file {}):\n".format(
                os.path.abspath(cfg_file)
            )
        )
        for key in cfg:
            print("{}:\t{}".format(key, cfg[key]))

        print("\n\nModels will be saved into this folder:", os.path.abspath(model_path))

    # add base folder to selected training datasets
    training_folders = [
        os.path.join(ground_truth_folder, ds) for ds in cfg["training_datasets"]
    ]

    # check if the training datasets can be found
    missing = False
    for folder in training_folders:
        if not os.path.isdir(folder):
            print(
                'The folder "{}" could not be found at the specified location "{}"'.format(
                    folder, os.path.abspath(folder)
                )
            )
            missing = True
    if missing:
        m = (
            'At least one training dataset could not be located.\nThis could mean that the given path "{}" '.format(
                ground_truth_folder
            )
            + "does not specify the correct location or that e.g. a training dataset referenced in the config.yaml file "
            + "contained a typo."
        )
        print(m)
        raise Exception(m)

    start = time.time()
    # Update model fitting status
    cfg["training_finished"] = "Running"
    config.write_config(cfg, os.path.join(model_path, "config.yaml"))

    nr_model_fits = len(cfg["noise_levels"]) * cfg["ensemble_size"]
    print("Fitting a total of {} models:".format(nr_model_fits))
  
    curr_model_nr = 0

    print(training_folders[0])

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    for noise_level in cfg["noise_levels"]:
        for ensemble in range(cfg["ensemble_size"]):
            # train 'ensemble_size' (e.g. 5) models for each noise level

            curr_model_nr += 1
            print(
                "\nFitting model {} with noise level {} (total {} out of {}).".format(
                    ensemble + 1, noise_level, curr_model_nr, nr_model_fits
                )
            )

            if cfg["sampling_rate"] > 30:

                windowsize_suggestion = int(np.power(cfg["sampling_rate"] / 30, 0.25) * 64)

                print(
                    "Window size should be enlarged to "
                    + str(windowsize_suggestion)
                    + " time points (if not done already) due to the high calcium imaging sampling rate ("
                    + str(cfg["sampling_rate"])
                    + ")."
                )

            # preprocess dataset to get uniform dataset for training
            X, Y = utils.preprocess_groundtruth_artificial_noise_balanced(
                ground_truth_folders=training_folders,
                before_frac=cfg["before_frac"],
                windowsize=cfg["windowsize"],
                after_frac=1 - cfg["before_frac"],
                noise_level=noise_level,
                sampling_rate=cfg["sampling_rate"],
                smoothing=cfg["smoothing"] * cfg["sampling_rate"],
                omission_list=[],
                permute=1,
                verbose=cfg["verbose"],
                replicas=1,
                causal_kernel=cfg["causal_kernel"],
            )

            model = utils.define_model(
                filter_sizes=cfg["filter_sizes"],
                filter_numbers=cfg["filter_numbers"],
                dense_expansion=cfg["dense_expansion"],
                windowsize=cfg["windowsize"],
                loss_function=cfg["loss_function"],
                optimizer=cfg["optimizer"],
            )

            model = model.to(device)

            optimizer = optim.Adagrad(model.parameters(), lr=0.05)
            
            loss_fn = nn.MSELoss()

            cfg["nr_of_epochs"] = np.minimum(
                cfg["nr_of_epochs"], int(10 * np.floor(5e6 / len(X)))
            )

            X_tensor = torch.FloatTensor(X).to(device)
            Y_tensor = torch.FloatTensor(Y).to(device)
            
            dataset = torch.utils.data.TensorDataset(X_tensor, Y_tensor)
            dataloader = torch.utils.data.DataLoader(dataset, batch_size=cfg["batch_size"], shuffle=True)

            model.train()
            for epoch in range(cfg["nr_of_epochs"]):
                epoch_loss = 0.0
                for batch_X, batch_Y in dataloader:
                    optimizer.zero_grad()
                    outputs = model(batch_X)
                    loss = loss_fn(outputs, batch_Y)
                    loss.backward()
                    optimizer.step()
                    epoch_loss += loss.item()
                
                if cfg["verbose"]:
                    print(f"Epoch {epoch+1}/{cfg['nr_of_epochs']}, Loss: {epoch_loss/len(dataloader):.4f}")

            # save model
            file_name = "Model_NoiseLevel_{}_Ensemble_{}.pth".format(
                int(noise_level), ensemble
            )
            torch.save(model.state_dict(), os.path.join(model_path, file_name))
            print("Saved model:", file_name)

    # Update model fitting status
    # cfg['training_finished'] = 'Yes'
    # config.write_config(cfg, os.path.join( model_path, 'config.yaml' ))

    print("\n\nDone!")
    print("Runtime: {:.0f} min".format((time.time() - start) / 60))


def predict(
    model_name, traces, model_folder="Pretrained_models", threshold=0, padding=np.nan, trace_noise_levels=None, verbosity=1, device=None
):

    """Use a specific trained neural network ('model_name') to predict spiking activity for calcium traces ('traces')

    In this function, a already trained model (generated by 'train_model' or downloaded) is loaded.
    The model (frame rate, noise levels, ground truth datasets) should be chosen
      to match the properties of the calcium recordings in 'traces'.
    An ensemble of 5 models is loaded for each noise level.
    These models are used to predict spiking activity of neurons from 'traces' with the same noise levels.
    The predictions are made with PyTorch inference.
    The predictions are returned as a matrix 'Y_predict'.

    Inference is performed in a streaming fashion: rather than materializing the
    full (neurons x timepoints x window_size) tensor up front, the sliding windows
    are built one chunk at a time on the target device. Peak memory is therefore
    set by the chunk size (see 'utils.estimate_inference_cell_chunk_size') and does
    not grow with the length of the recording. The predicted values are unaffected.


    Parameters
    ------------
    model_name : str
        Name of the model, e.g. 'Universal_30Hz_smoothing100ms'
        This name has to correspond to the folder in which the config.yaml and .pth files are stored which define
        the trained model

    traces : 2d numpy array (neurons x nr_timepoints)
        Df/f traces with recorded fluorescence (as fractions, not in percents) on which the spiking activity will
        be predicted. Required shape: (neurons x nr_timepoints)

    model_folder: str
        Absolute or relative path, which defines the location of the specified model_name folder
        Default value 'Pretrained_models' assumes a current working directory in the Cascade folder

    threshold : int or boolean
        Allowed values: 0, 1 or False
            0: All negative values are set to 0
            1 or True: Threshold signal to set every signal which is smaller than the expected signal size
                       of an action potential to zero (with dilated mask)
            False: No thresholding. The result can contain negative values as well

    padding : 0 or np.nan
        Value which is inserted for datapoints, where no prediction can be made (because of window around timepoint of prediction)
        Default value: np.nan, another recommended value would be 0 which circumvents some problems with following analysis.

    trace_noise_levels: float (neurons x 1)
        Noise levels of the traces. If not provided, the noise levels are calculated from the traces.

    verbosity : 0 or 1
        If set to 0, the output of predict() during inference in the console is suppressed
        Default value: 1

    device : torch.device or None
        Device to run inference on (cuda or cpu). If None, automatically selects cuda if available.

    Returns
    --------
    predicted_activity: 2d numpy array (neurons x nr_timepoints), float32
        Spiking activity as predicted by the model. The shape is the same as 'traces'
        This array can contain NaNs if the value 'padding' was np.nan as input argument

    """
    import torch

    # Set device
    if device is None:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    else:
        device = torch.device(device)

    # reshape matrix of traces if only a single neuron's activity is provided as input to the inference
    if len(traces.shape) == 1:
        traces = np.expand_dims(traces,0)

    traces = np.ascontiguousarray(traces, dtype=np.float32)

  
    model_path = os.path.join(model_folder, model_name)
    cfg_file = os.path.join(model_path, "config.yaml")

    # check if configuration file can be found
    if not os.path.isfile(cfg_file):
        m = (
            'The configuration file "config.yaml" can not be found at the location "{}".\n'.format(
                os.path.abspath(cfg_file)
            )
            + 'You have provided the model "{}" at the absolute or relative path "{}".\n'.format(
                model_name, model_folder
            )
            + 'Please check if there is a folder for model "{}" at the location "{}".'.format(
                model_name, os.path.abspath(model_folder)
            )
        )
        print(m)
        raise Exception(m)

    # Load config file
    cfg = config.read_config(cfg_file)

    # extract values from config file into variables
    verbose = cfg["verbose"]
    if verbosity == 0:
      verbose = 0
    training_data = cfg["training_datasets"]
    ensemble_size = cfg["ensemble_size"]
    batch_size = cfg["batch_size"]
    sampling_rate = cfg["sampling_rate"]
    before_frac = cfg["before_frac"]
    window_size = cfg["windowsize"]
    noise_levels_model = cfg["noise_levels"]
    smoothing = cfg["smoothing"]
    causal_kernel = cfg["causal_kernel"]

    model_description = (
        "\n \nThe selected model was trained on "
        + str(len(training_data))
        + " datasets, with "
        + str(ensemble_size)
        + " ensembles for each noise level, at a sampling rate of "
        + str(sampling_rate)
        + "Hz,"
    )
    if causal_kernel:
        model_description += (
            " with a resampled ground truth that was smoothed with a causal kernel"
        )
    else:
        model_description += (
            " with a resampled ground truth that was smoothed with a Gaussian kernel"
        )
    model_description += (
        " of a standard deviation of "
        + str(int(1000 * smoothing))
        + " milliseconds. \n \n"
    )
    if verbose:
        print(model_description)

    if verbose:
        print("Loaded model was trained at frame rate {} Hz".format(sampling_rate))
    if verbose:
        print(
            "Given argument traces contains {} neurons and {} frames.".format(
                traces.shape[0], traces.shape[1]
            )
        )

    if trace_noise_levels is None:
        # calculate noise levels for each trace
        trace_noise_levels = utils.calculate_noise_levels(traces, sampling_rate)
    trace_noise_levels = np.asarray(trace_noise_levels, dtype=np.float32).reshape(-1)

    if verbose:
        print(
            "Noise levels (mean, std; in standard units): "
            + str(int(np.nanmean(trace_noise_levels * 100)) / 100)
            + ", "
            + str(int(np.nanstd(trace_noise_levels * 100)) / 100)
        )

    # Get model paths as dictionary (key: noise_level) with lists of model
    # paths for the different ensembles
    model_dict = get_model_paths(model_path)  # function defined below
    if verbose > 2:
        print("Loaded models:", str(model_dict))

    # Valid prediction span implied by the receptive window. This reproduces the
    # placement that utils.preprocess_traces() used, without building the tensor.
    (
        left_context,
        right_context,
        valid_start,
        valid_stop_offset,
    ) = utils.get_prediction_window_geometry(
        before_frac=before_frac, window_size=window_size
    )
    valid_stop = traces.shape[1] - valid_stop_offset
    Y_predict = np.zeros(traces.shape, dtype=np.float32)

    # Compute difference of noise levels between each neuron and each model; find the best fit
    differences = np.array(trace_noise_levels)[:,None] - np.array(noise_levels_model)[None,:]
    relative_differences = np.min(differences,axis=1)
    if np.mean(relative_differences) > 2:
        print("WARNING: The available models cannot match the experimentally obtained noise levels (difference: ", str(np.mean(relative_differences)),"). Please check that the computation of dF/F is performed correctly. Otherwise, please reach out and ask for pretrained models with higher noise level models (see: https://github.com/HelmchenLabSoftware/Cascade/issues/61).")
    best_model_for_each_neuron = np.argmin(np.abs(differences),axis=1)

    # Streaming inference chunk sizes. One chunk holds
    # cell_chunk_size x time_chunk_frames x window_size float32 values.
    time_chunk_frames = 8192
    cell_chunk_size = utils.estimate_inference_cell_chunk_size(
        num_cells=traces.shape[0],
        window_size=window_size,
        time_chunk_frames=time_chunk_frames,
        device=device,
    )
    halo = window_size - 1

    if verbose:
        print(
            "Using streaming inference with cell_chunk_size={} and time_chunk_frames={}.".format(
                cell_chunk_size, time_chunk_frames
            )
        )

    loaded_models = dict()
    try:
        # Use for each noise level the matching model
        for i, model_noise in enumerate(noise_levels_model):

            if verbose:
                print("\nPredictions for noise level {}:".format(model_noise))

            # select neurons which have this noise level:
            neuron_idx = np.where(best_model_for_each_neuron == i)[0]
            
            if len(neuron_idx) == 0:  # no neurons were selected
                if verbose:
                    print("\tNo neurons for this noise level")
                continue  # jump to next noise level

            # load PyTorch models for the given noise level
            if model_noise not in loaded_models:
                models = list()
                for model_path_file in model_dict[model_noise]:
                    model = utils.define_model(
                        filter_sizes=cfg["filter_sizes"],
                        filter_numbers=cfg["filter_numbers"],
                        dense_expansion=cfg["dense_expansion"],
                        windowsize=cfg["windowsize"],
                        loss_function=cfg["loss_function"],
                        optimizer=cfg["optimizer"],
                    )
                    model.load_state_dict(torch.load(model_path_file, map_location=device))
                    model.to(device)
                    model.eval()
                    models.append(model)
                loaded_models[model_noise] = models

            models = loaded_models[model_noise]

            if valid_stop <= valid_start:
                continue

            num_cell_chunks = max(1, math.ceil(len(neuron_idx) / cell_chunk_size))
            num_time_chunks = max(
                1, math.ceil((valid_stop - valid_start) / time_chunk_frames)
            )

            for neuron_chunk_start in range(0, len(neuron_idx), cell_chunk_size):
                neuron_chunk_stop = min(
                    neuron_chunk_start + cell_chunk_size, len(neuron_idx)
                )
                neuron_chunk_idx = neuron_idx[neuron_chunk_start:neuron_chunk_stop]
                cell_chunk_index = neuron_chunk_start // cell_chunk_size + 1

                if verbose:
                    print(
                        "\tcell chunk {}/{} ({} neurons), {} temporal chunks x {} ensembles".format(
                            cell_chunk_index,
                            num_cell_chunks,
                            len(neuron_chunk_idx),
                            num_time_chunks,
                            len(models),
                        )
                    )

                for center_start in range(valid_start, valid_stop, time_chunk_frames):
                    center_end = min(center_start + time_chunk_frames, valid_stop)
                    valid_frames = center_end - center_start

                    # Extend the slice by the halo so every requested output frame
                    # has its full window available inside traces_chunk.
                    trace_start = max(0, center_start - halo)
                    trace_end = min(traces.shape[1], center_end + halo)
                    window_start_offset = center_start - left_context - trace_start

                    windows_chunk = utils.build_streaming_window_tensor(
                        traces_chunk=traces[neuron_chunk_idx, trace_start:trace_end],
                        window_size=window_size,
                        window_start_offset=window_start_offset,
                        valid_frames=valid_frames,
                        device=device,
                    )

                    prediction_sum = torch.zeros(
                        windows_chunk.shape[0], dtype=torch.float32, device=device
                    )

                    with torch.inference_mode():
                        for j, model in enumerate(models):
                            offset = 0
                            for batch_X in torch.split(windows_chunk, batch_size, dim=0):
                                outputs = model(batch_X).reshape(-1)
                                prediction_sum[offset : offset + outputs.shape[0]] += outputs
                                offset += outputs.shape[0]

                    prediction_chunk = (
                        prediction_sum / len(models)
                    ).view(len(neuron_chunk_idx), valid_frames)
                    Y_predict[neuron_chunk_idx, center_start:center_end] = (
                        prediction_chunk.detach().cpu().numpy()
                    )

                    del prediction_chunk
                    del prediction_sum
                    del windows_chunk
    finally:
        # Clean up models from memory
        for models in loaded_models.values():
            for model in models:
                del model
        loaded_models.clear()
        if device.type == "cuda" and torch.cuda.is_available():
            torch.cuda.empty_cache()

    if threshold is False:  # only if 'False' is passed as argument
        if verbose:
            print(
                "Skipping the thresholding. There can be negative values in the result."
            )

    elif threshold == 1:  # (1 or True)
        # Cut off noise floor (lower than 1/e of a single action potential)

        from scipy.ndimage import gaussian_filter
        from scipy.ndimage import binary_dilation

        # find out empirically  how large a single AP is (depends on frame rate and smoothing)
        single_spike = np.zeros(
            1001,
        )
        single_spike[501] = 1
        single_spike_smoothed = gaussian_filter(
            single_spike.astype(float), sigma=smoothing * sampling_rate
        )
        threshold_value = np.max(single_spike_smoothed) / np.exp(1)

        # Set everything below threshold to zero.
        # Use binary dilation to avoid clipping of true events.
        for neuron in range(Y_predict.shape[0]):
            # ignore warning because of nan's in Y_predict in comparison with value
            with np.errstate(invalid="ignore"):
                activity_mask = Y_predict[neuron, :] > threshold_value
            activity_mask = binary_dilation(
                activity_mask, iterations=int(smoothing * sampling_rate)
            )

            Y_predict[neuron, ~activity_mask] = 0

            Y_predict[
                Y_predict < 0
            ] = 0  # set possible negative values in dilated mask to 0

    elif threshold == 0:
        # ignore warning because of nan's in Y_predict in comparison with value
        with np.errstate(invalid="ignore"):
            Y_predict[Y_predict < 0] = 0

    else:
        raise Exception(
            'Invalid value of threshold "{}". Only 0, 1 (or True) or False allowed'.format(
                threshold
            )
        )

    # NaN or 0 for first and last datapoints, for which no predictions can be made
    Y_predict[:, 0 : int(before_frac * window_size)] = padding
    Y_predict[:, -int((1 - before_frac) * window_size) :] = padding

    print("Spike rate inference done.")

    return Y_predict

def verify_config_dict(config_dictionary):

    """Perform some test to catch the most likely errors when creating config files"""

    # TODO: Implement
    print("Not implemented yet...")


def create_model_folder(config_dictionary, model_folder="Pretrained_models"):

    """Creates a new folder in model_folder and saves config.yaml file there

    Parameters
    ----------
    config_dictionary : dict
        Dictionary with keys like 'model_name' or 'training_datasets'
        Values which are not specified will be set to default values defined in
        the config_template in config.py

    model_folder : str
        Absolute or relative path, which defines the location at which the new
        folder containing the config file will be created
        Default value 'Pretrained_models' assumes a current working directory
        in the Cascade folder

    """
    cfg = config_dictionary  # shorter name

    # TODO: call here verify_config_dict

    # TODO: check here the current directory, might not be the main folder...
    model_path = os.path.join(model_folder, cfg["model_name"])

    if not os.path.exists(model_path):
        # create folder
        try:
            os.mkdir(model_path)
            print('Created new directory "{}"'.format(os.path.abspath(model_path)))
        except:
            print(model_path + " already exists")

        # save config file into the folder
        config.write_config(cfg, os.path.join(model_path, "config.yaml"))

    else:
        warnings.warn(
            "There is already a folder called {}. ".format(cfg["model_name"])
            + "Please rename your model."
        )
def get_model_paths(model_path):
    """Find all models in the model folder and return as dictionary
    ( Helper function called by predict() )
    Returns
    -------
    model_dict : dict
        Dictionary with noise_level (int) as keys and entries are lists of model paths
    """
    import glob, re
    all_models = glob.glob(os.path.join(model_path, "*.pth"))
    all_models = sorted(all_models)  # sort
    # Exception in case no model was found to catch this mistake where it happened
    if len(all_models) == 0:
        m = 'No models (*.pth files) were found in the specified folder "{}".'.format(
            os.path.abspath(model_path)
        )
        raise Exception(m)
    # dictionary with key for noise level, entries are lists of models
    model_dict = dict()
    for model_path in all_models:
        try:
            noise_level = int(re.findall(r"_NoiseLevel_(\d+)", model_path)[0])
        except:
            print("Error while processing the file with name: ", model_path)
            raise
        # add model path to the model dictionary
        if noise_level not in model_dict:
            model_dict[noise_level] = list()
        model_dict[noise_level].append(model_path)
        
    return model_dict

def download_model(
    model_name,
    model_folder="Pretrained_models",
    info_file_link="https://raw.githubusercontent.com/PTRRupprecht/CascadeTorch/refs/heads/master/Pretrained_models/available_models_CascadeTorch.yaml",
    verbose=1,
):
    """Download and unzip pretrained model from the online repository

    Parameters
    ----------
    model_name : str
        Name of the model, e.g. 'Global_EXC_30Hz_smoothing25ms'
        This name has to correspond to a pretrained model that is available for download
        To see available models, run this function with model_name='update_models' and
        check the downloaded file 'available_models_CascadeTorch.yaml'

    model_folder: str
        Absolute or relative path, which defines the location of the specified model_name folder
        Default value 'Pretrained_models' assumes a current working directory in the Cascade folder

    info_file_link: str
        Direct download link to yaml file which contains download links for new models.
        Default value is official repository of models.

    verbose : int
        If 0, no messages are printed. if larger than 0, the user is informed about status.

    """

    from urllib.request import urlopen
    import zipfile

    # Download the current yaml file with information about available models first
    new_file = os.path.join(model_folder, "available_models_CascadeTorch.yaml")
    with urlopen(info_file_link) as response:
        text = response.read()

    with open(new_file, "wb") as f:
        f.write(text)

    # check if the specified model_name is present
    download_config = config.read_config(
        new_file
    )  # orderedDict with model names as keys

    if model_name not in download_config.keys():
        if model_name == "update_models":
            print(
                "You can now check the updated available_models_CascadeTorch.yaml file for valid model names."
            )
            print("File location:", os.path.abspath(new_file))
            return

        raise Exception(
            'The specified model_name "{}" is not in the list of available models. '.format(
                model_name
            )
            + "Available models for download are: {}".format(
                list(download_config.keys())
            )
        )

    if verbose:
        print('Downloading and extracting new model "{}"...'.format(model_name))

    # download and save .zip file of model
    download_link = download_config[model_name]["Link"]
    with urlopen(download_link) as response:
        data = response.read()

    tmp_file = os.path.join(model_folder, "tmp_zipped_model.zip")
    with open(tmp_file, "wb") as f:
        f.write(data)

    # unzip the model and save in the corresponding folder
    with zipfile.ZipFile(tmp_file, "r") as zip_ref:
        zip_ref.extractall(path=os.path.join(model_folder, model_name))

    os.remove(tmp_file)

    if verbose:
        print(
            'Pretrained model was saved in folder "{}"'.format(
                os.path.abspath(os.path.join(model_folder, model_name))
            )
        )

    return