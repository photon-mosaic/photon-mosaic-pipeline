import numpy as np


def define_model(
    filter_sizes,
    filter_numbers,
    dense_expansion,
    windowsize,
    loss_function,
    optimizer,
):
    """ "
    Defines the model using Torch.
    The model consists of 3 convolutional layers ('conv_filter'), 2
    downsampling layers ('MaxPooling1D') and 1 dense layer ('Dense').
    To modify the architecture of the network, only the define_model() function
    needs to be modified.
    Example:
        model = define_model(filter_sizes, filter_numbers, dense_expansion,
                             windowsize, loss_function, optimizer)
    """
    import torch.nn as nn

    class CascadeModel(nn.Module):
        def __init__(
            self, filter_sizes, filter_numbers, dense_expansion, windowsize
        ):
            super(CascadeModel, self).__init__()

            self.conv1 = nn.Conv1d(
                1, filter_numbers[0], filter_sizes[0], stride=1
            )
            self.relu1 = nn.ReLU()

            self.conv2 = nn.Conv1d(
                filter_numbers[0], filter_numbers[1], filter_sizes[1]
            )
            self.relu2 = nn.ReLU()

            self.pool1 = nn.MaxPool1d(2)

            self.conv3 = nn.Conv1d(
                filter_numbers[1], filter_numbers[2], filter_sizes[2]
            )
            self.relu3 = nn.ReLU()

            self.pool2 = nn.MaxPool1d(2)

            # Dense layer applied per timestep
            self.dense1 = nn.Linear(filter_numbers[2], dense_expansion)
            self.relu4 = nn.ReLU()

            # Calculate flattened size for final layer
            flattened_size = (
                self._calculate_flattened_size(windowsize, filter_sizes)
                * dense_expansion
            )

            self.dense2 = nn.Linear(flattened_size, 1)

        def _calculate_flattened_size(self, windowsize, filter_sizes):
            size = windowsize
            size = size - (filter_sizes[0] - 1)
            size = size - (filter_sizes[1] - 1)
            size = size // 2
            size = size - (filter_sizes[2] - 1)
            size = size // 2
            return size

        def forward(self, x):
            x = x.permute(0, 2, 1)

            x = self.conv1(x)
            x = self.relu1(x)

            x = self.conv2(x)
            x = self.relu2(x)

            x = self.pool1(x)

            x = self.conv3(x)
            x = self.relu3(x)

            x = self.pool2(x)

            x = x.permute(0, 2, 1)

            x = self.dense1(x)
            x = self.relu4(x)

            x = x.view(x.size(0), -1)

            x = self.dense2(x)

            return x

    model = CascadeModel(
        filter_sizes, filter_numbers, dense_expansion, windowsize
    )

    return model


def calculate_noise_levels(neurons_x_time, frame_rate):
    """ "
    Computes the noise levels for each neuron of the input matrix 'dF_traces'.

    The noise level is computed as the median absolute dF/F difference
    between two subsequent time points. This is a outlier-robust measurement
    that converges to the simple standard deviation of the dF/F trace for
    uncorrelated and outlier-free dF/F traces.

    Afterwards, the value is divided by the square root of the frame rate in
    order to make it comparable across recordings with different frame rates.


    input: dF_traces (matrix with nb_neurons x time_points)
    output: vector of noise levels for all neurons

    """
    dF_traces = neurons_x_time
    noise_levels = np.nanmedian(
        np.abs(np.diff(dF_traces, axis=-1)), axis=-1
    ) / np.sqrt(frame_rate)
    return noise_levels * 100  # scale noise levels to percent


def get_prediction_window_geometry(before_frac, window_size):
    """
    Return the valid prediction span implied by the receptive window.

    A prediction can be made for frame ``t`` only if the full window
    ``[t - left_context, t - left_context + window_size)`` lies inside the
    trace.

    Parameters
    ----------
    before_frac : float
        Positioning of the window around the current time point; 0.5 means
        the current time point sits at the centre of the window.
    window_size : int
        Size of the receptive window of the deep network.

    Returns
    -------
    left_context : int
        Number of frames of the window that precede the predicted frame.
    right_context : int
        Number of frames of the window that follow the predicted frame.
    valid_start : int
        First frame index for which a prediction can be made.
    valid_stop_offset : int
        Number of trailing frames for which no prediction can be made; the
        exclusive upper bound is ``n_frames - valid_stop_offset``.
    """

    left_context = int(before_frac * window_size - 1)
    right_context = window_size - left_context - 1
    valid_start = left_context
    valid_stop_offset = right_context
    return left_context, right_context, valid_start, valid_stop_offset


def estimate_inference_cell_chunk_size(
    num_cells, window_size, time_chunk_frames, device, max_chunk_size=256
):
    """
    Return a conservative fixed cell chunk size for streaming inference.

    The chunk size is deliberately a constant rather than something derived
    from available memory: peak usage is then predictable and independent of
    the recording size. With the defaults, one chunk is
    ``max_chunk_size x time_chunk_frames x window_size x 4 bytes`` (256 x 8192
    x 64 x 4 = 0.5 GB), regardless of how many neurons or frames the session
    contains.

    ``window_size``, ``time_chunk_frames`` and ``device`` are accepted but
    unused; they are part of the signature so the heuristic can be made
    adaptive later without touching call sites.

    Parameters
    ----------
    num_cells : int
        Total number of cells to be processed.
    window_size : int
        Unused. Size of the receptive window of the deep network.
    time_chunk_frames : int
        Unused. Number of predicted frames per temporal chunk.
    device : torch.device
        Unused. Device inference will run on.
    max_chunk_size : int or None
        Upper bound on cells per chunk. If None, all cells are processed at
        once.

    Returns
    -------
    int
        Number of cells to process per chunk.
    """

    if num_cells <= 0:
        return 0

    del window_size, time_chunk_frames, device
    if max_chunk_size is None:
        return num_cells
    return min(num_cells, max_chunk_size)


def build_streaming_window_tensor(
    traces_chunk, window_size, window_start_offset, valid_frames, device
):
    """
    Materialize only the requested sliding windows on the target device.

    ``traces_chunk`` must already include the halo needed on both sides, i.e.
    the caller is responsible for extending the slice by ``window_size - 1``
    frames where the trace allows it, and for computing ``window_start_offset``
    accordingly.

    Parameters
    ----------
    traces_chunk : 2d numpy array (cells x frames)
        Slice of the dF/F traces covering the requested frames plus halo.
    window_size : int
        Size of the receptive window of the deep network.
    window_start_offset : int
        Index, within the unfolded windows of ``traces_chunk``, of the window
        corresponding to the first requested output frame.
    valid_frames : int
        Number of output frames requested from this chunk.
    device : torch.device
        Device on which the windows are materialized.

    Returns
    -------
    torch.Tensor
        Tensor of shape ``(cells * valid_frames, window_size, 1)``, ordered
        cell-major, ready to be fed to the network.
    """

    import torch

    traces_chunk = np.ascontiguousarray(traces_chunk, dtype=np.float32)
    traces_tensor = torch.as_tensor(
        traces_chunk, dtype=torch.float32, device=device
    )
    window_views = traces_tensor.unfold(dimension=1, size=window_size, step=1)
    window_views = window_views[
        :, window_start_offset : window_start_offset + valid_frames, :
    ]
    return window_views.contiguous().view(-1, window_size, 1)
