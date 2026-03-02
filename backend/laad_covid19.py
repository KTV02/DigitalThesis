# LSTM-Autoencoder based Anomaly Detection (LAAD)
# detects abnormal RHR; uses all training data; augments 8 times the training data size.

######################################################
# Author: Gireesh K. Bogu                            #
# Email: gbogu17@stanford.edu                        #
# Location: Dept.of Genetics, Stanford University    #
# Date: Nov 26 2020                                  #
######################################################

#python laad_RHR_keras_v4.py  --heart_rate COVID-19-Wearables/ASFODQR_hr.csv --steps COVID-19-Wearables/ASFODQR_steps.csv --myphd_id ASFODQR --symptom_date 2024-08-14


import warnings
warnings.filterwarnings('ignore')
import sys 
import argparse
import copy
import numpy as np
import pandas as pd
import seaborn as sns
import itertools
from itertools import cycle
from tqdm import tqdm
import random

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

import numpy as np
try:
    # prefer NumPy (kept, stable)
    interp = np.interp
except Exception:
    # last resort for very old stacks
    from scipy import interp

import arff  # provided by liac-arff
import io
import os
from pathlib import Path


def _load_arff(source):
    """Load ARFF content from a path, file-like object, or raw string."""
    if hasattr(source, "read"):
        return arff.load(source)
    if isinstance(source, (bytes, bytearray)):
        return arff.load(io.StringIO(source.decode("utf-8")))
    if isinstance(source, str):
        if os.path.exists(source):
            with open(source, "r", encoding="utf-8") as f:
                return arff.load(f)
        return arff.load(io.StringIO(source))
    raise TypeError(f"Unsupported ARFF source type: {type(source)}")


def a2p(source):
    """Convert ARFF data into a pandas DataFrame, mirroring arff2pandas."""
    obj = _load_arff(source)  # dict: relation, attributes, data, description
    attrs = [name for (name, _spec) in obj["attributes"]]
    df = pd.DataFrame(obj["data"], columns=attrs)
    df.replace("?", np.nan, inplace=True)
    return df
from datetime import date, datetime, timedelta
from statsmodels.tsa.seasonal import seasonal_decompose

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.models import Sequential, load_model, save_model

from pylab import rcParams
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FormatStrFormatter
from matplotlib import rc
sns.set(style='whitegrid', palette='muted', font_scale=1.2)
palette = ["#01BEFE", "#FFDD00", "#FF7D00", "#FF006D", "#ADFF02", "#8F00FF"]
sns.set_palette(sns.color_palette(palette))
rcParams['figure.figsize'] = 12, 8

# as command prompts -----------------------

parser = argparse.ArgumentParser(description='Find anomalies in wearables time-series data')
parser.add_argument('--heart_rate', metavar='', help ='raw heart rate count with a header = heartrate')
parser.add_argument('--steps',metavar='', help ='raw steps count with a header = steps')
parser.add_argument('--myphd_id',metavar='', default = 'myphd_id', help ='user myphd_id')
parser.add_argument('--symptom_date', metavar='', default = 'NaN', help = 'symptom date with y-m-d format')
parser.add_argument('--random_seed', metavar='', type=int, default=42, help='random seed')
parser.add_argument('--output_dir', '--output-dir', dest='output_dir', default='.',
                    help='Directory where LAAD artifacts (CSVs, plots, models) will be written')
args = parser.parse_args()

# as arguments -----------------------

fitbit_oldProtocol_hr = args.heart_rate
fitbit_oldProtocol_steps = args.steps
myphd_id = args.myphd_id
symptom_date = args.symptom_date
RANDOM_SEED = args.random_seed
output_dir = Path(args.output_dir).expanduser().resolve()
output_dir.mkdir(parents=True, exist_ok=True)
print(f"[INFO] Saving LAAD artifacts to {output_dir}")

np.random.seed(RANDOM_SEED)
tf.random.set_seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Hyper-parameters --------------------

TIME_STEPS = 8
EPOCHS = 1200 
BATCH_SIZE = 64
VALIDATION_SPLIT = 0.05
LEARNING_RATE = 0.0001

#BASE_LINE_DAYS = 10


class LAAD:

    # infer resting heart rate ------------------------------------------------------

    def resting_heart_rate(self, heartrate, steps):
        """
        This function uses heart rate and steps data to infer resting heart rate.
        It filters the heart rate with steps that are zero in a rolling 12-minute
        window (including the current minute).
        """

        # --- Load heart rate (only needed columns) ---
        df_hr = pd.read_csv(heartrate)
        # Ensure expected columns exist
        if "datetime" not in df_hr.columns or "heartrate" not in df_hr.columns:
            raise ValueError("HR CSV must contain columns: ['datetime','heartrate']")

        df_hr["datetime"] = pd.to_datetime(df_hr["datetime"], errors="coerce")
        df_hr = df_hr.dropna(subset=["datetime"])
        df_hr = df_hr.set_index("datetime")

        # Keep only numeric HR
        df_hr = df_hr[["heartrate"]].apply(pd.to_numeric, errors="coerce")

        # --- Load steps (only needed columns) ---
        df_steps = pd.read_csv(steps)
        if "datetime" not in df_steps.columns or "steps" not in df_steps.columns:
            raise ValueError("Steps CSV must contain columns: ['datetime','steps']")

        df_steps["datetime"] = pd.to_datetime(df_steps["datetime"], errors="coerce")
        df_steps = df_steps.dropna(subset=["datetime"])
        df_steps = df_steps.set_index("datetime")

        # Keep only numeric steps
        df_steps = df_steps[["steps"]].apply(pd.to_numeric, errors="coerce")

        # --- Resample to minute grid (preserves your intent) ---
        # HR: mean per minute; Steps: sum per minute
        hr_min   = df_hr.resample("1min").mean()
        steps_min = df_steps.resample("1min").sum()

        # --- Merge on minute index (outer keeps zeros & gaps) ---
        df1 = hr_min.join(steps_min, how="outer")

        # --- Rolling 12-minute zero-steps filter (including current minute) ---
        # min_periods=12 preserves your original strict condition
        df1["steps_window_12"] = df1["steps"].rolling(12, min_periods=12).sum()

        # Keep only rows where steps were zero in the 12-min window AND HR present
        df1 = df1.loc[(df1["steps_window_12"] == 0) & (df1["heartrate"].notna())]

        return df1


    # pre-processing ------------------------------------------------------

    def pre_processing(self, resting_heart_rate: pd.DataFrame) -> pd.DataFrame:
        """
        Smooth RHR, resample hourly, drop non-RHR columns, and return a clean frame.
        Expects `resting_heart_rate` from resting_heart_rate() above.
        Returns a DataFrame with index=DatetimeIndex (hourly) and column 'RHR'.
        """
        df1 = resting_heart_rate.copy()

        # Guard: nothing to do if empty
        if df1.empty:
            return pd.DataFrame(columns=["RHR"])

        # Keep only numeric columns (heartrate, steps, steps_window_12)
        df1 = df1.select_dtypes(include="number")

        # Smooth (moving average). If too short, just keep as-is.
        if len(df1) >= 10:
            df1_rom = df1.rolling(400, min_periods=1).mean()
        else:
            df1_rom = df1

        # Hourly mean
        df1_resmp = df1_rom.resample("1H").mean()

        # Drop steps-related columns; keep only RHR
        out = df1_resmp.drop(columns=[c for c in df1_resmp.columns if c != "heartrate"], errors="ignore")
        out = out.rename(columns={"heartrate": "RHR"})
        out = out.dropna(subset=["RHR"])

        return out


   # data splitting ------------------------------------------------------

    def data_splitting(self, processed_data, symptom_date):
        """
        It splits data into training data by taking first 10 days and the rest as testing data.
        It also creates windows of pre- and post-symptomatic COVID-periods.
        """

        # --- derive window bounds ---
        symptom_date1         = pd.to_datetime(symptom_date)
        symptom_date_before_7 = symptom_date1 + timedelta(days=-7)
        symptom_date_after_21 = symptom_date1 + timedelta(days=21)
        symptom_date_before_20 = symptom_date1 + timedelta(days=-20)
        symptom_date_before_10 = symptom_date1 + timedelta(days=-10)

        # --- make sure index is a clean DatetimeIndex ---
        df = processed_data.copy()
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[df.index.notna()].sort_index()

        if df.empty:
            raise ValueError("processed_data is empty (or has no valid datetime index) before splitting.")

        # Optional: ensure we only keep the columns we actually need
        # (RHR should already be present from your pre_processing step)
        if "RHR" not in df.columns:
            raise ValueError("processed_data must contain an 'RHR' column before data_splitting.")

        # --- convenient date column for splitting by day (normalized to midnight) ---
        df["date"] = df.index.normalize()  # Timestamp at 00:00 of each day
        start = df.index.min()
        end   = df.index.max()

        # --- train/test split by date windows ---
        # Train: strictly before (symptom_date - 20 days)
        train_mask = df["date"] < symptom_date_before_20.normalize()
        train = df.loc[train_mask].drop(columns=["date"], errors="ignore")

        # Test: on/after (symptom_date - 20 days)
        test  = df.loc[~train_mask].drop(columns=["date"], errors="ignore")

        # Keep index as datetime for downstream code
        # (No set_index/reset_index gymnastics; index already is datetime)

        # --- delta RHR in anomaly window [ -7 , +21 ] around symptom date ---
        # This follows the original intent: compare anomaly-window RHR to train mean.
        if train.empty:
            raise ValueError("Training set is empty after split (no dates before symptom_date - 20 days).")
        train_baseline_RHR = train["RHR"].mean()

        test_anomaly_RHR = test.loc[
            (test.index >= symptom_date_before_7) & (test.index <= symptom_date_after_21),
            "RHR"
        ]
        test_anomaly_delta_RHR = test_anomaly_RHR - train_baseline_RHR

        # --- optional CSV of split dates (kept from your code) ---
        with open(output_dir / f"{myphd_id}_data_split_dates.csv", 'w') as f:
            print(
                "id", "start_date", "symptom_date1", "symptom_date_before_20",
                "symptom_date_before_7", "symptom_date_before_10", "symptom_date_after_21", "end_date", "\n",
                myphd_id, start, symptom_date, symptom_date_before_20, symptom_date_before_7,
                symptom_date_before_10, symptom_date_after_21, end,
                file=f
            )

        return (
            symptom_date1,
            symptom_date_before_20,
            symptom_date_before_7,
            symptom_date_before_10,
            symptom_date_after_21,
            train,
            test,
            test_anomaly_delta_RHR,
        )
    # standardization ------------------------------------------------------

    def standardization(self, train_data, test_data,
                    symptom_date_before_20, symptom_date_before_7,
                    symptom_date_before_10, symptom_date_after_21):
        """
        Standardize RHR with Z-score (fit on train, transform test).
        Then split test into test_normal and test_anomaly by date windows.

        Inputs/assumptions (unchanged from your pipeline):
        - train_data, test_data: indexed by DatetimeIndex
        - both contain a numeric column 'RHR'
        - date strings (symptom_date_*): sliceable with DatetimeIndex
        Returns:
        train_data, test_data, test_normal, test_anomaly, all_merged
        """

        # --- Defensive copies to avoid mutating upstream dataframes ---
        train_data = train_data.copy()
        test_data = test_data.copy()

        # --- Ensure DatetimeIndex for proper date slicing later ---
        if not isinstance(train_data.index, pd.DatetimeIndex):
            try:
                train_data.index = pd.to_datetime(train_data.index, errors="coerce")
            except Exception:
                pass
        if not isinstance(test_data.index, pd.DatetimeIndex):
            try:
                test_data.index = pd.to_datetime(test_data.index, errors="coerce")
            except Exception:
                pass

        # Drop any rows where index couldn't be parsed
        train_data = train_data[train_data.index.notna()]
        test_data  = test_data[test_data.index.notna()]

        # --- Keep only the RHR column for scaling (your logic uses RHR downstream) ---
        if "RHR" not in train_data.columns:
            raise ValueError("Train set is missing column 'RHR'.")
        if "RHR" not in test_data.columns:
            raise ValueError("Test set is missing column 'RHR'.")

        # --- Make sure RHR is numeric ---
        train_data["RHR"] = pd.to_numeric(train_data["RHR"], errors="coerce")
        test_data["RHR"]  = pd.to_numeric(test_data["RHR"], errors="coerce")

        # --- Confirm we have something to fit on ---
        if train_data["RHR"].dropna().empty:
            raise ValueError(
                "No training samples available after preprocessing/windowing. "
                "This usually means the 12-minute zero-steps filter removed all rows, "
                "or there is not enough HR data in the selected date range."
            )

        # --- Fit scaler on TRAIN only; transform TRAIN & TEST ---
        scaler = StandardScaler()
        train_data.loc[:, ["RHR"]] = scaler.fit_transform(train_data[["RHR"]])

        # Optional artifact from earlier merges: drop level_0 if it exists
        if "level_0" in test_data.columns:
            test_data = test_data.drop(columns=["level_0"])

        # If test has no non-NA RHR, bail early with a clear error
        if test_data["RHR"].dropna().empty:
            raise ValueError(
                "No test samples available (RHR all NA) after preprocessing/windowing. "
                "Adjust your date range or check inputs."
            )
        test_data.loc[:, ["RHR"]] = scaler.transform(test_data[["RHR"]])

        # --- Split test into normal/anomaly windows (your original windows) ---
        # Note: .loc on DatetimeIndex with ISO-like strings is fine.
        test_anomaly = test_data.loc[symptom_date_before_7:symptom_date_after_21]
        test_normal  = test_data.loc[symptom_date_before_20:symptom_date_before_10]

        # --- Merge for downstream plots/stats (unchanged) ---
        all_merged = pd.concat([train_data, test_data]).sort_index()

        # --- Optional: size report (kept as-is) ---
        with open(output_dir / f"{myphd_id}_data_size.csv", 'w') as f:
            print("id", "train ", "test ", "test_normal ", "test_anomaly ", "\n",
                myphd_id, train_data.shape, test_data.shape,
                test_normal.shape, test_anomaly.shape, file=f)

        return train_data, test_data, test_normal, test_anomaly, all_merged


    # creating LSTM input ------------------------------------------------------
    """
    Apply lag method to create subsequences by keeping the temporal order of the data constant 
    """

    def create_dataset(self, dataset, time_steps=1):
        Xs = []
        for i in range(len(dataset) - time_steps):
            v = dataset.iloc[i:(i + time_steps)].values
            Xs.append(v)
        return np.array(Xs)


    # Data Augmentation ------------------------------------------------------
    """
    Applies a combination of different distortions to the data including 
    scaling, rotating, permutating, magnitude warping, time-warping,
    window slicing, and window warping.

    Input shape convention: (N, T, C)
    N: number of windows/samples
    T: time steps
    C: channels/features
    """
    def augmentation(self, dataset):

        def scaling(dataset, sigma=0.1):
            factor = np.random.normal(loc=1., scale=sigma, size=(dataset.shape[0], dataset.shape[2]))
            # broadcast factor along time axis
            data_scaled = dataset * factor[:, np.newaxis, :]
            return data_scaled

        def rotation(dataset):
            # random sign flip per (sample, channel), then randomly permute channel order
            flip = np.random.choice([-1, 1], size=(dataset.shape[0], dataset.shape[2]))
            rotate_axis = np.arange(dataset.shape[2])
            np.random.shuffle(rotate_axis)
            data_rotation = flip[:, np.newaxis, :] * dataset[:, :, rotate_axis]
            return data_rotation

        def permutation(dataset, max_segments=5, seg_mode="equal"):
            """
            Randomly cut the time axis into segments and permute those segments.
            FIX: avoid np.random.permutation on a ragged list of arrays.
            """
            T = dataset.shape[1]
            orig_steps = np.arange(T)
            # number of segments per sample in [1, max_segments]
            num_segs = np.random.randint(1, max_segments + 1, size=(dataset.shape[0],))
            data_permute = np.zeros_like(dataset)

            for i, pat in enumerate(dataset):
                k = int(num_segs[i])
                if k > 1:
                    if seg_mode == "random":
                        # choose k-1 split points in (1..T-1) to avoid empty segments
                        split_points = np.random.choice(np.arange(1, T), size=k - 1, replace=False)
                        split_points.sort()
                        splits = np.split(orig_steps, split_points)
                    else:
                        # equal (or as equal as possible) segments
                        splits = np.array_split(orig_steps, k)

                    # --- robust shuffle of variable-length splits ---
                    order = np.random.permutation(len(splits))
                    reordered = [splits[idx] for idx in order]
                    warp = np.concatenate(reordered, axis=0)  # 1D index array length T
                    # ------------------------------------------------

                    data_permute[i] = pat[warp]
                else:
                    data_permute[i] = pat
            return data_permute

        def magnitude_warp(dataset, sigma=0.2, knot=4):
            from scipy.interpolate import CubicSpline
            T = dataset.shape[1]
            orig_steps = np.arange(T)
            random_warps = np.random.normal(loc=1.0, scale=sigma, size=(dataset.shape[0], knot + 2, dataset.shape[2]))
            warp_steps = (np.ones((dataset.shape[2], 1)) * (np.linspace(0, T - 1., num=knot + 2))).T
            data_m_Warp = np.zeros_like(dataset)
            for i, pat in enumerate(dataset):
                warper = np.array([
                    CubicSpline(warp_steps[:, dim], random_warps[i, :, dim])(orig_steps)
                    for dim in range(dataset.shape[2])
                ]).T
                data_m_Warp[i] = pat * warper
            return data_m_Warp

        def time_warp(dataset, sigma=0.2, knot=4):
            from scipy.interpolate import CubicSpline
            T = dataset.shape[1]
            orig_steps = np.arange(T)
            random_warps = np.random.normal(loc=1.0, scale=sigma, size=(dataset.shape[0], knot + 2, dataset.shape[2]))
            warp_steps = (np.ones((dataset.shape[2], 1)) * (np.linspace(0, T - 1., num=knot + 2))).T
            data_t_Warp = np.zeros_like(dataset)
            for i, pat in enumerate(dataset):
                for dim in range(dataset.shape[2]):
                    time_warp_curve = CubicSpline(warp_steps[:, dim], warp_steps[:, dim] * random_warps[i, :, dim])(orig_steps)
                    scale = (T - 1) / time_warp_curve[-1]
                    data_t_Warp[i, :, dim] = np.interp(orig_steps, np.clip(scale * time_warp_curve, 0, T - 1), pat[:, dim]).T
            return data_t_Warp

        def window_slice(dataset, reduce_ratio=0.9):
            T = dataset.shape[1]
            target_len = int(np.ceil(reduce_ratio * T))
            if target_len >= T:
                return dataset
            starts = np.random.randint(low=0, high=T - target_len, size=(dataset.shape[0])).astype(int)
            ends = (target_len + starts).astype(int)
            data_w_Slice = np.zeros_like(dataset)
            base_x = np.arange(target_len)
            out_x = np.linspace(0, target_len - 1, num=T)
            for i, pat in enumerate(dataset):
                s, e = int(starts[i]), int(ends[i])
                for dim in range(dataset.shape[2]):
                    seg = pat[s:e, dim]
                    data_w_Slice[i, :, dim] = np.interp(out_x, base_x, seg)
            return data_w_Slice

        def window_warp(dataset, window_ratio=0.1, scales=(0.5, 2.)):
            T = dataset.shape[1]
            warp_scales = np.random.choice(scales, dataset.shape[0])
            warp_size = int(np.ceil(window_ratio * T))
            if warp_size < 2 or warp_size >= T - 1:
                return dataset
            window_steps = np.arange(warp_size)
            starts = np.random.randint(low=1, high=T - warp_size - 1, size=(dataset.shape[0])).astype(int)
            ends = (starts + warp_size).astype(int)
            data_w_Warp = np.zeros_like(dataset)
            out_x = np.linspace(0, T - 1., num=T)
            for i, pat in enumerate(dataset):
                s, e = int(starts[i]), int(ends[i])
                for dim in range(dataset.shape[2]):
                    start_seg = pat[:s, dim]
                    window_seg = np.interp(np.linspace(0, warp_size - 1, num=int(max(1, warp_size * warp_scales[i]))),
                                        window_steps, pat[s:e, dim])
                    end_seg = pat[e:, dim]
                    warped = np.concatenate((start_seg, window_seg, end_seg))
                    data_w_Warp[i, :, dim] = np.interp(np.arange(T), np.linspace(0, T - 1., num=warped.size), warped)
            return data_w_Warp

        # Run all augmentations and concatenate
        data_scaled   = scaling(dataset)
        data_rotation = rotation(dataset)
        data_permute  = permutation(dataset)          # <-- fixed here
        data_m_Warp   = magnitude_warp(dataset)
        data_t_Warp   = time_warp(dataset)
        data_w_Slice  = window_slice(dataset)
        data_w_Warp   = window_warp(dataset)

        augment_dataset = np.concatenate(
            [dataset, data_scaled, data_rotation, data_permute, data_m_Warp, data_t_Warp, data_w_Slice, data_w_Warp],
            axis=0
        )
        return augment_dataset


    # LSTM Autoencoder model ------------------------------------------------------
    """
    Builds ENCODER and DECODER architecture with LSTM layers

    """

    def LA(self, train, valid):
        model = keras.Sequential()
        # shape [batch, time, features] => [batch, time, lstm_units]
        model.add(keras.layers.LSTM(units=128,
            input_shape=(train_dataset.shape[1], train_dataset.shape[2]), # univariate input
            return_sequences=True))
        #model.add(keras.layers.Dropout(rate=0.2))
        model.add(keras.layers.LSTM(units=64, return_sequences=False))
        model.add(keras.layers.RepeatVector(n=train_dataset.shape[1]))
        model.add(keras.layers.LSTM(units=64, return_sequences=True))
        model.add(keras.layers.LSTM(units=128, return_sequences=True))
        #model.add(keras.layers.Dropout(rate=0.2))
        # shape => [batch, time, features]
        model.add(keras.layers.TimeDistributed(
            keras.layers.Dense(units=train_dataset.shape[2]))) # univariate output
        model.compile(loss=tf.losses.MeanSquaredError(),
                optimizer=tf.optimizers.Adam(learning_rate=LEARNING_RATE),
                metrics=[tf.metrics.MeanSquaredError()])
        history = model.fit(train, valid, 
            batch_size=BATCH_SIZE,
            epochs=EPOCHS,
            validation_split=VALIDATION_SPLIT, 
            shuffle=False,
            callbacks=[early_stopping_callback, checkpoint_callback])
        return history, model


   # visualization ------------------------------------------------------

    def visualize_loss(self, history):
        history = pd.DataFrame(history.history)
        fig, ax = plt.subplots(1, figsize=(8,6))
        #ax = plt.figure(figsize=(8,5)).gca()
        ax.plot(history['loss'], lw=1, c='blue')
        ax.plot(history['val_loss'], lw=1, c='magenta')
        plt.ylabel('Loss\n')
        plt.xlabel('\nEpoch')
        plt.legend(['train', 'validation'])
        plt.title(myphd_id)
        plt.tight_layout()
        figure = fig.savefig(output_dir / f"{myphd_id}_loss.pdf")
        return figure


    # save model  ------------------------------------------------------

    def save_model(self, model):
        MODEL_PATH = myphd_id+'.pth' 
        torch.save(model, MODEL_PATH)
        return MODEL_PATH


    # define automatic threshold  ------------------------------------------------------
    """
    take the maximum MAE - Mean Absolute Error (loss) value of the train data  as a threshold to detect anomalies in test data
    """

    def predictions_loss_train(self, losses, train_dataset):
        plt.figure(figsize=(5,3))
        figure = sns.distplot(losses, bins=50, kde=True).set_title(myphd_id)
        plt.savefig(output_dir / f"{myphd_id}_predictions_loss_train.pdf")
        return figure

    def anomaly_threshold(self, losses):
        stats = pd.DataFrame(losses).describe()
        #print(stats)
        mean = stats.filter(like='mean', axis=0)
        mean = float(mean[0]) 
        std = stats.filter(like='std', axis=0)
        std = float(std[0]) 
        max = stats.filter(like='max', axis=0)
        max = float(max[0])
        
        
        # We can calculate the mean and standard deviation of training data loss 
        # then calculate the cut-off as more than 2 standard deviations from the mean.
        # We can then identify anomalies as those examples that fall outside of the defined upper limit.
        #cut_off = std * 3
        #THRESHOLD =  mean + cut_off
        THRESHOLD =  max
        return THRESHOLD


   # visualization ------------------------------------------------------

    def predictions_loss_test_normal(self, losses, train_normal_dataset):
        plt.figure(figsize=(5,3))
        figure = sns.distplot(losses, bins=50, kde=True).set_title(myphd_id)
        plt.savefig(output_dir / f"{myphd_id}_predictions_loss_test_normal.pdf")
        return figure

    def predictions_loss_test_anomaly(self, losses, test_anomaly_dataset):
        plt.figure(figsize=(5,3))
        figure = sns.distplot(losses, bins=50, kde=True).set_title(myphd_id)
        plt.savefig(output_dir / f"{myphd_id}_predictions_loss_test_anomaly.pdf")
        return figure
    
    def predictions_loss_test(self, losses, test_dataset):
        plt.figure(figsize=(5,3))
        figure = sns.distplot(losses, bins=50, kde=True).set_title(myphd_id)
        plt.savefig(output_dir / f"{myphd_id}_predictions_loss_test.pdf")
        return figure


    # save anomalies and delta RHR  ------------------------------------------------------

    def save_anomalies(self, test, test_anomaly_delta_RHR):
        test_score_df = pd.DataFrame(index=test[TIME_STEPS:].index)
        test_score_df['loss'] = losses
        test_score_df['threshold'] = THRESHOLD
        test_score_df['anomaly'] = test_score_df.loss > test_score_df.threshold
        test_score_df['RHR'] = test[TIME_STEPS:].RHR
        anomalies = test_score_df[test_score_df.anomaly == True]

        # turn lowered RHR to zero (we are only interested inn elevated RHR)
        #anomalies.loc[anomalies.RHR <=0 , 'loss'] = 0
        #anomalies.loc[anomalies.RHR <=0 , 'anomaly'] = False

        print("..................................................................\n" + myphd_id +": Anomalies:")
        print("..................................................................\n")
        print(anomalies)

        #save delta RHR of test anomaly data
        delta_RHR = pd.merge(anomalies, test_anomaly_delta_RHR, left_index=True, right_index=True)
        delta_RHR = delta_RHR.rename(columns={'RHR_y':'delta_RHR'})
        delta_RHR = delta_RHR['delta_RHR']
        #print(delta_RHR)
        delta_RHR.to_csv(output_dir / f"{myphd_id}_delta_RHR.csv")
        anomalies.to_csv(output_dir / f"{myphd_id}_anomalies.csv")
        return anomalies, delta_RHR


    # evaluate complete dataset  ------------------------------------------------------
    """
    For figures evaluate complete dataset annd plot loss of the all  the values as anomaly score later
    """

    def evaluate_complete_dataset(self, all_merged, THRESHOLD):
        plt.figure(figsize=(5,3))
        sns.distplot(losses, bins=50, kde=True).set_title(myphd_id)
        plt.savefig(output_dir / f"{myphd_id}_predictions_loss_all.pdf")
        anomalies = sum(l < THRESHOLD for l in losses)

        all_score_df = pd.DataFrame(index=all_merged[TIME_STEPS:].index)
        all_score_df['loss'] = losses
        all_score_df['threshold'] = THRESHOLD
        all_score_df['anomaly'] = all_score_df.loss > all_score_df.threshold
        all_score_df['RHR'] = all_merged[TIME_STEPS:].RHR
        all_anomalies = all_score_df
        
        all_anomalies.index = all_anomalies.index.rename('datetime')
        all_anomalies = all_anomalies.sort_index()

        # turn lowered RHR to zero (we are only interested inn elevated RHR)
        #all_anomalies.loc[all_anomalies.RHR <=0 , 'loss'] = 0
        #all_anomalies.loc[all_anomalies.RHR <=0 , 'anomaly'] = False

        all_anomalies.to_csv(output_dir / f"{myphd_id}_anomalies_all.csv")
        return all_anomalies


    # evaluate metrics  ------------------------------------------------------
    """
    True positives (TP) are the number of anomalous days that are correctly identified as anomalous,
    False negatives (FN) are the no.of anomalous days that are incorrectly identified as normal.
    -7+21 window (True preds are TPs and False are TNs)
    True negative (TN) are the number of normal days that are correctly identified as normal
    False positives (FP) are the no.of normal days that are incorrectly identified as anomalous. 
    -20-10: window (False=1)
    """

    def metrics_1(
        self,
        all_anomalies: pd.DataFrame,
        test_normal_data: pd.DataFrame,
        symptom_date_before_7: str,
        symptom_date_after_21: str,
        myphd_id: str,
    ):
        """
        Compute TP/FP/TN/FN using:
        - 7..21 day window around symptom date for positives (TP/FN)
        - a preconstructed 'test_normal_data' window for negatives (TN/FP)

        Expects:
        - all_anomalies index: datetime-like
        - columns: 'RHR' (numeric), 'anomaly' (bool/0-1)
        - test_normal_data index: datetime-like, column 'RHR' (numeric)
        """

        def listToStringWithoutBrackets(list1):
            return (
                str(list1)
                .replace("[", "").replace("]", "")
                .replace("'", "").replace("(", "").replace(")", "")
                .replace(": , ", ":").replace(":, ", ":")
            )

        # --- Ensure DatetimeIndex and column types
        def _ensure_dt_index(df):
            out = df.copy()
            if not isinstance(out.index, pd.DatetimeIndex):
                out.index = pd.to_datetime(out.index, errors="coerce")
            return out[~out.index.isna()]

        all_anom = _ensure_dt_index(all_anomalies)
        normal = _ensure_dt_index(test_normal_data)

        # Coerce expected columns
        if "RHR" in all_anom.columns:
            all_anom["RHR"] = pd.to_numeric(all_anom["RHR"], errors="coerce")
        else:
            raise ValueError("all_anomalies must contain 'RHR'")

        # anomaly may be bool/0-1/float; coerce to bool
        if "anomaly" in all_anom.columns:
            # anything >0 counts as True
            all_anom["anomaly"] = all_anom["anomaly"].astype(bool)
        else:
            raise ValueError("all_anomalies must contain 'anomaly'")

        # --- Hourly aggregation
        # anomaly per hour = any(True) in that hour; RHR per hour = mean
        hourly = all_anom.resample("1H").agg(
            RHR=("RHR", "mean"),
            anomaly=("anomaly", "max")
        )
        hourly = hourly.fillna({"anomaly": False})
        hourly = hourly[hourly["RHR"].notna() & (hourly["RHR"] != 0)]

        # ---- Positive window (7..21) → TP/FN
        win_pos = hourly.loc[symptom_date_before_7 : symptom_date_after_21]

        # Count hours labeled anomalous vs not (you can change to daily by resampling('1D').max())
        pos_counts = win_pos.groupby("anomaly")["RHR"].size().rename("count").reset_index()

        TP = int(pos_counts.loc[pos_counts["anomaly"] == True, "count"].sum()) if not pos_counts.empty else 0
        FN = int(pos_counts.loc[pos_counts["anomaly"] == False, "count"].sum()) if not pos_counts.empty else 0

        print("..................................................................")
        print(f"{myphd_id}: Metrics:")
        print("..................................................................")

        # ---- Negative window (your precomputed 'normal' subset) → TN/FP
        # Join hourly anomaly onto the normal window’s index
        normal = normal.copy()
        if "RHR" in normal.columns:
            normal["RHR"] = pd.to_numeric(normal["RHR"], errors="coerce")

        # Reduce normal to hourly too (so indices align semantically)
        normal_hourly = normal.resample("1H").agg(RHR=("RHR", "mean"))
        normal_hourly = normal_hourly[normal_hourly["RHR"].notna()]

        # Outer join to bring in anomaly labels (left join works too)
        neg_join = normal_hourly.join(hourly[["anomaly"]], how="left")
        neg_join["anomaly"] = neg_join["anomaly"].fillna(False).astype(bool)

        neg_counts = neg_join.groupby("anomaly")["RHR"].size().rename("count").reset_index()
        TN = int(neg_counts.loc[neg_counts["anomaly"] == False, "count"].sum()) if not neg_counts.empty else 0
        FP = int(neg_counts.loc[neg_counts["anomaly"] == True, "count"].sum()) if not neg_counts.empty else 0

        print("TP:", TP, "FP:", FP, "TN:", TN, "FN:", FN)

        # ---- Save & return
        out_df = pd.DataFrame([[TP, FP, TN, FN]], columns=["TP", "FP", "TN", "FN"], index=[myphd_id])
        out_df.to_csv(output_dir / f"{myphd_id}_all_basic_metrics.csv", header=True)

        formatted_list_2 = ('TP: ', TP, 'FP: ', FP, 'TN: ', TN, 'FN:', FN)
        formatted_list_2 = listToStringWithoutBrackets(formatted_list_2)

        return TP, FP, TN, FN, formatted_list_2

    # visualization ------------------------------------------------------

    def visualize_complete_dataset1(self, all_anomalies, symptom_date1, symptom_date_before_7, symptom_date_after_21, formatted_list_2):
        
        # original sequence
        all_score_df = all_anomalies
        ax1 = all_score_df[['RHR']].plot(figsize=(24,4.5), color="black", rot=90)
        ax1.set_xlim(all_score_df.index[0], all_score_df.index[-1]) 
        ax1.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%y-%b-%d'))
        ax1.set_ylabel('Orig Seq\n', fontsize = 20) # Y label
        ax1.set_xlabel('', fontsize = 0) # X label
        ax1.tick_params(axis='both', which='major', labelsize=22)
        ax1.set_xlabel('', fontsize = 0) # X label
        ax1.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
        ax1.set_title(myphd_id,fontweight="bold", size=30) # Title
        plt.xticks(fontsize=0, rotation=90)
        plt.tick_params(axis='both',which='both',bottom=True, top=False, labelbottom=True)
        plt.tight_layout()
        plt.savefig(output_dir / f"{myphd_id}_all_original_seq.pdf", bbox_inches='tight')
        #plt.show()


        # plot anomaly scores
        all_score_df1 = all_anomalies.reset_index()
        ax3 = all_score_df1.plot.scatter(x='datetime',y='loss', figsize=(24,4),  rot=90, marker='o', lw=2,
            c=['red' if i== True else 'mediumblue'  for i in all_score_df1['anomaly']])
        ax3.set_xlim(all_score_df1['datetime'].iloc[0], all_score_df1['datetime'].iloc[-1]) 
        ax3.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
        ax3.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%y-%b-%d'))
        ax3.set_ylabel('Anomaly Score\n', fontsize = 20) # Y label
        ax3.set_xlabel('', fontsize = 0) # X label
        ax3.axvline(pd.to_datetime(symptom_date_before_7), color='orange', zorder=1, linestyle='--',  lw=6, alpha=0.5) # Symptom date 
        ax3.axvline(pd.to_datetime(symptom_date1), color='red', zorder=1, linestyle='--',  lw=6, alpha=0.5) # Symptom date 
        ax3.axvline(pd.to_datetime(symptom_date_after_21), color='purple', zorder=1, linestyle='--', lw=6, alpha=0.5) # Symptom date 
        ax3.tick_params(axis='both', which='major', labelsize=18)
        ax3.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
        ax3.set_title(myphd_id+ '\n', fontweight="bold", size=30) # Title
        plt.axhline(y=THRESHOLD, color='grey', linestyle='--', lw=3, alpha=0.3)
        plt.tick_params(axis='both',which='both',bottom=True, top=False, labelbottom=True) 
        #plt.title(myphd_id + '\n\n', fontweight="bold", size=30) # Sub title
        plt.suptitle(formatted_list_2+ '\n', fontweight="bold", size=20) # Sub title
        #plt.tight_layout()
        plt.savefig(output_dir / f"{myphd_id}_all_anomaly_scores.pdf", bbox_inches='tight')
        #plt.show()


    @staticmethod
    def _safe_div(numer, denom, default=0.0):
        """Divide with zero-protection; returns default (0.0) if denom==0."""
        try:
            denom = float(denom)
            return float(numer) / denom if denom != 0 else float(default)
        except Exception:
            return float(default)

    def metrics_2(self, tp, fp, tn, fn, beta: float = 0.1):
        """
        Return scalar metrics with safe divisions.
        beta: F-beta weighting for Recall (default 0.1 as in your code).
        """
        tp = int(tp); fp = int(fp); tn = int(tn); fn = int(fn)

        sensitivity = self._safe_div(tp, tp + fn)  # recall (TPR)
        specificity = self._safe_div(tn, tn + fp)  # TNR
        ppv         = self._safe_div(tp, tp + fp)  # precision / PPV
        npv         = self._safe_div(tn, tn + fn)  # NPV
        precision   = ppv
        recall      = sensitivity

        denom = (beta * beta) * precision + recall
        fbeta = ((1.0 + beta * beta) * precision * recall) / denom if denom != 0 else 0.0

        return sensitivity, specificity, ppv, npv, precision, recall, fbeta


    # save metrics  ------------------------------------------------------
    """
    Calculate Sensitivity, Specificity, PPV, NPV, Precision, Recall, F1
    """

    def save_metrics(self, TP, FP, TN, FN,
                 Sensitivity, Specificity, PPV, NPV, Precision, Recall, Fbeta):
        """
        Save scalar metrics; no tuple/list coercion, no unsafe casts.
        """
        # Round for printing
        Sensitivity_r = round(float(Sensitivity), 3)
        Specificity_r = round(float(Specificity), 3)
        PPV_r         = round(float(PPV), 3)
        NPV_r         = round(float(NPV), 3)
        Precision_r   = round(float(Precision), 3)
        Recall_r      = round(float(Recall), 3)
        Fbeta_r       = round(float(Fbeta), 3)

        formatted_list  = (
            f"TP: {TP} FP: {FP} TN: {TN} FN: {FN} "
            f"Sensitivity: {Sensitivity_r} Specificity: {Specificity_r} "
            f"PPV: {PPV_r} NPV: {NPV_r} "
            f"Precision: {Precision_r} Recall: {Recall_r} Fbeta: {Fbeta_r}"
        )

        formatted_list_1  = (
            f"TP: {TP} FP: {FP} TN: {TN} FN: {FN} "
            f"Precision: {Precision_r} Recall: {Recall_r} Fbeta: {Fbeta_r}"
        )

        metrics_df = pd.DataFrame([{
            "TP": TP, "FP": FP, "TN": TN, "FN": FN,
            "Sensitivity": Sensitivity_r, "Specificity": Specificity_r,
            "PPV": PPV_r, "NPV": NPV_r, "Precision": Precision_r,
            "Recall": Recall_r, "Fbeta": Fbeta_r
        }], index=[myphd_id])

        metrics_df.to_csv(output_dir / f"{myphd_id}_metrics.csv", header=True)
        return formatted_list, formatted_list_1


    # visualization ------------------------------------------------------

    def visualize_complete_dataset2(self, all_anomalies, symptom_date1, symptom_date_before_7, symptom_date_after_21, formatted_list_1):
        # plot original data
        all_score_df = all_anomalies
        ax1 = all_score_df[['RHR']].plot(figsize=(24,4.5), color="black", rot=90)
        ax1.set_xlim(all_score_df.index[0], all_score_df.index[-1]) 
        ax1.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%y-%b-%d'))
        ax1.set_ylabel('Orig Seq\n', fontsize = 20) # Y label
        ax1.set_xlabel('', fontsize = 0) # X label
        ax1.tick_params(axis='both', which='major', labelsize=22)
        ax1.set_xlabel('', fontsize = 0) # X label
        ax1.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
        ax1.set_title(myphd_id,fontweight="bold", size=30) # Title
        plt.xticks(fontsize=0, rotation=90)
        plt.tick_params(axis='both',which='both',bottom=True, top=False, labelbottom=True)
        plt.tight_layout()
        plt.savefig(output_dir / f"{myphd_id}_all_original_seq.pdf", bbox_inches='tight')
        #plt.show()

        # plot anomaly scores
        all_score_df1 = all_anomalies.reset_index()
        ax3 = all_score_df1.plot.scatter(x='datetime',y='loss', figsize=(24,4),  rot=90, marker='o', lw=2,
            c=['red' if i== True else 'mediumblue'  for i in all_score_df1['anomaly']])
        ax3.set_xlim(all_score_df1['datetime'].iloc[0], all_score_df1['datetime'].iloc[-1]) 
        ax3.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
        ax3.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%y-%b-%d'))
        ax3.set_ylabel('Anomaly Score\n', fontsize = 20) # Y label
        ax3.set_xlabel('', fontsize = 0) # X label
        ax3.axvline(pd.to_datetime(symptom_date_before_7), color='orange', zorder=1, linestyle='--',  lw=6, alpha=0.5) # Symptom date 
        ax3.axvline(pd.to_datetime(symptom_date1), color='red', zorder=1, linestyle='--',  lw=6, alpha=0.5) # Symptom date 
        ax3.axvline(pd.to_datetime(symptom_date_after_21), color='purple', zorder=1, linestyle='--', lw=6, alpha=0.5) # Symptom date 
        ax3.tick_params(axis='both', which='major', labelsize=18)
        ax3.yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
        ax3.set_title(myphd_id+ '\n', fontweight="bold", size=30) # Title
        plt.axhline(y=THRESHOLD, color='grey', linestyle='--', lw=3, alpha=0.3)
        plt.tick_params(axis='both',which='both',bottom=True, top=False, labelbottom=True) 
        #plt.title(myphd_id + '\n\n', fontweight="bold", size=30) # Sub title
        plt.suptitle(formatted_list_1+ '\n', fontweight="bold", size=20) # Sub title
        #plt.tight_layout()
        plt.savefig(output_dir / f"{myphd_id}_all_anomaly_scores.pdf", bbox_inches='tight')
        #plt.show()


#################################################################################################

LAAD = LAAD()

# pre-process data
df1 = LAAD.resting_heart_rate(fitbit_oldProtocol_hr, fitbit_oldProtocol_steps)
processed_data = LAAD.pre_processing(df1)

# split dates and data using assumptions listed in the paper
symptom_date1, symptom_date_before_20, symptom_date_before_7, symptom_date_before_10, symptom_date_after_21, train, test, test_anomaly_delta_RHR = LAAD.data_splitting(processed_data, symptom_date)

# standardization
train_data, test_data, test_normal_data, test_anomaly_data, all_merged = LAAD.standardization(train, test, symptom_date_before_20, symptom_date_before_7, symptom_date_before_10, symptom_date_after_21)


#  Create subsequences in tensor format from a dataframe
train_dataset= LAAD.create_dataset(train_data[['RHR']],TIME_STEPS)
test_dataset= LAAD.create_dataset(test_data[['RHR']],TIME_STEPS)
#test_normal_dataset= LAAD.create_dataset(test_normal_data[['RHR']],TIME_STEPS)
#test_anomaly_dataset= LAAD.create_dataset(test_anomaly_data[['RHR']],TIME_STEPS)
all_merged_dataset= LAAD.create_dataset(all_merged[['RHR']],TIME_STEPS)


# data augmentation of trainign dataset
train_aug_dataset = LAAD.augmentation(train_dataset)

# Use train model as both input and target  since this is recosntruction model
# save the best model with lowest loss
early_stopping_callback = keras.callbacks.EarlyStopping(monitor='val_loss', patience=5, mode="min")
checkpoint_callback = keras.callbacks.ModelCheckpoint(
    filepath=str(output_dir / f"{myphd_id}_model.keras"),  # native format
    monitor='val_loss',
    verbose=1,
    save_best_only=True,
    mode='min'
)
history, LA = LAAD.LA(train_aug_dataset, train_aug_dataset)
LAAD.visualize_loss(history)

# Save the model
#filepath = './'+myphd_id+'_model.h5'
#save_model(LA, filepath, save_format='h5')

# evaluate train dataset to calculate MAE loss and set a threshold
predictions = LA.predict(train_dataset)
losses = np.mean(np.abs(predictions - train_dataset), axis=1)
LAAD.predictions_loss_train(losses, train_dataset)
THRESHOLD = LAAD.anomaly_threshold(losses)

# evaluate test normal and anomaly datasets
#predictions = LA.predict(test_normal_dataset)
#losses = np.mean(np.abs(predictions - test_normal_dataset), axis=1)
#LAAD.predictions_loss_test_normal(losses, test_normal_dataset)

#predictions = LA.predict(test_anomaly_dataset)
#losses = np.mean(np.abs(predictions - test_anomaly_dataset), axis=1)
#LAAD.predictions_loss_test_anomaly(losses, test_anomaly_dataset)

# evaluate test dataset
predictions = LA.predict(test_dataset)
losses = np.mean(np.abs(predictions - test_dataset), axis=1)
LAAD.predictions_loss_test(losses, test_dataset)

# save anomalies
anomalies, delta_RHR = LAAD.save_anomalies(test, test_anomaly_delta_RHR)

# evaluate complete dataset
predictions = LA.predict(all_merged_dataset)
losses = np.mean(np.abs(predictions - all_merged_dataset), axis=1)
all_anomalies = LAAD.evaluate_complete_dataset(all_merged, THRESHOLD)
  
# metrics
TP, FP, TN, FN, formatted_list_2 = LAAD.metrics_1(
    all_anomalies,
    test_normal_data,
    symptom_date_before_7,
    symptom_date_after_21,
    myphd_id
)
LAAD.visualize_complete_dataset1(all_anomalies, symptom_date1, symptom_date_before_7, symptom_date_after_21, formatted_list_2)
Sensitivity, Specificity, PPV, NPV, Precision, Recall, Fbeta = LAAD.metrics_2(TP, FP, TN, FN)

# visualization
formatted_list,formatted_list_1 = LAAD.save_metrics(TP, FP, TN, FN, Sensitivity, Specificity, PPV, NPV, Precision, Recall, Fbeta)
LAAD.visualize_complete_dataset2(all_anomalies, symptom_date1, symptom_date_before_7, symptom_date_after_21, formatted_list_1)

print("\nCompleted!\n")
