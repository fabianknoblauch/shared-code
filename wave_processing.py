# TODO:
# - test and validate
# - use spectral moments instead

import compress_pickle as cpkl
import arduino_logging
from datetime import datetime
import math

import numpy as np

from scipy import signal

import matplotlib.pyplot as plt

from arduino_logging import compute_checksum

DEG_TO_RAD = 2.0 * math.pi / 360
RAD_TO_DEG = 1.0 / DEG_TO_RAD

# installation properties: how things are set up on the boat, from a "geometry" point of view
INSTALLATION_PROPERTIES = {}
#
INSTALLATION_PROPERTIES["gauge_ug1_meters"] = {}
INSTALLATION_PROPERTIES["gauge_ug1_meters"]["x_offset_m"] = 0.0
INSTALLATION_PROPERTIES["gauge_ug1_meters"]["y_offset_m"] = 0.0
#
INSTALLATION_PROPERTIES["gauge_ug2_meters"] = {}
INSTALLATION_PROPERTIES["gauge_ug2_meters"]["x_offset_m"] = 0.0
INSTALLATION_PROPERTIES["gauge_ug2_meters"]["y_offset_m"] = 0.0
#
INSTALLATION_PROPERTIES["gauge_radar1_meters"] = {}
INSTALLATION_PROPERTIES["gauge_radar1_meters"]["x_offset_m"] = 0.0
INSTALLATION_PROPERTIES["gauge_radar1_meters"]["y_offset_m"] = 0.0


def round_to_ending_01(number_in):
    number_out = math.floor(number_in * 10) / 10
    return number_out


def generate_common_timebase(time_base_1, time_base_2, time_base_3, time_base_4):
    # find the largest common time base
    # use the ms ending in 00, as we are logging at 100Hz
    min_common_time = round_to_ending_01(max(time_base_1[0], time_base_2[0], time_base_3[0], time_base_4[0])) + 0.1
    max_common_time = round_to_ending_01(min(time_base_1[-1], time_base_2[-1], time_base_3[-1], time_base_4[-1])) - 0.1

    common_time_base = np.arange(min_common_time, max_common_time, 0.1)

    return common_time_base


def load_data_dump(path_to_file, show_interpolation=False, resistor_values=160.0):
    """Load a data dump, and extract all the data, ready to use."""

    # load compressed data
    with open(path_to_file, "br") as fh:
        data = cpkl.load(fh, compression="lzma")
        
    #print(data)

    # put all the data at different stages of processing in a common dict
    dict_data = {}

    # the raw data read from the log file
    dict_data["lists_raw_data"] = {}
    dict_data["lists_raw_data"]["IMU"]=[]
    #dict_data["lists_raw_data"]["IMU"]["datetime"] = []
    dict_data["lists_raw_data"]["gauges"] = []
    dict_data["lists_raw_data"]["meta"] = []
    # TODO: load 9dof
    dict_data["lists_raw_data"]["extraIMU_0"] = []
    dict_data["lists_raw_data"]["extraIMU_1"] = []
    
    dict_data["list_time_series"] = {}
    dict_data["list_time_series"]["extraIMU_0"]={}
    dict_data["list_time_series"]["IMU"]={}
    dict_data["list_time_series"]["extraIMU_1"]={}
    dict_data["list_time_series"]["gauges"] = {}
    dict_data["list_time_series"]["extraIMU_0"]["datetime"] = []
    dict_data["list_time_series"]["extraIMU_1"]["datetime"] = []        
    dict_data["list_time_series"]["IMU"]["datetime"]=[]    
    dict_data["list_time_series"]["gauges"]["datetime"]=[]
               


    # TODO: now that using both due and extra IMUs, need to use the RPi timestamp in order to perform "common time base"

    # split in the correct categories
    # TODO: add info about RPi timesamping
    for crrt_entry in data:
        # print(crrt_entry)
        crrt_datetime = crrt_entry[0]
        #print(crrt_entry[1])
        crrt_kind = crrt_entry[1]
        crrt_packet = crrt_entry[2]
        if crrt_kind == "I":
            dict_data["lists_raw_data"]["IMU"].append(crrt_packet)
            dict_data["list_time_series"]["IMU"]["datetime"].append(crrt_datetime)
        elif crrt_kind == "G":
            dict_data["lists_raw_data"]["gauges"].append(crrt_packet)
            dict_data["list_time_series"]["gauges"]["datetime"].append(crrt_datetime)
        elif crrt_kind == "9dof":
            if crrt_packet.metadata == 0:
                dict_data["lists_raw_data"]["extraIMU_0"].append(crrt_packet)
                dict_data["list_time_series"]["extraIMU_0"]["datetime"].append(crrt_datetime)
            elif crrt_packet.metadata == 1:
                dict_data["lists_raw_data"]["extraIMU_1"].append(crrt_packet)
                dict_data["list_time_series"]["extraIMU_1"]["datetime"].append(crrt_datetime)

            else:
                print("error meta value sorting")
        
        else:
            dict_data["lists_raw_data"]["meta"].append(crrt_packet)
        # TODO: add field for 9dof

    # produce the time series for the probe and IMU
 
    dict_data["list_time_series"]["IMU"]["accel_D"] = []
    dict_data["list_time_series"]["IMU"]["pitch"] = []
    dict_data["list_time_series"]["IMU"]["roll"] = []
    dict_data["list_time_series"]["IMU"]["yaw"] = []
    dict_data["list_time_series"]["gauges"]["gauge_ug1_meters"] = []
    dict_data["list_time_series"]["gauges"]["gauge_ug2_meters"] = []
    dict_data["list_time_series"]["gauges"]["gauge_radar1_meters"] = []
    
    dict_data["list_time_series"]["extraIMU_0"]["accel_D"] = []
    dict_data["list_time_series"]["extraIMU_0"]["pitch"] = []
    dict_data["list_time_series"]["extraIMU_0"]["roll"] = []
    dict_data["list_time_series"]["extraIMU_0"]["yaw"] = []
    dict_data["list_time_series"]["extraIMU_1"]["accel_D"] = []
    dict_data["list_time_series"]["extraIMU_1"]["pitch"] = []
    dict_data["list_time_series"]["extraIMU_1"]["roll"] = []
    dict_data["list_time_series"]["extraIMU_1"]["yaw"] = []
    dict_data["list_time_series"]["IMU"]["timestamps"]=[]
    dict_data["list_time_series"]["extraIMU_0"]["timestamps"]=[]
    dict_data["list_time_series"]["extraIMU_1"]["timestamps"]=[]
    dict_data["list_time_series"]["gauges"]["timestamps"]=[]
    
    for crrt_IMU0_packet in dict_data["lists_raw_data"]["extraIMU_0"]:
        dict_data["list_time_series"]["extraIMU_0"]["accel_D"].append(crrt_IMU0_packet.acc_D)
        dict_data["list_time_series"]["extraIMU_0"]["pitch"].append(crrt_IMU0_packet.pitch)
        dict_data["list_time_series"]["extraIMU_0"]["roll"].append(crrt_IMU0_packet.roll_)
        dict_data["list_time_series"]["extraIMU_0"]["yaw"].append(crrt_IMU0_packet.yaw__)

    for crrt_IMU1_packet in dict_data["lists_raw_data"]["extraIMU_1"]:
        dict_data["list_time_series"]["extraIMU_1"]["accel_D"].append(crrt_IMU1_packet.acc_D)
        dict_data["list_time_series"]["extraIMU_1"]["pitch"].append(crrt_IMU1_packet.pitch)
        dict_data["list_time_series"]["extraIMU_1"]["roll"].append(crrt_IMU1_packet.roll_)
        dict_data["list_time_series"]["extraIMU_1"]["yaw"].append(crrt_IMU1_packet.yaw__)

    for crrt_imu_packet in dict_data["lists_raw_data"]["IMU"]:
        dict_data["list_time_series"]["IMU"]["accel_D"].append(crrt_imu_packet.acc_D)
        dict_data["list_time_series"]["IMU"]["pitch"].append(crrt_imu_packet.pitch)
        dict_data["list_time_series"]["IMU"]["roll"].append(crrt_imu_packet.roll)
        dict_data["list_time_series"]["IMU"]["yaw"].append(crrt_imu_packet.yaw)

    for crrt_gauges_packet in dict_data["lists_raw_data"]["gauges"]:
        height_1, height_2, height_3 = \
            arduino_logging.convert_g_packet_adc_to_distances(crrt_gauges_packet, resistor_values, resistor_values, resistor_values, 12, 3.3)
        dict_data["list_time_series"]["gauges"]["gauge_ug1_meters"].append(height_1)
        dict_data["list_time_series"]["gauges"]["gauge_ug2_meters"].append(height_2)
        dict_data["list_time_series"]["gauges"]["gauge_radar1_meters"].append(height_3)

    # extract for the 9dof

    # TODO: compute a new time base based on the RPi timestamp since needs to "synchronize" the Due and extra IMUs
    #print("length list time series: ",len(dict_data["list_time_series"]["IMU"]["roll"]))
    
    
    for cnt in range(len(dict_data["list_time_series"]["IMU"]["datetime"])):
        dict_data["list_time_series"]["IMU"]["timestamps"].append(datetime.timestamp(dict_data["list_time_series"]["IMU"]["datetime"][cnt]))
    for cnt in range(len(dict_data["list_time_series"]["extraIMU_0"]["datetime"])):
        dict_data["list_time_series"]["extraIMU_0"]["timestamps"].append(datetime.timestamp(dict_data["list_time_series"]["extraIMU_0"]["datetime"][cnt]))
    for cnt in range(len(dict_data["list_time_series"]["extraIMU_1"]["datetime"])):
        dict_data["list_time_series"]["extraIMU_1"]["timestamps"].append(datetime.timestamp(dict_data["list_time_series"]["extraIMU_1"]["datetime"][cnt]))
    for cnt in range(len(dict_data["list_time_series"]["gauges"]["datetime"])):
        dict_data["list_time_series"]["gauges"]["timestamps"].append(datetime.timestamp(dict_data["list_time_series"]["gauges"]["datetime"][cnt]))

    # fix lists for sensor blackout
    imus=["IMU","extraIMU_0","extraIMU_1"]
    for imu in imus:
        if len(dict_data["list_time_series"][imu]["timestamps"]) < 1:
            dict_data["list_time_series"][imu]["timestamps"].append(dict_data["list_time_series"]["gauges"]["timestamps"][0])
            dict_data["list_time_series"][imu]["timestamps"].append(dict_data["list_time_series"]["gauges"]["timestamps"][-1])
            print("IMU: "+imu+" failed from "+dict_data["list_time_series"]["gauges"]["datetime"][0].strftime("%H:%M")+" to "+dict_data["list_time_series"]["gauges"]["datetime"][-1].strftime("%H:%M"))
    common_time_base = generate_common_timebase(
        dict_data["list_time_series"]["IMU"]["timestamps"],
        dict_data["list_time_series"]["gauges"]["timestamps"],
        dict_data["list_time_series"]["extraIMU_1"]["timestamps"],
        dict_data["list_time_series"]["extraIMU_0"]["timestamps"]
    )

    # interpolate the time series on a common time base
    # TODO: also interpolate 9dof
    dict_data["list_time_series_interpolated"] = {}
    dict_data["list_time_series_interpolated"]["common_time_stamps"] = common_time_base
    dict_data["list_time_series_interpolated"]["common_datetime"]=[]
    for cnt in range(len(dict_data["list_time_series_interpolated"]["common_time_stamps"])):
        dict_data["list_time_series_interpolated"]["common_datetime"].append(datetime.fromtimestamp(dict_data["list_time_series_interpolated"]["common_time_stamps"][cnt]))
    dict_data["list_time_series_interpolated"]["IMU"]={}
    dict_data["list_time_series_interpolated"]["extraIMU_0"]={}
    dict_data["list_time_series_interpolated"]["extraIMU_1"]={}
    dict_data["list_time_series_interpolated"]["gauges"]={}

    
    for imu in imus:
        
        if len(dict_data["list_time_series"][imu]["timestamps"]) > 2:
            
            dict_data["list_time_series_interpolated"][imu]["accel_D"] = np.interp(
            common_time_base,dict_data["list_time_series"][imu]["timestamps"],
            dict_data["list_time_series"][imu]["accel_D"])
            
            dict_data["list_time_series_interpolated"][imu]["pitch"] = np.interp(
            common_time_base,dict_data["list_time_series"][imu]["timestamps"],
            dict_data["list_time_series"][imu]["pitch"])
            
            dict_data["list_time_series_interpolated"][imu]["roll"] = np.interp(
            common_time_base,dict_data["list_time_series"][imu]["timestamps"],
            dict_data["list_time_series"][imu]["roll"])
            
            dict_data["list_time_series_interpolated"][imu]["yaw"] = np.interp(
            common_time_base,dict_data["list_time_series"][imu]["timestamps"],
            dict_data["list_time_series"][imu]["yaw"])
            
        else:

            dict_data["list_time_series_interpolated"][imu]["accel_D"]=[]
            for cnt in range(len(common_time_base)):
                dict_data["list_time_series_interpolated"][imu]["accel_D"].append(999999999)
            dict_data["list_time_series_interpolated"][imu]["pitch"]=dict_data["list_time_series_interpolated"][imu]["accel_D"]*1
            dict_data["list_time_series_interpolated"][imu]["roll"]=dict_data["list_time_series_interpolated"][imu]["accel_D"]*1
    
    
    dict_data["list_time_series_interpolated"]["gauges"]["gauge_ug1_meters"] = np.interp(
        common_time_base,
        dict_data["list_time_series"]["gauges"]["timestamps"],
        dict_data["list_time_series"]["gauges"]["gauge_ug1_meters"]
    )
    dict_data["list_time_series_interpolated"]["gauges"]["gauge_ug2_meters"] = np.interp(
        common_time_base,
        dict_data["list_time_series"]["gauges"]["timestamps"],
        dict_data["list_time_series"]["gauges"]["gauge_ug2_meters"]
    )
    dict_data["list_time_series_interpolated"]["gauges"]["gauge_radar1_meters"] = np.interp(
        common_time_base,
        dict_data["list_time_series"]["gauges"]["timestamps"],
        dict_data["list_time_series"]["gauges"]["gauge_radar1_meters"]
    )
    
    #print("length list time series interpolated: ",len(dict_data["list_time_series_interpolated"]["IMU"]["roll"]))
    
    return dict_data


def integrate_twice(data_in, plot=False):
    '''integrate twice using fft and ifft, and ignoring data outside of the frequency
    bands that are of interest to us.'''

    # calculate fft, filter, and then ifft to get heights
    SAMPLING_FRQ = 10.0
    ACC_SIZE = data_in.shape[0]

    # suppress divide by 0 warning
    np.seterr(divide='ignore')

    Y = np.fft.fft(data_in)

    # calculate weights before applying ifft
    freq = np.fft.fftfreq(ACC_SIZE, d=1.0 / SAMPLING_FRQ)
    weights = -1.0 / ((2 * np.pi * freq)**2)
    # need to do some filtering for low frequency (from Kohout)
    f1 = 0.03
    f2 = 0.04
    Yf = np.zeros_like(Y)
    ind = np.argwhere(np.logical_and(freq >= f1, freq <= f2))
    Yf[ind] = Y[ind] * 0.5 * (1 - np.cos(np.pi * (freq[ind] - f1) / (f2 - f1))) * weights[ind]
    Yf[freq > f2] = Y[freq > f2] * weights[freq > f2]

    # apply ifft to get height
    elev = np.real(np.fft.ifft(2 * Yf))

    if plot:
        plt.figure()
        plt.plot(data_in, label="data_in")
        plt.plot(elev, label="elev")
        plt.legend()
        plt.show()

    return elev


def find_index_of_fist_element_greater_than_value(array, value):
    indexes_where_greater_than = np.where(array > value)[0]
    if len(indexes_where_greater_than) == 0:
        return None
    else:
        return indexes_where_greater_than[0]


def compute_wave_spectrum_moments(list_frequencies, wave_spectrum, min_freq=0.04, max_freq=0.5, PERFORM_KET_PLOTS=False):
    """Compute the moments of the wave spectrum."""

    min_ind = find_index_of_fist_element_greater_than_value(list_frequencies, min_freq)
    max_ind = find_index_of_fist_element_greater_than_value(list_frequencies, max_freq)

    wave_spectrum = wave_spectrum[min_ind:max_ind]
    list_frequencies = list_frequencies[min_ind:max_ind]

    if PERFORM_KET_PLOTS:
        plt.figure()
        plt.plot(list_frequencies, wave_spectrum)
        plt.show()

    omega = 2 * np.pi * list_frequencies

    M0 = np.integrate.trapz(wave_spectrum, x=omega)
    M1 = np.integrate.trapz(wave_spectrum * (omega), x=omega)
    M2 = np.integrate.trapz(wave_spectrum * (omega**2), x=omega)
    M3 = np.integrate.trapz(wave_spectrum * (omega**3), x=omega)
    M4 = np.integrate.trapz(wave_spectrum * (omega**4), x=omega)
    MM1 = np.integrate.trapz(wave_spectrum * (omega**(-1)), x=omega)
    MM2 = np.integrate.trapz(wave_spectrum * (omega**(-2)), x=omega)

    return(M0, M1, M2, M3, M4, MM1, MM2)


def compute_spectral_properties(M0, M2, M4, wave_spectrum, list_frequencies, min_freq=0.04, max_freq=0.5):
    """Compute SWH and the peak period, both zero up-crossing and peak-to-peak,
    from spectral moments."""

    min_ind = find_index_of_fist_element_greater_than_value(list_frequencies, min_freq)
    max_ind = find_index_of_fist_element_greater_than_value(list_frequencies, max_freq)

    wave_spectrum = wave_spectrum[min_ind:max_ind]
    list_frequencies = list_frequencies[min_ind:max_ind]

    Hs = np.sqrt(M0) * 4.0 / np.sqrt(2 * np.pi)
    T_z = 2.0 * np.pi * np.sqrt(M0 / M2)
    T_c = 2.0 * np.pi * np.sqrt(M2 / M4)
    T_p = 1.0 / list_frequencies[np.argmax(wave_spectrum)]

    if False:
        print('Hs (from M0) = {} m'.format(Hs))
        print('T_z = {} s | {} Hz'.format(T_z, 1.0 / T_z))
        print('T_p = {} s | {} Hz'.format(T_p, 1.0 / T_p))

    return(Hs, T_z, T_c, T_p)


def welch_spectrum(data_in, sampling_rate=10.0, overlap_proportion=0.9, segment_duration_seconds=400, smooth=True):
    nperseg = int(segment_duration_seconds * sampling_rate)
    noverlap = int(overlap_proportion * nperseg)

    f, Pxx_den = signal.welch(data_in, sampling_rate, nperseg=nperseg, noverlap=noverlap)

    if smooth:
        Pxx_den = signal.savgol_filter(Pxx_den, window_length=9, polyorder=2)

    if False:
        plt.figure()
        plt.plot(f, Pxx_den)
        plt.show()

    return(f, Pxx_den)


def compute_SWH(elevation):
    SWH = 4.0 * np.std(elevation)
    if False:
        print("SWH = {}".format(SWH))
    return SWH


def compute_wave_elevation(dict_data_in, installation_properties, probe="gauge_ug1_meters", plot=False):
    # TODO: all these signs etc need to be CAREFULLY CHECKED!!!

    time_base = dict_data_in["list_time_series_interpolated"]["common_time_base"]
    time_base = (time_base - time_base[0]) / 1000.0

    gauge_distance_meters = dict_data_in["list_time_series_interpolated"][probe]
    accel_D = dict_data_in["list_time_series_interpolated"]["accel_D"]
    pitch_rad = DEG_TO_RAD * dict_data_in["list_time_series_interpolated"]["pitch"]
    roll_rad = DEG_TO_RAD * dict_data_in["list_time_series_interpolated"]["roll"]

    # IMU displacement component ----------------------------------------
    # compute the twice integrated acceleration, pointing DOWN due to the NED referential
    imu_displacement_down = integrate_twice(accel_D)

    # convert it into the IMU displacement upwards, in a referential pointing UP
    imu_displacement_upwards = - imu_displacement_down

    # probe pitch and roll component ----------------------------------------
    # pitch: positive is higher over water
    # roll: being on the left of the boat, positive is higher over water
    probe_pitch_displacement_upwards = installation_properties[probe]["y_offset_m"] * np.sin(pitch_rad)
    probe_roll_displacement_upwards = installation_properties[probe]["y_offset_m"] * np.cos(roll_rad)

    # total prove displacement ----------------------------------------
    total_probe_displacement_upwards = imu_displacement_upwards + probe_pitch_displacement_upwards + probe_roll_displacement_upwards

    # gauge distance vertically over the water, taking into accound pitch and roll --------------------
    # compute the right wave height etc
    gauge_height_over_water = gauge_distance_meters * np.cos(pitch_rad) * np.cos(roll_rad)

    # total water elevation ----------------------------------------
    water_elevation = total_probe_displacement_upwards - gauge_height_over_water

    dict_data_in["list_time_series_interpolated"]["water_elevation_{}".format(probe)] = water_elevation

    if plot:
        plt.figure()

        plt.plot(time_base, accel_D, label="accel_D")
        plt.plot(time_base, imu_displacement_upwards, label="imu_displacement_upwards")
        plt.plot(time_base, total_probe_displacement_upwards, label="total_probe_displaacement_upwards")
        plt.plot(time_base, gauge_height_over_water, label="gauge_height_over_water")
        plt.plot(time_base, water_elevation, label="water_elevation")

        plt.xlabel("time [s]")
        plt.ylabel("values [m] or [deg] or [m/s2]")
        plt.title("elevation using probe {}".format(probe))

        plt.legend()
        plt.show()

    return dict_data_in


def format_swh_4sigma_for_udp(swh_4sigma):
    str_nmea = "$SWH_4STD,{:05.2f}".format(swh_4sigma)
    checksum = compute_checksum(str_nmea)
    str_nmea += "*{}".format(checksum)
    str_nmea += "\r\n"
    str_nmea = str_nmea.encode("ascii")

    return str_nmea


if __name__ == "__main__":
    # load the data in a nicely usable format, including making sure that all data are on the same timebase
    if False:
        PATH_TO_FOLDER_DUMP = "/home/pi/Git/ultrasound_radar_bow_wave_sensor_Statsraad_Lehmkuhl_2021_2022/data/"
        example_file = "2021/06/04/2021-06-04_09:30:00__2021-06-04_10:00:00.pkl.lzma"
    if True:
        PATH_TO_FOLDER_DUMP = "/home/jrmet/Desktop/Git/ultrasound_radar_bow_wave_sensor_Statsraad_Lehmkuhl_2021_2022/data/"
        example_file = "2021/05/24/2021-05-24_09:30:00__2021-05-24_10:00:00.pkl.lzma"
    dict_data = load_data_dump(PATH_TO_FOLDER_DUMP + example_file, show_interpolation=True)

    # compute the wave elevation
    crrt_probe = "gauge_ug1_meters"
    dict_data = compute_wave_elevation(dict_data, INSTALLATION_PROPERTIES, probe=crrt_probe, plot=True)

    # compute the wave properties: SWH
    # for now, only use 4 * std(eta)
    SWH_4std = compute_SWH(dict_data["list_time_series_interpolated"]["water_elevation_{}".format(crrt_probe)])

    print("found SWH: {}".format(SWH_4std))

    str_nmea_swh_4sigma = format_swh_4sigma_for_udp(SWH_4std)
    print("will be broadcasted as: {}".format(str_nmea_swh_4sigma))

